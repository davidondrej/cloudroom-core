#!/usr/bin/env python3
"""Install or upgrade this VM's core from a verified release folder (ADR 0082). Run as root.

  update.py install DIR   New VM: copy the release in before configure.py starts the core.
  update.py upgrade DIR   Existing VM: wait until idle, back up, swap, restart, verify, roll back on failure.
  update.py unblock       Remove the API gate (the update job's ExecStopPost).

DIR holds `cloudroom`, `update-manifest.json`, and `install/`. Never prints secrets.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path('/usr/local/lib/cloudroom')
BINARY = ROOT / 'cloudroom'
FILES = ('configure.py', 'agent-home.py', 'previews.py', 'update.py')
STATE = Path('/var/lib/cloudroom-update')
BACKUPS = Path('/var/backups/cloudroom')
GUIDE = Path('/etc/systemd/system/cloudroom.service.d/40-machine-instructions.conf')
CLAUDE_CODE = '2.1.281'  # The Claude Code version tested with this release.
TAG = 'cloudroom-core-update'
QUIET_MS = 10 * 60_000  # Restart only after 10 minutes without session activity.
RETRY_SECONDS = 15 * 60
IDLE = ('idle', 'sleeping', 'closed', 'process_lost', 'failed', 'saved_history_only')
HARNESS_NAMES = (
    'codex', 'codex-code-mode', 'pi', 'cursor-agent', 'claude',
    'fx', 'opencode', 'opencode.exe', 'hermes', 'devin', 'openclaw',
)


def run(*args, timeout=300, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout, **kwargs).stdout.strip()


def sha(path):
    with open(path, 'rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def version(text):
    return tuple(int(part) for part in text.split('.'))


def write(path, data, mode):
    """Atomic, root-owned replace on the same filesystem."""
    fd, temp = tempfile.mkstemp(dir=path.parent, prefix='.update-')
    try:
        with os.fdopen(fd, 'wb') as file:
            file.write(data)
            file.flush()
            os.fchown(file.fileno(), 0, 0)
            os.fchmod(file.fileno(), mode)
            os.fsync(file.fileno())
        os.replace(temp, path)
    finally:
        Path(temp).unlink(missing_ok=True)


def service_env():
    """The core's settings, from the environment files systemd gives it. Empty before configure.py."""
    listed = run('systemctl', 'show', 'cloudroom.service', '-p', 'EnvironmentFiles', '--value').splitlines()
    values = {}
    for path in (Path(line.split(' (')[0]) for line in listed if line.strip()):
        for line in path.read_text().splitlines() if path.is_file() else []:
            if '=' in line and not line.lstrip().startswith('#'):
                key, raw = line.split('=', 1)
                parts = shlex.split(raw)
                values[key] = parts[0] if parts else ''
    return values


def port(values):
    return values.get('CLOUDROOM_LISTEN', '127.0.0.1:9840').rsplit(':', 1)[1]


def api(values, path):
    request = urllib.request.Request(f'http://127.0.0.1:{port(values)}{path}',
                                     headers={'Authorization': 'Bearer ' + values['CLOUDROOM_TOKEN']})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt == 2:
                raise
            time.sleep(3)


def sessions(values):
    dashboard = api(values, '/v1/dashboard')
    if dashboard['sessionCount'] != len(dashboard['sessions']):
        raise RuntimeError('Too many sessions to check')
    return dashboard, {s['id']: api(values, '/v1/sessions/' + s['id'])['session'] for s in dashboard['sessions']}


def busy(values):
    """Why restarting the core now could interrupt work, or None."""
    dashboard, details = sessions(values)
    if not dashboard['runtime']['ready']:
        return 'local runtime is not ready'
    recent = max((s.get('lastActivity') or 0 for s in dashboard['sessions']), default=0)
    if time.time() * 1000 - recent < QUIET_MS:
        return 'recent session activity'
    for sid, session in details.items():
        if session['state'] not in IDLE or any(session.get(k) for k in ('current_request', 'current_turn', 'compacting', 'rewind_request')):
            return 'active session ' + sid
        # Paused queues survive restarts; a runnable queue means work could start.
        if session.get('queue') and not session.get('queue_paused') and session['state'] not in ('process_lost', 'failed', 'closed'):
            return 'runnable queue in ' + sid
    state = Path(values.get('CLOUDROOM_STATE_DIR', '/srv/cloudroom/core/history'))
    for manifest in (state / 'teleports').glob('*/manifest.json'):
        # Stalled transfers stay on disk and resume after a restart; only recent uploads block.
        if time.time() - max(path.stat().st_mtime for path in manifest.parent.rglob('*')) > QUIET_MS / 1000:
            continue
        transfer = api(values, '/v1/teleports/' + manifest.parent.name)
        if transfer.get('phase') != 'cancelled' and not all(f.get('complete') for f in transfer.get('files', [])):
            return 'Teleport transfer in progress'
    for procs in Path('/sys/fs/cgroup/system.slice/cloudroom.service').rglob('cgroup.procs'):
        for pid in procs.read_text().split():
            try:
                exe = str(Path('/proc', pid, 'exe').resolve())
                args = Path('/proc', pid, 'cmdline').read_bytes().split(b'\0')
            except (FileNotFoundError, ProcessLookupError):
                continue
            # Idle harness processes are expected; shells and other tools mean active work.
            harness = (exe.startswith(('/usr/local/lib/cloudroom/', '/usr/local/lib/node_modules/@anthropic-ai/',
                                       '/srv/cloudroom/agent/.local/share/claude/versions/', '/opt/ascii-agent/'))
                       or Path(exe).name in HARNESS_NAMES
                       or any(a.endswith(b'/codex.js') for a in args[1:3]))
            if not harness and not mcp_server(pid):
                return 'running tool ' + Path(exe).name
    return None


# `@playwright/mcp@latest`, `playwright-mcp`, `mcp-server-git`: MCP servers launched by a harness.
MCP_ARG = re.compile(rb'(^|[/@])([\w.]+-)*mcp(-[\w.]+)*(@[\w.]+)?$')


def mcp_server(pid):
    """MCP servers and their children belong to their harness and restart with it."""
    for _ in range(8):
        try:
            args = Path('/proc', pid, 'cmdline').read_bytes().split(b'\0')
            pid = next(line.split()[1] for line in Path('/proc', pid, 'status').read_text().splitlines()
                       if line.startswith('PPid:'))
        except (OSError, StopIteration):
            return False
        if any(MCP_ARG.search(arg) for arg in args[:4]):
            return True
        if pid in ('0', '1'):
            return False
    return False


def gate(values, on):
    """Reject remote and non-root local API traffic while the core is replaced."""
    for tool in ('/usr/sbin/iptables', '/usr/sbin/ip6tables'):
        if not Path(tool).exists():  # Machines without a firewall tool skip the gate; the core is stopped for the swap anyway.
            continue
        for chain, *rule in (['INPUT', '!', '-i', 'lo'], ['OUTPUT', '-o', 'lo', '-m', 'owner', '!', '--uid-owner', '0']):
            spec = [*rule, '-p', 'tcp', '--dport', port(values), '-m', 'comment', '--comment', TAG, '-j', 'REJECT', '--reject-with', 'tcp-reset']
            if on:
                run(tool, '-w', '5', '-I', chain, '1', *spec)
                continue
            while subprocess.run([tool, '-w', '5', '-C', chain, *spec], capture_output=True).returncode == 0:
                run(tool, '-w', '5', '-D', chain, *spec)


def copy_release(stage, manifest):
    if sha(stage / 'cloudroom') != manifest['sha256']:
        raise RuntimeError('Release binary does not match its manifest')
    for name in FILES:
        write(ROOT / name, (stage / 'install' / name).read_bytes(), 0o644)
    write(BINARY, (stage / 'cloudroom').read_bytes(), 0o755)


def guard_guide():
    """A failing machine guide must never stop the core from starting."""
    if GUIDE.is_file():
        text = GUIDE.read_text()
        fixed = text.replace('ExecStartPre=+/usr/bin/python3', 'ExecStartPre=-+/usr/bin/python3')
        if fixed != text:
            write(GUIDE, fixed.encode(), 0o644)
            run('systemctl', 'daemon-reload')


def ensure_claude(values):
    """Upgrade Claude Code to this release's version; never downgrade."""
    home = Path(values.get('CLOUDROOM_ACCOUNT_HOME', '/srv/cloudroom/agent'))
    native = home / '.local/bin/claude'
    binary = native if native.exists() else Path('/usr/local/bin/claude')  # Core's own lookup order.
    if not binary.exists():
        return
    as_agent = ['runuser', '-u', 'cloudroom-agent', '--', 'env', f'HOME={home}']
    def installed():
        return run(*as_agent, str(binary), '--version', cwd=home, timeout=60).split()[0]
    if version(installed()) >= version(CLAUDE_CODE):
        return
    if binary == native:
        run(*as_agent, str(native), 'install', CLAUDE_CODE, cwd=home, timeout=600)
    else:
        # A global npm install deletes the `claude` link here, so fetch the native build into a
        # scratch folder and atomically replace the file the link points to.
        package = '@anthropic-ai/claude-code-linux-' + {'x86_64': 'x64', 'aarch64': 'arm64'}[os.uname().machine]
        STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=STATE) as scratch:
            run('npm', 'install', '--prefix', scratch, '--ignore-scripts', '--no-audit', '--no-fund', f'{package}@{CLAUDE_CODE}', timeout=600)
            write(binary.resolve(), (Path(scratch) / 'node_modules' / package / 'claude').read_bytes(), 0o755)
    if installed() != CLAUDE_CODE:
        raise RuntimeError('Claude Code did not update')


def optional(name, step, notes):
    """VM tools shipped with each release. Their failures are reported, never fatal."""
    try:
        step()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        notes.append(f'{name}: {type(error).__name__}')


def preview_state(values):
    return Path(values.get('CLOUDROOM_STATE_DIR', '/srv/cloudroom/core/history')) / 'previews'


def previews(values):
    run('python3', str(ROOT / 'previews.py'), str(preview_state(values).parent))


def restarts():
    return int(run('systemctl', 'show', 'cloudroom.service', '-p', 'NRestarts', '--value'))


def start():
    """Start the core and return systemd's restart count, which survives manual starts."""
    count = restarts()
    run('systemctl', 'start', 'cloudroom.service')
    return count


def verify(values, before, expected, features, baseline, deadline=300):
    """The new core runs, reports the release, and kept every session."""
    stop = time.monotonic() + deadline
    while True:
        try:
            dashboard, after = sessions(values)
            capabilities = api(values, '/v1/capabilities')
            pid = run('systemctl', 'show', 'cloudroom.service', '-p', 'MainPID', '--value')
            running = sha(f'/proc/{pid}/exe')
        except (OSError, RuntimeError, KeyError, ValueError, subprocess.SubprocessError):
            if restarts() > baseline:
                raise RuntimeError('The core keeps restarting') from None
            if time.monotonic() > stop:
                raise
            time.sleep(3)
            continue
        failures = [name for name, ok in {
            'running binary': running == expected,
            'features': all(capabilities.get(f) is True for f in features),
            'session set': set(after) == set(before),
        }.items() if not ok]
        waiting = not dashboard['runtime']['ready']
        for sid in before.keys() & after.keys():
            old, new = before[sid], after[sid]
            if any(new.get(k) != old.get(k) for k in ('native_id', 'workspace', 'queue', 'queue_paused', 'harness', 'provider')) \
                    or new.get('last_sequence', 0) < old.get('last_sequence', 0):
                failures.append('changed ' + sid)
            elif new['state'] != old['state']:
                # Sleeping sessions hold no process; they may come back idle, sleeping, or resuming.
                if old['state'] in ('idle', 'sleeping') and new['state'] in ('idle', 'sleeping', 'resuming'):
                    waiting |= new['state'] == 'resuming'
                else:
                    failures.append(f'{sid} is {new["state"]}')
        if failures:
            raise RuntimeError('; '.join(failures))
        if not waiting:
            return
        if time.monotonic() > stop:
            raise RuntimeError('The core did not finish starting')
        time.sleep(3)


def record(**data):
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    write(STATE / 'state.json', json.dumps({**data, 'at': int(time.time())}).encode(), 0o600)
    print(json.dumps(data))


def prune(stage):
    """Keep this release's stage and the two newest automatic backups."""
    for path in STATE.iterdir():
        if path.is_dir() and path != stage:
            shutil.rmtree(path, ignore_errors=True)
    for path in sorted(BACKUPS.glob('auto-*'))[:-2]:
        shutil.rmtree(path, ignore_errors=True)


def upgrade(stage, manifest):
    values, target = service_env(), manifest['commit']
    previous = json.loads((STATE / 'state.json').read_text()) if (STATE / 'state.json').is_file() else {}
    if sha(BINARY) == manifest['sha256']:
        return record(target=target, result='current')
    if previous.get('target') == target and previous.get('result') in ('rolled_back', 'needs_operator'):
        return print('This release failed here before. An administrator must review it.')
    if previous.get('target') == target and time.time() - previous.get('at', 0) < RETRY_SECONDS:
        return print('Waiting before the next attempt.')
    running = api(values, '/v1/dashboard')['runtime']['version']
    if version(running) > version(manifest['version']):
        return record(target=target, result='ahead', running=running)  # Never downgrade.
    if shutil.disk_usage('/').free < 3 * (stage / 'cloudroom').stat().st_size + 1_000_000_000:
        return record(target=target, result='waiting', reason='low disk space')
    if reason := busy(values):
        return record(target=target, result='waiting', reason=reason)
    notes = []
    optional('Claude Code', lambda: ensure_claude(values), notes)  # Before the gate: users wait only for the swap.
    try:
        gate(values, True)
        time.sleep(2)
        if reason := busy(values):
            return record(target=target, result='waiting', reason=reason)
        _, before = sessions(values)
        old = sha(BINARY)
        backup = BACKUPS / f'auto-{time.strftime("%Y%m%d-%H%M%S")}-{old[:12]}'
        backup.mkdir(mode=0o700, parents=True)
        had_previews = preview_state(values).exists()
        for path in [BINARY, *(ROOT / name for name in FILES)]:
            if path.exists():
                shutil.copy2(path, backup / path.name)
        try:
            run('systemctl', 'stop', 'cloudroom.service')
            copy_release(stage, manifest)
            guard_guide()
            optional('previews', lambda: previews(values), notes)
            verify(values, before, manifest['sha256'], manifest.get('features', []), start())
        except Exception as error:
            try:
                run('systemctl', 'stop', 'cloudroom.service')
                if not had_previews and preview_state(values).exists():  # A new preview setup may be what failed.
                    shutil.move(preview_state(values), backup / 'previews-failed')
                for name in FILES:
                    if not (backup / name).exists():
                        (ROOT / name).unlink(missing_ok=True)
                for name in ('cloudroom', *FILES):
                    if (backup / name).is_file():
                        write(ROOT / name, (backup / name).read_bytes(), 0o755 if name == 'cloudroom' else 0o644)
                verify(values, before, old, [], start())
                record(target=target, result='rolled_back', error=str(error)[:500], backup=str(backup))
            except Exception as recovery:
                record(target=target, result='needs_operator', error=str(error)[:500], recovery=str(recovery)[:500], backup=str(backup))
            sys.exit(1)
    finally:
        gate(values, False)
    record(target=target, result='updated', previous=old[:12], backup=str(backup), notes=notes)
    prune(stage)


def main():
    if os.geteuid() != 0:
        sys.exit('Run as root.')
    if sys.argv[1:] == ['unblock']:
        return gate(service_env(), False)
    if len(sys.argv) != 3 or sys.argv[1] not in ('install', 'upgrade'):
        sys.exit(__doc__)
    stage = Path(sys.argv[2]).resolve()
    manifest = json.loads((stage / 'update-manifest.json').read_text())
    lock = open('/run/lock/cloudroom-update.lock', 'a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit('Another update is running.')
    if sys.argv[1] == 'install':
        # A new VM has no sessions yet: install everything, then configure.py starts the core.
        copy_release(stage, manifest)
        guard_guide()
        values, notes = service_env(), []
        optional('Claude Code', lambda: ensure_claude(values), notes)
        optional('previews', lambda: previews(values), notes)
        return record(target=manifest['commit'], result='installed', notes=notes)
    try:
        upgrade(stage, manifest)
    except SystemExit:
        raise
    except Exception as error:  # Record it, so the retry delay applies to unexpected failures too.
        record(target=manifest['commit'], result='failed', error=f'{type(error).__name__}: {error}'[:500])
        sys.exit(1)


if __name__ == '__main__':
    main()
