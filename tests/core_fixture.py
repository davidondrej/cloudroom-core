#!/usr/bin/env python3
"""Deterministic Rust HTTP/Codex-protocol checks; no inference or database required."""
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from core_e2e import Service, run, until, next_event


def codex():
    native = str(uuid.uuid4())
    path = Path(os.environ["CODEX_HOME"]) / "sessions" / (native + ".jsonl")
    path.parent.mkdir(exist_ok=True)
    path.write_text('{"fixture":"start"}\n')
    children, turn, slow_exit, goal = {}, "", False, None
    login_cancelled = threading.Event()

    def finish_login(login_id):
        while not login_cancelled.wait(.02):
            if Path('auth-finish').exists():
                success = Path('auth-finish').read_text() == 'success'
                if success: Path('auth-state').write_text('ready')
                send({'method': 'account/login/completed', 'params': {'loginId': login_id, 'success': success, 'error': None if success else 'PRIVATE-AUTH-ERROR-CANARY'}})
                return

    def send(message):
        print(json.dumps(message), flush=True)

    def event(method, **params):
        send({"method": method, "params": {"threadId": native, **params}})

    try:
        for line in sys.stdin:
            message = json.loads(line)
            if "id" not in message:
                continue
            method, params, result = message["method"], message.get("params", {}), {}
            if method == 'account/read':
                mode = Path('auth-state').read_text() if Path('auth-state').exists() else 'ready'
                result = {'account': None if mode == 'missing' else {'type': 'chatgpt', 'email': 'fixture@example.invalid', 'planType': 'plus'}, 'requiresOpenaiAuth': True}
            elif method == 'account/rateLimits/read':
                mode = Path('auth-state').read_text() if Path('auth-state').exists() else 'ready'
                if mode in ('offline', 'unauthorized'):
                    send({'id': message['id'], 'error': {'code': -1, 'message': '401 Unauthorized' if mode == 'unauthorized' else 'Network unavailable'}})
                    continue
                result = {'rateLimits': {'primary': {'usedPercent': 100 if mode in ('limited', 'credits') else 0}, 'secondary': None, 'credits': {'hasCredits': mode == 'credits', 'unlimited': False}}}
            elif method == 'account/login/start':
                login_id = 'fixture-login'
                with Path('auth-attempts').open('a') as file: file.write('login\n')
                result = {'type': 'chatgptDeviceCode', 'loginId': login_id, 'verificationUrl': 'https://auth.openai.com/codex/device', 'userCode': 'TEST-1234'}
                threading.Thread(target=finish_login, args=(login_id,), daemon=True).start()
            elif method == 'account/login/cancel':
                login_cancelled.set()
                result = {'status': 'canceled'}
            elif method == "model/list":
                result = {"data": [
                    {"model": "fixture", "supportedReasoningEfforts": [{"reasoningEffort": level} for level in ["low", "medium", "high", "xhigh", "max"]]},
                    {"model": "basic", "supportedReasoningEfforts": [{"reasoningEffort": "high"}]},
                ], "nextCursor": None}
            elif method == "thread/start":
                with path.open("a") as file:
                    file.write(json.dumps({"fixture": "launch", "reasoning": params.get("config", {}).get("model_reasoning_effort")}) + "\n")
                slow_exit = params["model"] == "slow-exit"
                result = {"thread": {"id": native, "path": str(path)}, "model": params["model"]}
            elif method == "thread/resume":
                if Path("hold-resume").exists():
                    Path("resume-ready").touch()
                    Path(params['threadId']+'.resume-ready').touch()
                    while not Path("release-resume").exists():
                        time.sleep(.01)
                if Path("reject-resume").exists():
                    send({"id": message["id"], "error": {"code": -32602, "message": "resume rejected"}})
                    continue
                # Reattach to the caller's existing rollout and append to the same file.
                native = params["threadId"]
                path = Path(os.environ["CODEX_HOME"]) / "sessions" / (native + ".jsonl")
                result = {"thread": {"id": native, "path": str(path)}, "model": params["model"]}
            elif method == "turn/start":
                with path.open("a") as file:
                    file.write(json.dumps({"fixture": "turn", "reasoning": params.get("effort"), "text": params["input"][0]["text"]}) + "\n")
                with Path(native + ".requests").open("a") as audit:
                    audit.write(params["clientUserMessageId"] + "\n")
                text = params["input"][0]["text"]
                if text == "crash":
                    Path("crash-ready").touch()
                    while not Path("release-crash").exists():
                        time.sleep(.01)
                    os._exit(1)
                if text in ("reject", "late-reject"):
                    if text == "late-reject":
                        Path("reject-ready").touch()
                        while not Path("release-reject").exists():
                            time.sleep(.01)
                    send({"id": message["id"], "error": {"code": -32602, "message": "fixture rejection"}})
                    if text == "late-reject":
                        while not Path("finish-reject").exists():
                            time.sleep(.01)
                    continue
                if text == "no-reply":
                    Path("no-reply").touch()
                    continue
                if text == "delay":
                    while not Path("release").exists():
                        time.sleep(.02)
                turn = str(uuid.uuid4())
                reply = {"id": message["id"], "result": {"turn": {"id": turn}}}
                if text == "reply-first":
                    send(reply)
                    time.sleep(.05)
                event("turn/started", turn={"id": turn, "status": "inProgress"})
                if text == "finish-first":
                    event("turn/completed", turn={"id": turn, "status": "completed"})
                    time.sleep(.05)
                    send(reply)
                    continue
                if text != "reply-first":
                    send(reply)
                if text == "hold":
                    job = f"from pathlib import Path; import time\nfor n in range(300):\n Path('{native}.ticks').write_text(str(n)); time.sleep(.1)\n"
                    child = subprocess.Popen([sys.executable, "-c", job], start_new_session=True,
                                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    children[str(child.pid)] = child
                    Path(native + ".pid").write_text(str(child.pid))
                    event("item/started", turnId=turn, item={"id": "tool", "type": "commandExecution", "processId": str(child.pid)})
                elif text == "hang":
                    time.sleep(30)
                else:
                    event("item/agentMessage/delta", turnId=turn, itemId="text", delta="hello")
                    event("turn/completed", turn={"id": turn, "status": "completed"})
                if text == "goal":
                    # Codex continues an active goal on its own: a turn nobody requested.
                    goal = {"objective": "fixture goal", "status": "active", "tokenBudget": None, "tokensUsed": 0, "timeUsedSeconds": 0}
                    turn = str(uuid.uuid4())
                    event("turn/started", turn={"id": turn, "status": "inProgress"})
                    event("item/agentMessage/delta", turnId=turn, itemId="goal", delta="continuing")
                    Path("goal-turn").write_text(turn)
                continue
            elif method == "thread/goal/get":
                result = {"goal": goal}
            elif method == "thread/goal/set":
                with Path(native + ".goal").open("a") as file:
                    file.write(json.dumps(params) + "\n")
                goal = {**(goal or {}), "status": params.get("status", "active")}
                result = {"goal": goal}
            elif method == "turn/interrupt":
                event("turn/completed", turn={"id": turn, "status": "interrupted"})
            elif method == "thread/backgroundTerminals/terminate":
                child = children.pop(params["processId"], None)
                if child:
                    child.kill(); child.wait()
                result = {"terminated": child is not None}
            elif method == "thread/backgroundTerminals/clean":
                for child in children.values():
                    child.kill(); child.wait()
                children.clear()
            elif method == "turn/steer":
                if params.get("expectedTurnId") != turn:
                    send({"id": message["id"], "error": {"code": -32602, "message": "steer target is no longer active"}})
                    continue
            elif method == "thread/compact/start":
                mode = Path('compact-mode').read_text() if Path('compact-mode').exists() else 'legacy'
                if mode == 'reject':
                    send({'id': message['id'], 'error': {'code': -32602, 'message': 'compact rejected'}})
                    continue
                if mode == 'legacy':
                    event("thread/compacted", turn={"id": turn or "compact", "status": "completed"})
                else:
                    send({'id': message['id'], 'result': {}})
                    turn = str(uuid.uuid4())
                    event('turn/started', turn={'id': turn, 'status': 'inProgress'})
                    event('item/started', turnId=turn, item={'id': 'compact', 'type': 'contextCompaction'})
                    event('turn/completed', threadId='another-thread', turn={'id': turn, 'status': 'completed'})
                    event('turn/completed', turn={'id': 'another-turn', 'status': 'completed'})
                    Path('compact-ready').touch()
                    while not Path('release-compact').exists(): time.sleep(.01)
                    event('item/completed', turnId=turn, item={'id': 'compact', 'type': 'contextCompaction'})
                    event('turn/completed', turn={'id': turn, 'status': mode})
                    event('turn/completed', turn={'id': turn, 'status': mode})
                    continue
            elif method == "thread/fork":
                if Path("hold-fork").exists():
                    Path("fork-ready").touch()
                    while not Path("release-fork").exists():
                        time.sleep(.01)
                forked = str(uuid.uuid4())
                forked_path = Path(os.environ["CODEX_HOME"]) / "sessions" / (forked + ".jsonl")
                forked_path.write_text('{"fixture":"fork"}\n')
                result = {"thread": {"id": forked, "path": str(forked_path)}}
                native, path = forked, forked_path
            send({"id": message["id"], "result": result})
    finally:
        if slow_exit:
            with open(os.devnull, "w") as sink:
                os.dup2(sink.fileno(), 1)
            time.sleep(.6)
        for child in children.values():
            child.kill(); child.wait()
        with path.open("a") as file:
            file.write('{"fixture":"shutdown"}\n')


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="cloudroom-http-fixture-")
        self.root = Path(self.directory.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        run("git", "init", "--quiet", str(self.repo))
        home = self.root / "home"
        (home / ".codex").mkdir(parents=True)
        self.state = self.root / "state"
        self.state.mkdir()
        # The database is a missing socket: it fails at once, where a refused port is retried for 2s on every shutdown.
        self.env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin", "CLOUDROOM_UNPROTECTED_TEST_MODE": "1", "CLOUDROOM_LISTEN": "127.0.0.1:0",
            "CLOUDROOM_TOKEN": "fixture-token-" + "x" * 32, "CLOUDROOM_STATE_DIR": str(self.state),
            "CLOUDROOM_REPOSITORY": str(self.repo), "CLOUDROOM_DATABASE_URL": f"postgres://localhost/fixture?host={self.root}/no-database",
            "CLOUDROOM_STORE": "fixture", "CLOUDROOM_ALLOW_INSECURE_DATABASE": "1",
            "CLOUDROOM_CODEX_BINARY": str(Path(__file__).resolve()), "CLOUDROOM_ACCOUNT_HOME": str(home),
            "CLOUDROOM_CODEX_HOME": str(home / ".codex"), "CLOUDROOM_MODEL": "fixture",
        }
        self.sequence, self.service = 0, None

    def tearDown(self):
        if self.service:
            self.service.stop()
        self.directory.cleanup()

    def append(self, session, data, native=None):
        self.sequence += 1
        record = {"sequence": self.sequence, "session_id": session, "kind": "text_delta", "data": data}
        if native is not None:
            record["native"] = native
        file = self.state / f"{self.sequence:020}.record"
        file.write_text(json.dumps(record))
        return file

    def start(self):
        self.service = Service(self.env, self.root / "service.log").start()

    def test_codex_login_is_private_idempotent_cancellable_and_verified(self):
        from workspaces import WorkspaceTests
        WorkspaceTests.database(self)
        (self.repo / 'auth-state').write_text('missing')
        self.start()
        api = self.service.request
        api('GET', '/v1/accounts/codex', expected=401, token=None)
        self.assertEqual(api('GET', '/v1/accounts/codex')['state'], 'missing')
        start = {'request_id': 'auth-gated', 'harness': 'codex', 'model': 'fixture'}
        self.assertEqual(api('POST', '/v1/sessions', start, 409)['code'], 'codex_auth_required')
        first = api('POST', '/v1/accounts/codex/login', {'request_id': 'login-one'}, 202)
        self.assertEqual(first['state'], 'waiting')
        self.assertEqual(first['user_code'], 'TEST-1234')
        self.assertEqual(api('POST', '/v1/accounts/codex/login', {'request_id': 'login-one'}, 202), first)
        self.assertEqual(api('POST', '/v1/accounts/codex/cancel', {'request_id': 'stale-login'}, 202), first)
        self.assertEqual((self.repo / 'auth-attempts').read_text().splitlines(), ['login'])
        self.assertEqual(api('POST', '/v1/accounts/codex/cancel', {'request_id': 'login-one'}, 202)['state'], 'missing')
        second = api('POST', '/v1/accounts/codex/login', {'request_id': 'login-two'}, 202)
        self.assertEqual(second['state'], 'waiting')
        (self.repo / 'auth-finish').write_text('success')
        until(lambda: api('GET', '/v1/accounts/codex')['state'] == 'connected', 'verified cloud login', 6)
        self.assertEqual(api('GET', '/v1/accounts/codex')['email'], 'fixture@example.invalid')
        self.assertIsNone(api('GET', '/v1/accounts/codex')['user_code'])
        self.assertEqual(list(self.state.glob('*.record')), [], 'Login must not create sessions or conversation records')
        for file in self.state.rglob('*.jsonl'):
            self.assertNotIn('TEST-1234', file.read_text())
            self.assertNotIn('PRIVATE-AUTH-ERROR-CANARY', file.read_text())
        sid = api('POST', '/v1/sessions', start, 202)['session_id']
        self.assertEqual(api('POST', '/v1/sessions', start, 202)['session_id'], sid)
        until(lambda: self.service.session(sid)['state'] == 'idle', 'authenticated start', 10)
        prompt = {'request_id': 'once', 'text': 'hello'}
        api('POST', f'/v1/sessions/{sid}/prompts', prompt, 202)
        until(lambda: self.service.session(sid)['receipts']['once']['state'] == 'completed', 'first authenticated task', 10)
        api('POST', f'/v1/sessions/{sid}/prompts', prompt, 202)
        native = self.service.session(sid)['native_id']
        self.assertEqual((self.repo / (native + '.requests')).read_text().splitlines(), ['once'])

    def test_codex_account_failure_and_limits_are_not_confused_with_login(self):
        for mode, expected in [('offline', 'unavailable'), ('unauthorized', 'missing'), ('limited', 'limited'), ('credits', 'connected')]:
            with self.subTest(mode=mode):
                (self.repo / 'auth-state').write_text(mode)
                self.start()
                self.assertEqual(self.service.request('GET', '/v1/accounts/codex')['state'], expected)
                if mode == 'offline':
                    self.assertEqual(self.service.request('POST', '/v1/accounts/codex/login', {'request_id': 'must-not-replace'}, 202)['state'], expected)
                    self.assertFalse((self.repo / 'auth-attempts').exists())
                if mode == 'limited':
                    # A limited account may switch to another subscription.
                    self.assertEqual(self.service.request('POST', '/v1/accounts/codex/login', {'request_id': 'switch'}, 202)['state'], 'waiting')
                self.service.stop()
        (self.repo / 'auth-state').write_text('missing')
        self.start()
        self.service.request('POST', '/v1/accounts/codex/login', {'request_id': 'rejected'}, 202)
        (self.repo / 'auth-finish').write_text('failure')
        until(lambda: self.service.request('GET', '/v1/accounts/codex')['state'] == 'error', 'failed login', 6)
        self.assertNotIn('PRIVATE-AUTH-ERROR-CANARY', json.dumps(self.service.request('GET', '/v1/accounts/codex')))

    def test_non_loopback_http_requires_permission_before_storage_or_binding(self):
        binary = Path(__file__).resolve().parents[1] / 'target/debug/cloudroom'
        remote = ['0.0.0.0:0', '[::]:0', '192.0.2.10:0', '10.0.0.10:0', '[2001:db8::10]:0']
        for listen in remote:
            for permission in [None, '', '0', 'true', '01', ' 1 ', '1']:
                for unprotected in [False, True]:
                    with self.subTest(listen=listen, permission=permission, unprotected=unprotected):
                        env = {**self.env, 'CLOUDROOM_LISTEN': listen}
                        if not unprotected:
                            env.pop('CLOUDROOM_UNPROTECTED_TEST_MODE')
                        if permission is not None:
                            env['CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP'] = permission
                        result = subprocess.run([str(binary)], env=env, capture_output=True, text=True, timeout=5)
                        self.assertNotEqual(result.returncode, 0)
                        expected = ('CLOUDROOM_UNPROTECTED_TEST_MODE requires a loopback listener' if unprotected else
                                    'CLOUDROOM_STORAGE_POLICY is required' if permission == '1' else
                                    'CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP=1')
                        self.assertIn(expected, result.stderr)
                        self.assertNotIn('Cloudroom listening on ', result.stderr)
                        self.assertNotIn(env['CLOUDROOM_TOKEN'], result.stderr)
        self.assertEqual(list(self.state.iterdir()), [])

    def test_loopback_http_needs_no_permission_and_remains_authenticated(self):
        for listen in ['127.0.0.1:0', '[::1]:0']:
            with self.subTest(listen=listen):
                self.env['CLOUDROOM_LISTEN'] = listen
                self.start()
                try:
                    self.service.request('GET', '/v1/health', expected=401, token=None)
                    self.service.request('GET', '/v1/health')
                finally:
                    self.service.stop()

    def test_harness_stderr_is_bounded_local_only_for_start_resume_and_discovery(self):
        marker = 'SYNTHETIC_PRIVATE_STDERR_CREDENTIAL_7f54dcb2'
        binary = self.root / 'stderr-harness'
        binary.write_text('#!/usr/bin/env python3\nimport os, sys\n'
                          'if "--version" in sys.argv:\n print("0.85.1"); sys.exit(0)\n'
                          'payload = b"x" * (4 * 1024 * 1024) + b"\\xff\\x00" + '
                          + repr(marker.encode()) + '\n'
                          'while payload:\n n = os.write(2, payload); payload = payload[n:]\n'
                          'os._exit(42)\n')
        binary.chmod(0o700)
        self.env['CLOUDROOM_CODEX_BINARY'] = str(binary)
        pi_home = self.root / 'home/.pi'; pi_home.mkdir()
        self.env.update(CLOUDROOM_PI_BINARY=str(binary), CLOUDROOM_PI_HOME=str(pi_home),
                        CLOUDROOM_PI_PROVIDER='fixture')
        native = Path(self.env['CLOUDROOM_CODEX_HOME']) / 'sessions/resumed.jsonl'
        native.parent.mkdir(); native.write_text('{"fixture":"seed"}\n')
        seeds = [('codex', 'receipt', {'request_id':'codex','command':'start','input':{},'state':'accepted'}),
                 ('pi', 'receipt', {'request_id':'pi','command':'start','input':{'harness':'pi'},'state':'accepted'}),
                 ('resumed', 'receipt', {'request_id':'resumed','command':'start','input':{},'state':'completed'}),
                 ('resumed', 'native_identity', {'id':'resumed','path':str(native)}),
                 ('resumed', 'state', {'state':'idle'})]
        for seq, (sid, kind, data) in enumerate(seeds, 1):
            (self.state / f'{seq:020}.record').write_text(json.dumps(
                {'sequence':seq,'session_id':sid,'kind':kind,'data':data}))
        self.start()
        until(lambda: all(self.service.session(s)['state'] in ['failed','process_lost']
                          for s in ['codex','pi','resumed']), 'failed harnesses', 15)
        capabilities = self.service.request('GET', '/v1/capabilities')
        self.assertIsNone(next(h for h in capabilities['harnesses'] if h['id']=='codex')['models'])
        private = self.state / 'harness-diagnostics'
        def captures():
            try:
                return [json.loads(line) for p in private.glob('stderr*.jsonl') for line in p.read_text().splitlines()]
            except (FileNotFoundError, json.JSONDecodeError):
                return []
        until(lambda: len(captures()) == 4, 'protected stderr captures', 15)
        captured = captures()
        self.assertEqual({r['session_id'] for r in captured}, {'codex','pi','resumed',None})
        for r in captured:
            self.assertEqual(r['exit_code'], 42)
            self.assertTrue(r['stderr_complete'] and r['stderr_truncated'])
            self.assertEqual(r['stderr_bytes'], 4 * 1024 * 1024 + 2 + len(marker))
            self.assertTrue(r['stderr'].endswith('\ufffd\x00' + marker))
            self.assertEqual(len(r['stderr']), 16 * 1024)
        responses = [capabilities, self.service.request('GET', '/v1/dashboard')]
        for sid in ['codex','pi','resumed']:
            responses.append(self.service.session(sid))
            records = self.service.records(sid); responses.append(records)
            capture = next(r for r in captured if r['session_id'] == sid)
            diagnostic_id = f'{capture["run_id"]}-{capture["sequence"]}'
            self.assertTrue(any(r['data'].get('diagnostic_id') == diagnostic_id for r in records))
            connection, stream = self.service.stream(sid)
            try:
                responses.extend(next_event(stream) for _ in records)
            finally:
                stream.close(); connection.close()
            # Crash stderr tails are shown to the session owner (ADR 0123), bounded to 2000 bytes.
            tails = [r['data']['stderr'] for r in records if r['data'].get('stderr')]
            self.assertTrue(tails and tails[-1].endswith(marker) and len(tails[-1]) <= 2000)
        self.assertNotIn(marker, json.dumps(responses[:2]))
        self.service.request('GET', '/v1/health')
        self.service.stop()
        self.assertEqual(private.stat().st_mode & 0o777, 0o700)
        for path in private.glob('*.jsonl'):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertLessEqual(path.stat().st_size, 1024 * 1024)
        for path in [*self.state.glob('diagnostics*.jsonl'), self.root / 'service.log']:
            self.assertNotIn(marker, path.read_text())
        before = {p.name:p.read_bytes() for p in private.glob('*.jsonl')}
        private.chmod(0o755)  # An unsafe sink must lose diagnostics, not stop execution or leak text.
        try:
            self.start()
            self.service.request('GET', '/v1/capabilities')
            until(lambda: 'private harness diagnostic write failed' in (self.root / 'service.log').read_text(),
                  'rejected unsafe diagnostic sink', 15)
            self.service.request('GET', '/v1/health')
            self.assertEqual(before, {p.name:p.read_bytes() for p in private.glob('*.jsonl')})
            self.assertNotIn(marker, (self.root / 'service.log').read_text())
        finally:
            self.service.stop()
            private.chmod(0o700)

    def test_restart_limits_concurrent_startup_not_loaded_sessions(self):
        total = (os.cpu_count() or 1) + 2
        directory = Path(self.env['CLOUDROOM_CODEX_HOME'])/'sessions'; directory.mkdir()
        sequence = 0
        for i in range(total):
            native = 'batch-'+str(i); path = directory/(native+'.jsonl'); path.write_text('{"fixture":"seed"}\n')
            for kind,data in [('receipt',{'request_id':native,'command':'start','input':{},'state':'completed'}),
                              ('native_identity',{'id':native,'path':str(path)}),('state',{'state':'idle'})]:
                sequence += 1
                (self.state/f'{sequence:020}.record').write_text(json.dumps({'sequence':sequence,'session_id':'cr_'+native,'kind':kind,'data':data}))
        (self.repo/'hold-resume').touch(); self.start()
        try:
            until(lambda:list(self.repo.glob('batch-*.resume-ready')),'first restore batch',8)
            time.sleep(2)
            waiting = len(list(self.repo.glob('batch-*.resume-ready')))
            self.assertLessEqual(waiting,max(1,(os.cpu_count() or 1)//2))
            self.assertLess(waiting,total)
        finally: (self.repo/'release-resume').touch()
        until(lambda:all(self.service.session('cr_batch-'+str(i))['state']=='idle' for i in range(total)),'all sessions restored',30)
        self.assertEqual(len(list(self.repo.glob('batch-*.resume-ready'))),total)
        for i in range(total): self.assertEqual(self.service.session('cr_batch-'+str(i))['native_id'],'batch-'+str(i))
        self.service.stop()
        (self.repo/'release-resume').unlink()
        for path in self.repo.glob('batch-*.resume-ready'): path.unlink()
        self.start()
        until(lambda:list(self.repo.glob('batch-*.resume-ready')),'restart during recovery',8)
        self.service.stop()
        (self.repo/'release-resume').touch(); self.start()
        until(lambda:all(self.service.session('cr_batch-'+str(i))['state']=='idle' for i in range(total)),'interrupted recovery is resumable',30)

    def test_completed_http_requests_release_gateway_connections(self):
        import http.client
        self.append('saved', {'text': 'fixture'})
        self.start()
        connections = []
        try:
            for _ in range(12):
                connection = http.client.HTTPConnection(self.service.address, timeout=5)
                connections.append(connection)
                connection.request('GET', '/v1/health', headers={'Authorization': 'Bearer ' + self.env['CLOUDROOM_TOKEN'], 'Connection': 'keep-alive'})
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                response.read()
                self.assertTrue(response.will_close, 'Completed proxy requests must not occupy core descriptors indefinitely')
                self.assertIsNone(connection.sock)
            stream = http.client.HTTPConnection(self.service.address, timeout=5)
            connections.append(stream)
            stream.request('GET', '/v1/sessions/saved/stream', headers={'Authorization': 'Bearer ' + self.env['CLOUDROOM_TOKEN']})
            response = stream.getresponse()
            self.assertEqual(response.status, 200)
            self.assertTrue(response.getheader('Content-Type').startswith('text/event-stream'))
            self.assertFalse(response.will_close)
            self.assertTrue(response.readline())
            response.close()
        finally:
            for connection in connections:
                connection.close()

    def test_session_workspace_reports_live_cloud_checkout(self):
        run("git", "-C", str(self.repo), "symbolic-ref", "HEAD", "refs/heads/cloud-branch")
        (self.state / "00000000000000000001.record").write_text(json.dumps({
            "sequence": 1, "session_id": "cr_checkout", "kind": "receipt",
            "data": {"request_id": "checkout", "command": "start", "input": {}, "state": "completed",
                     "workspace": {"id": "checkout", "path": str(self.repo)}}}))
        self.start()
        path = "/v1/sessions/cr_checkout/workspace"
        self.service.request("GET", path, expected=401, token=None)
        workspace = self.service.request("GET", path)
        assert workspace == {"path": str(self.repo), "branch": "cloud-branch", "head": None}
        run("git", "-C", str(self.repo), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "--allow-empty", "-m", "fixture")
        workspace = self.service.request("GET", path)
        assert workspace["branch"] == "cloud-branch" and len(workspace["head"]) == 40
        run("git", "-C", str(self.repo), "checkout", "--detach")
        detached = self.service.request("GET", path)
        assert detached["branch"] is None and detached["head"] == workspace["head"]
        run("git", "-C", str(self.repo), "checkout", "-b", "changed-in-cloud")
        assert self.service.request("GET", path)["branch"] == "changed-in-cloud"
        shutil.rmtree(self.repo / ".git")
        assert self.service.request("GET", path) == {"path": str(self.repo), "branch": None, "head": None}

    def test_core_boots_before_agent_setup_and_does_not_fake_database_readiness(self):
        for key in ['CLOUDROOM_CODEX_BINARY', 'CLOUDROOM_CODEX_HOME', 'CLOUDROOM_MODEL', 'CLOUDROOM_REPOSITORY']:
            self.env.pop(key)
        self.start()
        data = self.service.request('GET', '/v1/dashboard')
        self.assertFalse(data['runtime']['configured'])
        self.assertEqual(data['sessions'], [])
        self.service.request('GET', '/v1/ready', expected=401, token=None)
        self.assertFalse(self.service.request('GET', '/v1/ready', expected=503)['ready'])
        error = self.service.request('POST', '/v1/sessions', {'request_id': 'not-configured'}, 409)
        self.assertIn('setup is incomplete', error['error'])

    def test_dashboard_is_authenticated_bounded_and_contains_only_summaries(self):
        for n in range(1001):
            record = {"sequence": n + 1, "session_id": f"session-{n}", "kind": "state",
                      "data": {"state": "closed", "secret": "SECRET-CANARY"}, "native": "PRIVATE-TRANSCRIPT"}
            (self.state / f"{n + 1:020}.record").write_text(json.dumps(record))
        self.start()
        self.service.request("GET", "/v1/dashboard", expected=401, token=None)
        self.service.request("GET", "/v1/dashboard", expected=401, token="wrong")
        data = self.service.request("GET", "/v1/dashboard")
        self.assertEqual(data["storage"], self.service.request("GET", "/v1/health")["storage"])
        self.assertFalse(data["storage"]["enabled"])
        for key in ["workspace_available_bytes", "history_available_bytes", "workspace_total_bytes", "history_total_bytes", "sampled_at"]:
            self.assertIsNone(data["storage"][key])
        self.assertEqual(data["sessionCount"], 1001)
        self.assertEqual(len(data["sessions"]), 1000)
        self.assertEqual(data["sessions"][0]["id"], "session-1000")
        self.assertEqual(data["sessions"][0]["state"], "stopped")
        self.assertIsNone(data["sessions"][0]["lastActivity"])
        self.assertIsNone(data["sessions"][0]["model"])
        self.assertEqual(data["capabilities"], {"settings": True, "updates": False})
        self.assertTrue(self.service.request("GET", "/v1/settings")["autoSync"])
        self.assertEqual(data["appConnectivity"], "unknown")
        self.assertEqual(data["onboarding"], {"localConnected": None, "offlineTaskVerified": None})
        for secret in ["SECRET-CANARY", "PRIVATE-TRANSCRIPT", str(self.root), self.env["CLOUDROOM_TOKEN"], "receipts", "native_path"]:
            self.assertNotIn(secret, json.dumps(data))
        self.service.request("GET", "/v1/sessions", expected=401, token="wrong")
        listed = self.service.request("GET", "/v1/sessions")
        self.assertEqual((listed["total"], len(listed["sessions"])), (1001, 1000))
        self.assertEqual({k: listed["sessions"][0][k] for k in ("session_id", "state", "queued")},
                         {"session_id": "session-1000", "state": "closed", "queued": 0})
        self.assertNotIn("SECRET-CANARY", json.dumps(listed))
        self.assertNotIn("PRIVATE-TRANSCRIPT", json.dumps(listed))
        until(lambda: self.service.request("GET", "/v1/dashboard")["sampledAt"] is not None, "resource sampling", 15)
        sampled = self.service.request("GET", "/v1/dashboard")
        self.assertLessEqual(sampled["sampledAt"], int(time.time() * 1000))
        self.assertTrue(all(v is None or 0 <= v <= 100 for v in sampled["resources"].values()))

    def test_replay_does_not_read_other_sessions(self):
        self.append("a", {"delta": "first"})
        other = self.append("b", {"delta": "unrelated"})
        self.append("a", {"delta": "last"})
        self.start()
        # A damaged unrelated file must not be opened to answer session A's read.
        other.write_text("unreadable unrelated history")
        records = self.service.records("a")
        self.assertEqual([r["data"]["delta"] for r in records if r["kind"] == "text_delta"], ["first", "last"])

    def test_compact_storage_preserves_legacy_wire_fields(self):
        params = {"threadId": "native", "itemId": "text", "delta": "hello"}
        raw = json.dumps({"method": "item/agentMessage/delta", "params": params}) + "\n"
        self.append("a", {"method": "item/agentMessage/delta", "delta": "hello"}, raw)
        self.start()
        record = self.service.records("a")[0]
        self.assertEqual(record["data"].get("value"), params)
        self.assertEqual(record["native"], raw)

    def test_slow_stream_cannot_hold_shutdown(self):
        for _ in range(64):
            self.append("a", {"delta": "x" * (128 * 1024)})
        self.start()
        host, port = self.service.address.rsplit(":", 1)
        client = socket.create_connection((host, int(port)))
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        client.sendall(("GET /v1/sessions/a/stream HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer "
                        + self.env["CLOUDROOM_TOKEN"] + "\r\n\r\n").encode())
        try:
            self.assertIn(b"200", client.recv(256))
            time.sleep(.2)
            self.service.process.send_signal(signal.SIGTERM)
            self.service.process.wait(timeout=7)
            self.assertEqual(self.service.process.returncode, 0)
        finally:
            client.close()


class RecoveryTests(unittest.TestCase):
    """Actual Rust HTTP service; only Codex and the crash-boundary journal are fixtures."""
    setUp, tearDown, start = ReplayTests.setUp, ReplayTests.tearDown, ReplayTests.start
    def seed(self, extra=()):
        native = "recovery-native"
        path = Path(self.env["CLOUDROOM_CODEX_HOME"]) / "sessions" / (native + ".jsonl")
        path.parent.mkdir(); path.write_text('{"fixture":"seed"}\n')
        records = [("receipt", {"request_id": "seed", "command": "start", "input": {}, "state": "completed"}),
                   ("native_identity", {"id": native, "path": str(path)}), ("state", {"state": "idle"}), *extra]
        for n, (kind, data) in enumerate(records, 1):
            (self.state / f"{n:020}.record").write_text(json.dumps({"sequence": n, "session_id": "cr_seed", "kind": kind, "data": data}))
        self.native = native

    def prompt(self, key, text):
        return self.service.request("POST", "/v1/sessions/cr_seed/prompts", {"request_id": key, "text": text}, 202)

    def status(self):
        return self.service.session("cr_seed")

    def wait(self, check, label):
        return until(check, label, 6)

    def ready(self):
        self.wait(lambda: self.status()["state"] == "idle", "resumed idle session")
        self.assertEqual(self.status()["native_id"], self.native)

    def test_malformed_rollout_does_not_poison_the_journal_or_service_restart(self):
        self.seed(); self.start(); self.ready()
        path = Path(self.status()['native_path'])
        with path.open('ab') as output:
            output.write(b'{"before":true}\n\xff\n{"after":true}\n'); output.flush(); os.fsync(output.fileno())
        self.wait(lambda:self.status()['state']=='process_lost','malformed history isolated')
        self.assertEqual(self.service.request('GET','/v1/health')['status'],'ready')
        self.service.stop(); self.start()
        self.assertEqual(self.status()['state'],'process_lost')
        self.assertIn(b'\xff',path.read_bytes(), 'corrupt source must remain intact')
        records = self.service.records('cr_seed')
        self.assertTrue(any(r['kind']=='native_history_unavailable' for r in records))

    def test_occupied_port_does_not_change_saved_recovery_state(self):
        self.seed([('receipt',{'request_id':'queued','command':'prompt','input':{'text':'hello'},'state':'accepted'})])
        before = {p.name:p.read_bytes() for p in self.state.glob('*.record')}
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1',0)); occupied.listen()
            env = {**self.env,'CLOUDROOM_LISTEN':'127.0.0.1:'+str(occupied.getsockname()[1])}
            failed = subprocess.run([str(Path(__file__).resolve().parents[1]/'target/debug/cloudroom')],env=env,capture_output=True,timeout=8)
            self.assertNotEqual(failed.returncode,0)
            self.assertEqual(before,{p.name:p.read_bytes() for p in self.state.glob('*.record')})
        self.start()
        self.wait(lambda:self.status()['receipts']['queued']['state']=='completed','queued work preserved')
        self.assertEqual(self.status()['native_id'],self.native)

    def test_removed_harness_fails_only_its_pending_session(self):
        self.seed()
        cases = {'removed': {'model': 'fixture'}, 'provider': {'provider': 'fixture'},
                 'both': {'model': 'fixture', 'provider': 'fixture'}, 'legacy': {}}
        sequence = 3
        for key, overrides in cases.items():
            for receipt in [
                {'request_id': key, 'command': 'start', 'input': {'harness': 'pi'},
                 'state': 'accepted', **overrides},
                {'request_id': 'queued', 'command': 'prompt', 'input': {'text': 'must not run'},
                 'state': 'accepted'},
            ]:
                sequence += 1
                (self.state / f'{sequence:020}.record').write_text(json.dumps({
                    'sequence': sequence, 'session_id': 'cr_' + key, 'kind': 'receipt', 'data': receipt}))
        before = {p.name: p.read_bytes() for p in self.state.glob('*.record')}
        self.start(); self.ready()
        failed_history = {}
        for key in cases:
            with self.subTest(overrides=key):
                sid = 'cr_' + key
                self.wait(lambda: self.service.session(sid)['receipts']['queued']['state'] == 'failed', 'failed pending queue')
                session = self.service.session(sid)
                self.assertEqual(session['state'], 'failed')
                self.assertEqual(session['receipts'][key]['state'], 'failed')
                self.assertEqual(session['queue'], [])
                self.assertIsNone(session['native_id'])
                records = self.service.records(sid)
                self.assertFalse(any(r['kind'] == 'harness' or r['data'].get('state') == 'starting' for r in records))
                self.assertTrue(any(r['kind'] == 'state' and r['data'].get('reason') ==
                                    'The selected harness is no longer configured' for r in records))
                connection, response = self.service.stream(sid, records[-2]['sequence'])
                try:
                    self.assertEqual(next_event(response), records[-1])
                finally:
                    response.close(); connection.close()
                failed_history[sid] = records
        self.prompt('unaffected', 'hello')
        self.wait(lambda: self.status()['receipts']['unaffected']['state'] == 'completed', 'other session remains usable')
        self.assertEqual(self.service.request('GET', '/v1/health')['status'], 'ready')
        self.assertTrue(self.service.request('GET', '/v1/dashboard')['runtime']['ready'])
        self.service.stop()
        self.assertNotIn('PoisonError', (self.root / 'service.log').read_text())
        self.assertNotIn('panicked at', (self.root / 'service.log').read_text())

        # Restoring configuration must not revive failed starts. A new pending
        # start also proves saved model/provider selections still override defaults.
        home = self.root / 'pi-home'; home.mkdir()
        self.env.update(CLOUDROOM_PI_HOME=str(home),
                        CLOUDROOM_PI_BINARY=str(Path(__file__).with_name('pi_fixture.py').resolve()),
                        CLOUDROOM_PI_MODEL='changed-model', CLOUDROOM_PI_PROVIDER='changed-provider')
        sequence = len(list(self.state.glob('*.record'))) + 1
        (self.state / f'{sequence:020}.record').write_text(json.dumps({
            'sequence': sequence, 'session_id': 'cr_configured', 'kind': 'receipt', 'data': {
                'request_id': 'configured', 'command': 'start', 'input': {'harness': 'pi'},
                'state': 'accepted', 'model': 'fixture', 'provider': 'fixture'}}))
        self.start(); self.ready()
        self.wait(lambda: self.service.session('cr_configured')['state'] == 'idle', 'configured pending start')
        # Identity records are partial updates; a later path-only record retains model/provider.
        identity = {}
        for record in self.service.records('cr_configured'):
            if record['kind'] == 'native_identity': identity.update(record['data'])
        self.assertEqual((identity['model'], identity['provider']), ('fixture', 'fixture'))
        configured = self.service.session('cr_configured')
        summary = next(s for s in self.service.request('GET', '/v1/dashboard')['sessions'] if s['id'] == 'cr_configured')
        self.assertEqual((summary['model'], configured['provider']), ('fixture', 'fixture'))
        for key in cases:
            with self.subTest(restored=key):
                sid = 'cr_' + key
                self.assertEqual(self.service.session(sid)['state'], 'failed')
                retry = self.service.request('POST', '/v1/sessions', {'request_id': key, 'harness': 'pi'}, 202)
                self.assertEqual(retry['receipt']['state'], 'failed')
                self.assertEqual(self.service.records(sid), failed_history[sid])
        self.prompt('after-restart', 'hello')
        self.wait(lambda: self.status()['receipts']['after-restart']['state'] == 'completed', 'peer after restart')
        self.assertEqual((self.repo / (self.native + '.requests')).read_text().splitlines(), ['unaffected', 'after-restart'])
        self.service.stop()
        self.assertNotIn('PoisonError', (self.root / 'service.log').read_text())
        self.assertNotIn('panicked at', (self.root / 'service.log').read_text())
        self.assertEqual(before, {name: (self.state / name).read_bytes() for name in before})

    def test_journal_failure_is_unhealthy_until_explicit_recovery(self):
        from workspaces import WorkspaceTests
        WorkspaceTests.database(self)
        self.seed(); self.start(); self.ready()
        self.wait(lambda:self.service.request('GET','/v1/health')['saving']['pending_records']==0,'saved baseline')
        self.assertTrue(self.service.request('GET','/v1/ready')['ready'])
        (self.state/'pending.tmp').mkdir()
        self.service.request('POST','/v1/sessions/cr_seed/prompts',{'request_id':'fault','text':'hello'},503)
        (self.state/'pending.tmp').rmdir()
        self.service.request('POST','/v1/sessions/cr_seed/prompts',{'request_id':'again','text':'hello'},503)
        self.assertFalse(self.service.request('GET','/v1/ready',expected=503)['ready'])
        self.assertFalse(self.service.request('GET','/v1/dashboard')['runtime']['ready'])
        self.service.request('POST','/v1/sessions/cr_seed/attachments?request_id=blocked&name=note.txt&kind=file',{},503)
        self.assertFalse((self.repo/'.cloudroom/attachments/blocked/note.txt').exists())
        self.assertTrue(self.service.records('cr_seed'))
        self.service.stop(crash=True); self.start(); self.ready()
        self.prompt('recovered','hello')
        self.wait(lambda:self.status()['receipts']['recovered']['state']=='completed','recording recovered')

    def test_partial_acceptance_and_legacy_enqueue_run_once(self):
        receipt = {"request_id": "pending", "command": "prompt", "input": {"text": "hello"}, "state": "accepted"}
        self.seed([("receipt", receipt)])  # Crash after receipt fsync, before old enqueue fsync.
        self.start()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "recovered partial acceptance")
        self.assertEqual(self.prompt("pending", "hello")["receipt"]["state"], "completed")
        self.service.request("POST", "/v1/sessions/cr_seed/prompts", {"request_id": "pending", "text": "different"}, 409)
        self.service.stop()
        # Historical logs with a separate enqueue record must not double-queue.
        for p in self.state.glob("*.record"):
            p.unlink()
        (self.state / "saved").unlink(missing_ok=True)
        path = Path(self.env["CLOUDROOM_CODEX_HOME"]) / "sessions"
        shutil.rmtree(path)
        self.seed([("receipt", receipt), ("enqueue", {"request_id": "pending"})])
        self.start()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "legacy queue")
        self.ready()
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["pending", "pending"])

    def test_graceful_restart_preserves_queue_and_close_settles_it(self):
        self.seed(); self.start(); self.ready()
        self.prompt("active", "no-reply")
        self.wait(lambda: (self.repo / "no-reply").exists(), "active native command")
        self.prompt("pending", "hello")
        self.service.stop(); self.start()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "queue after graceful restart")
        self.ready()
        self.assertIn(self.status()["receipts"]["active"]["state"], ["unknown", "unknown_after_restart"])
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["active", "pending"])
        self.prompt("active2", "no-reply"); self.prompt("cancelled", "hello")
        self.service.request("POST", "/v1/sessions/cr_seed/close", {"request_id": "close"}, 202)
        self.wait(lambda: self.status()["state"] == "closed", "deliberate close")
        self.assertEqual(self.status()["receipts"]["cancelled"]["state"], "failed")
        self.service.stop(); self.start()
        self.assertEqual(self.status()["state"], "closed")
        self.assertEqual(self.status()["queue"], [])

    def test_manual_stop_keeps_queue_paused_across_restart_until_resume(self):
        self.seed(); self.start(); self.ready()
        self.prompt("running", "hold")
        self.wait(lambda: self.status()["state"] == "running", "running turn")
        self.prompt("queued", "hello")
        self.service.request("POST", "/v1/sessions/cr_seed/stop", {"request_id": "stop"}, 202)
        self.wait(lambda: self.status()["receipts"]["stop"]["state"] == "completed", "stop acknowledgement")
        self.ready()
        self.assertTrue(self.status()["queue_paused"])
        self.assertEqual(self.status()["queue"], ["queued"])
        self.service.stop(); self.start(); self.ready()
        self.assertTrue(self.status()["queue_paused"])
        self.assertEqual(self.status()["queue"], ["queued"])
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["running"])
        self.service.request("POST", "/v1/sessions/cr_seed/resume", {"request_id": "resume"}, 202)
        self.wait(lambda: self.status()["receipts"]["queued"]["state"] == "completed", "resumed queue")
        self.assertFalse(self.status()["queue_paused"])
        self.service.request("POST", "/v1/sessions/cr_seed/resume", {"request_id": "resume"}, 202)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["running", "queued"])

    def test_child_notice_steers_a_busy_parent_and_queues_for_an_idle_one(self):
        self.seed(); self.start(); self.ready()
        self.prompt("running", "hold")
        self.wait(lambda: self.status()["state"] == "running", "running turn")
        self.service.request("POST", "/v1/sessions", {"request_id": "kid", "parent_session": "cr_seed", "prompt": "hello"}, 202)
        receipts = lambda: self.status()["receipts"]
        self.wait(lambda: receipts().get("notice_cr_child_kid_task_kid_steer", {}).get("state") == "completed", "steered notice")
        self.assertIn("completed", receipts()["notice_cr_child_kid_task_kid_steer"]["input"]["text"])
        self.assertNotIn("notice_cr_child_kid_task_kid", receipts())
        self.service.request("POST", "/v1/sessions/cr_seed/interrupt", {"request_id": "halt", "target_request_id": "running"}, 202)
        self.ready()
        self.service.request("POST", "/v1/sessions/cr_child_kid/prompts", {"request_id": "again", "text": "hello"}, 202)
        self.wait(lambda: receipts().get("notice_cr_child_kid_again", {}).get("state") == "completed", "queued notice")
        self.assertNotIn("notice_cr_child_kid_again_steer", receipts())

    def test_parent_hears_nothing_of_a_turn_cut_by_restart_or_a_detached_child(self):
        self.seed(); self.start(); self.ready()
        self.service.request("POST", "/v1/sessions", {"request_id": "kid", "parent_session": "cr_seed", "prompt": "hold"}, 202)
        child = lambda: self.service.session("cr_child_kid")
        receipts = lambda: self.status()["receipts"]
        self.wait(lambda: child()["state"] == "running", "running child")
        self.service.stop(); self.start(); self.ready()
        self.wait(lambda: child()["state"] == "idle", "resumed child")
        self.assertIn(child()["receipts"]["task_kid"]["state"], ["unknown", "unknown_after_restart"])
        self.service.request("POST", "/v1/sessions/cr_child_kid/prompts", {"request_id": "first", "text": "hello"}, 202)
        self.wait(lambda: "notice_cr_child_kid_first" in receipts(), "notice for a turn after the restart")
        time.sleep(.5)
        self.assertNotIn("notice_cr_child_kid_task_kid", receipts())
        self.wait(lambda: self.status()["state"] == "idle", "parent idle again")
        self.service.request("POST", "/v1/sessions/cr_child_kid/detach", {}, 202)
        self.service.request("POST", "/v1/sessions/cr_child_kid/prompts", {"request_id": "again", "text": "hello"}, 202)
        self.wait(lambda: child()["receipts"]["again"]["state"] == "completed", "detached child turn")
        time.sleep(.5)
        self.assertNotIn("notice_cr_child_kid_again", receipts())

    def test_codex_goal_turns_are_tracked_paused_by_stop_and_resumed_by_the_user(self):
        self.seed(); self.start(); self.ready()
        self.prompt("first", "goal")
        self.wait(lambda: (self.repo / "goal-turn").exists(), "goal continuation")
        auto = "auto_" + (self.repo / "goal-turn").read_text()
        self.wait(lambda: self.status()["state"] == "running" and auto in self.status()["receipts"], "tracked goal turn")
        self.prompt("queued", "hello")
        self.assertEqual(self.status()["queue"], ["queued"])
        self.service.request("POST", "/v1/sessions/cr_seed/stop", {"request_id": "stop"}, 202)
        self.wait(lambda: self.status()["receipts"][auto]["state"] == "interrupted", "stopped goal turn")
        goals = lambda: [json.loads(line) for line in (self.repo / (self.native + ".goal")).read_text().splitlines()]
        self.assertEqual(goals(), [{"threadId": self.native, "status": "paused", "origin": "user"}])
        self.service.request("POST", "/v1/sessions/cr_seed/goal", {"request_id": "resume-goal", "status": "active"}, 202)
        self.wait(lambda: self.status()["receipts"]["resume-goal"]["state"] == "completed", "resumed goal")
        self.assertEqual(goals()[-1], {"threadId": self.native, "status": "active", "origin": "user"})

    def test_agent_crash_recovers_once_and_failed_resume_settles_queue(self):
        self.seed(); self.start(); self.ready()
        self.prompt("crash", "crash")
        self.wait(lambda: (self.repo / "crash-ready").exists(), "crash gate")
        self.prompt("pending", "hello")
        (self.repo / "release-crash").touch()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "agent crash recovery")
        self.ready()
        self.assertEqual(self.status()["receipts"]["crash"]["state"], "unknown_after_restart")
        (self.repo / "release-crash").unlink(); (self.repo / "crash-ready").unlink()
        self.prompt("crash2", "crash")
        self.wait(lambda: (self.repo / "crash-ready").exists(), "second crash gate")
        self.prompt("unrun", "hello")
        (self.repo / "reject-resume").touch(); (self.repo / "release-crash").touch()
        self.wait(lambda: self.status()["state"] == "process_lost", "failed resume")
        self.wait(lambda: self.status()["receipts"]["unrun"]["state"] == "failed", "failed pending receipt")
        launches = len([r for r in self.service.records("cr_seed") if r["kind"] == "harness" and r["data"]["pid"] is not None])
        time.sleep(.5)
        self.assertEqual(len([r for r in self.service.records("cr_seed") if r["kind"] == "harness" and r["data"]["pid"] is not None]), launches)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["crash", "pending", "crash2"])
        self.wait(lambda: self.status()['state']=='process_lost' and self.status().get('startup_error')=='resume_failed', 'classified failure')
        (self.repo / 'reject-resume').unlink()
        time.sleep(.5)
        self.service.request('POST','/v1/sessions/cr_seed/resume',{'request_id':'retry-resume'},202)
        self.wait(lambda:self.status()['receipts']['retry-resume']['state']=='completed','explicit recovery')
        self.ready()
        self.service.request('POST','/v1/sessions/cr_seed/resume',{'request_id':'retry-resume'},202)
        self.assertEqual(self.status()['native_id'],self.native)
        self.assertEqual((self.repo / (self.native + '.requests')).read_text().splitlines(),['crash','pending','crash2'])
        self.prompt('after-retry','hello')
        self.wait(lambda:self.status()['receipts']['after-retry']['state']=='completed','work after retry')

    def test_recovery_preflight_distinguishes_empty_and_missing_history(self):
        self.seed(); self.start(); self.ready()
        path = Path(self.status()['native_path'])
        before = self.status()['last_sequence']
        self.service.request('GET','/v1/sessions/cr_seed/recovery',expected=401,token=None)
        self.assertEqual(self.service.request('GET','/v1/sessions/cr_seed/recovery')['status'],'ready')
        self.assertEqual(self.status()['last_sequence'],before)
        saved = path.read_bytes(); path.unlink()
        self.assertEqual(self.service.request('GET','/v1/sessions/cr_seed/recovery')['status'],'empty')
        path.write_bytes(saved)
        self.prompt('real-work','hello')
        self.wait(lambda:self.status()['receipts']['real-work']['state']=='completed','real context')
        self.service.stop(); path.unlink(); self.start()
        self.wait(lambda:self.status()['state']=='process_lost','missing native history')
        self.assertEqual(self.status()['startup_error'],'missing_history')
        self.assertEqual(self.service.request('GET','/v1/sessions/cr_seed/recovery')['status'],'missing')
        self.service.request('POST','/v1/sessions/cr_seed/resume',{'request_id':'no-replacement'},409)
        self.assertEqual(self.status()['native_id'],self.native)
        self.assertFalse(path.exists())
        self.assertTrue(self.service.records('cr_seed'))

    def test_repeated_crash_does_not_loop(self):
        self.seed(); self.start(); self.ready()
        self.prompt("first", "crash")
        self.wait(lambda: (self.repo / "crash-ready").exists(), "first crash")
        self.prompt("second", "crash"); self.prompt("unrun", "hello")
        (self.repo / "release-crash").touch()
        self.wait(lambda: self.status()["receipts"]["unrun"]["state"] == "failed", "bounded recovery")
        self.assertEqual(self.status()["state"], "process_lost")
        time.sleep(.3)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["first", "second"])
        self.assertEqual(len([r for r in self.service.records("cr_seed") if r["kind"] == "harness" and r["data"]["pid"] is not None]), 2)

    def test_close_during_resume_never_dispatches_queued_input(self):
        self.seed(); (self.repo / "hold-resume").touch(); self.start()
        self.wait(lambda: (self.repo / "resume-ready").exists(), "native resume gate")
        try:
            self.prompt("unrun", "hello")
            self.assertEqual(self.status()["queue"], ["unrun"])
            self.service.request("POST", "/v1/sessions/cr_seed/close", {"request_id": "close"}, 202)
        finally:
            (self.repo / "release-resume").touch()
        self.wait(lambda: self.status()["state"] == "closed", "close while resuming")
        self.assertEqual(self.status()["receipts"]["unrun"]["state"], "failed")
        self.assertFalse((self.repo / (self.native + ".requests")).exists())
        self.service.stop(); self.start()
        self.assertEqual(self.status()["state"], "closed")

    def test_queue_edit_retries_do_not_restore_old_input(self):
        self.seed(); self.start(); self.ready()
        self.service.request("POST", "/v1/sessions/cr_seed/stop", {"request_id":"pause"}, 202)
        self.prompt("pending", "old")
        edit = {"request_id":"edit", "target_request_id":"pending", "expected_revision":1, "text":"new", "reasoning":"low"}
        self.service.request("POST", "/v1/sessions/cr_seed/edit", edit, 202)
        self.service.request("POST", "/v1/sessions/cr_seed/edit", edit, 202)
        self.service.request("POST", "/v1/sessions/cr_seed/edit", {**edit,"request_id":"stale","text":"older"}, 409)
        self.prompt("pending", "old")
        self.assertEqual(self.status()["receipts"]["pending"]["input"]["text"], "old")
        self.assertEqual(self.status()["prompts"]["pending"]["input"]["text"], "new")
        self.service.stop(); self.start(); self.ready()
        self.service.request("POST", "/v1/sessions/cr_seed/resume", {"request_id":"resume"}, 202)
        self.wait(lambda:self.status()["receipts"]["pending"]["state"] == "completed", "edited prompt")
        turns = [json.loads(line) for line in Path(self.status()["native_path"]).read_text().splitlines() if json.loads(line).get("fixture") == "turn"]
        self.assertEqual([(turn["text"],turn["reasoning"]) for turn in turns], [("new","low")])
        self.service.request("POST", "/v1/sessions/cr_seed/edit", {**edit,"request_id":"too-late","expected_revision":2}, 409)

    def test_rewind_blocks_dispatch_and_replays_replacement_once(self):
        self.seed(); self.start(); self.ready()
        self.prompt("original", "hello")
        self.wait(lambda: self.status()["receipts"]["original"]["state"] == "completed", "original")
        (self.repo / "hold-fork").touch()
        body = {"request_id": "rewind", "before": "original-turn", "replacement": {"request_id": "corrected", "text": "corrected"}}
        self.service.request("POST", "/v1/sessions/cr_seed/rewind", body, 202)
        self.wait(lambda: (self.repo / "fork-ready").exists(), "fork waiting")
        self.service.request("POST", "/v1/sessions/cr_seed/prompts", {"request_id": "racing", "text": "must not run"}, 409)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["original"])
        (self.repo / "release-fork").touch()
        self.wait(lambda: self.status()["receipts"].get("corrected", {}).get("state") == "completed", "replacement")
        forked = self.status()["native_id"]
        self.assertNotEqual(forked, self.native)
        self.service.request("POST", "/v1/sessions/cr_seed/rewind", body, 202)
        self.assertEqual((self.repo / (forked + ".requests")).read_text().splitlines(), ["corrected"])
        self.service.stop(); self.start()
        self.wait(lambda: self.status()["state"] == "idle", "resumed fork")
        self.assertEqual(self.status()["native_id"], forked)
        self.assertEqual((self.repo / (forked + ".requests")).read_text().splitlines(), ["corrected"])

    def test_reused_pid_never_kills_unrelated_process(self):
        sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", self.env["CLOUDROOM_CODEX_BINARY"]],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self.seed([("harness", {"pid": sleeper.pid})])
            self.start()
            self.wait(lambda: self.status()["state"] in ["idle", "process_lost"], "reconciliation")
            self.assertIsNone(sleeper.poll(), "unrelated process was killed by a command-text match")
            self.assertEqual(self.status()["state"], "process_lost", "unknown ownership must not launch a replacement")
        finally:
            if sleeper.poll() is None:
                sleeper.kill()
            sleeper.wait()


def progress_checks(service, root):
    sid = service.request("POST", "/v1/sessions", {"request_id": "ordering"}, 202)["session_id"]
    until(lambda: service.session(sid)["state"] == "idle", "ordering session")
    for text in ["reply-first", "finish-first", "events-first", "reject"]:
        expected = "failed" if text == "reject" else "completed"
        service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": text, "text": text}, 202)
        until(lambda: service.session(sid)["state"] == "idle" and service.session(sid)["receipts"][text]["state"] == expected, text)
        time.sleep(.1)  # Let a trailing RPC acknowledgement arrive after completion.
        assert service.session(sid)["receipts"][text]["state"] == expected
        dashboard = service.request("GET", "/v1/dashboard")
        summary = next(s for s in dashboard["sessions"] if s["id"] == sid)
        assert summary["state"] == ("failed" if expected == "failed" else "waiting")
        assert isinstance(summary["lastActivity"], int)
        assert summary["model"] == service.env["CLOUDROOM_MODEL"]
        running = [r for r in service.records(sid) if r["kind"] == "state" and r["data"].get("state") == "running" and r["data"].get("request_id") == text]
        assert len(running) == (0 if text == "reject" else 1), "turn progress was recorded more than once"
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "late", "text": "late-reject"}, 202)
    until(lambda: (root / "repo/reject-ready").exists(), "pending rejection")
    try:
        service.request("POST", f"/v1/sessions/{sid}/close", {"request_id": "close"}, 202)
    finally:
        (root / "repo/release-reject").touch()
    try:
        until(lambda: service.session(sid)["receipts"]["late"]["state"] == "failed", "late rejection delivered", 3)
        assert service.session(sid)["state"] == "closing"
    finally:
        (root / "repo/finish-reject").touch()
    until(lambda: service.session(sid)["state"] == "closed", "close despite late rejection")
    states = [r["data"]["state"] for r in service.records(sid) if r["kind"] == "state"]
    assert "idle" not in states[states.index("closing"):], "late reply reopened a closing session"


def session_checks(service, root):
    """Additional API checks run by core_e2e.py --fixture with disposable PostgreSQL."""
    progress_checks(service, root)
    ids = []
    for key in ["fixture-a", "fixture-b", "fixture-third"]:
        sid = service.request("POST", "/v1/sessions", {"request_id": key}, 202)["session_id"]
        until(lambda: service.session(sid)["state"] == "idle", "fixture start")
        ids.append(sid)
    a, b, third = ids
    native_a = service.session(a)["native_id"]
    repo = root / "repo"
    until(lambda: service.session(a)["native_offset"] > 0, "initial native tail")
    connection, response = service.stream(a, service.session(a)["last_sequence"])
    try:
        service.request("POST", f"/v1/sessions/{a}/prompts", {"request_id": "delay", "text": "delay"}, 202)
        # The harness emits nothing until released: only the durable acceptance can wake SSE.
        event = next_event(response)
        assert event["kind"] == "receipt" and event["data"]["request_id"] == "delay"
        assert event["data"]["state"] == "accepted"
    finally:
        (repo / "release").write_text("release")
        response.close(); connection.close()
    until(lambda: service.session(a)["state"] == "idle", "delayed fixture completion")
    for sid in ids:
        service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "hold", "text": "hold"}, 202)
    ticks = [repo / (service.session(sid)["native_id"] + ".ticks") for sid in ids]
    until(lambda: all(service.session(sid)["state"] == "running" for sid in ids)
          and all(path.exists() for path in ticks), "three concurrent turns")
    before = [path.read_text() for path in ticks]
    until(lambda: all(path.read_text() != value for path, value in zip(ticks, before)), "all three tools progress")
    service.request("POST", f"/v1/sessions/{b}/stop", {"request_id": "stop-b"}, 202)
    until(lambda: service.session(b)["state"] == "idle", "stopped peer remains open")
    service.request("POST", f"/v1/sessions/{third}/close", {"request_id": "close-third"}, 202)
    until(lambda: service.session(third)["state"] == "closed", "third session closed")
    tick_a = repo / (native_a + ".ticks")
    until(tick_a.exists, "active peer tool")
    close = {"request_id": "close-b"}
    service.request("POST", f"/v1/sessions/{b}/close", close, 202)
    until(lambda: service.session(b)["state"] == "closed" and service.session(b)["receipts"]["close-b"]["state"] == "completed", "idle session close")
    service.request("POST", f"/v1/sessions/{b}/close", close, 202)
    service.request("POST", f"/v1/sessions/{b}/prompts", {"request_id": "after-close", "text": "no"}, 409)
    c = service.request("POST", "/v1/sessions", {"request_id": "freed"}, 202)["session_id"]
    until(lambda: service.session(c)["state"] == "idle", "new session alongside existing peer")
    native_c = service.session(c)["native_id"]
    service.request("POST", f"/v1/sessions/{c}/prompts", {"request_id": "hold", "text": "hold"}, 202)
    tick_c = repo / (native_c + ".ticks")
    until(tick_c.exists, "close target tool")
    pid_c = int((repo / (native_c + ".pid")).read_text())
    service.request("POST", f"/v1/sessions/{c}/close", {"request_id": "close-active"}, 202)
    until(lambda: service.session(c)["state"] == "closed" and service.session(c)["receipts"]["close-active"]["state"] == "completed", "active close")
    before_a, before_c = tick_a.read_text(), tick_c.read_text()
    time.sleep(.4)
    assert tick_c.read_text() == before_c and tick_a.read_text() != before_a
    assert not Path(f"/proc/{pid_c}").exists(), "close left the target tool alive"
    assert service.session(a)["native_id"] == native_a
    service.stop()
    before_a = tick_a.read_text()
    time.sleep(.3)
    assert tick_a.read_text() == before_a, "tool kept writing after core shutdown"
    service.start()
    assert service.session(c)["state"] == "closed"
    assert service.request("POST", f"/v1/sessions/{c}/close", {"request_id": "close-active"}, 202)["receipt"]["state"] == "completed"
    until(lambda: service.session(a)["state"] == "idle", "peer resumes after graceful shutdown")
    assert service.session(a)["native_id"] == native_a
    service.request("POST", f"/v1/sessions/{a}/close", {"request_id": "close-resumed"}, 202)
    until(lambda: service.session(a)["state"] == "closed", "resumed peer closed")
    queue_and_recovery_checks(service, root)


def queue_and_recovery_checks(service, root):
    """Ordered queued delivery, safe retries, and crash-resume of a running session."""
    repo = root / "repo"
    release = repo / "release"
    if release.exists():
        release.unlink()
    sid = service.request("POST", "/v1/sessions", {"request_id": "queueing"}, 202)["session_id"]
    until(lambda: service.session(sid)["state"] == "idle", "queue session start")
    # Hold the first turn open, then queue two more prompts while busy.
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "q1", "text": "delay"}, 202)
    until(lambda: service.session(sid)["state"] == "starting_turn", "first turn in flight")
    for key in ["q2", "q3"]:
        r = service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": key, "text": key}, 202)
        assert r["receipt"]["state"] == "accepted", r
    assert service.session(sid)["queue"] == ["q2", "q3"], service.session(sid)["queue"]
    # A sandbox host may stop the machine only after Core drains; a turn in flight refuses it.
    assert service.request("POST", "/v1/drain", {"hold": True}, 200)["drained"] is False
    # Retrying a queued id is idempotent; conflicting content is rejected.
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "q2", "text": "q2"}, 202)
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "q2", "text": "different"}, 409)
    release.write_text("go")
    for key in ["q1", "q2", "q3"]:
        until(lambda k=key: service.session(sid)["receipts"][k]["state"] == "completed", f"{key} completed")
    until(lambda: service.session(sid)["state"] == "idle", "queue drained")
    assert service.request("POST", "/v1/drain", {"hold": True}, 200)["drained"] is True
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "while-draining", "text": "x"}, 409)
    service.request("POST", "/v1/drain", {"hold": False}, 200)
    order = [r["data"]["request_id"] for r in service.records(sid)
             if r["kind"] == "state" and r["data"].get("state") == "starting_turn"]
    assert order == ["q1", "q2", "q3"], order

    # A deliberately closed session must never be revived by recovery.
    closed = service.request("POST", "/v1/sessions", {"request_id": "closed-keep"}, 202)["session_id"]
    until(lambda: service.session(closed)["state"] == "idle", "closable session")
    service.request("POST", f"/v1/sessions/{closed}/close", {"request_id": "shut"}, 202)
    until(lambda: service.session(closed)["state"] == "closed", "closed before crash")

    # Crash with an in-flight turn and a never-dispatched queued prompt.
    native = service.session(sid)["native_id"]
    selected_model = service.session(sid)["receipts"]["queueing"]["model"]
    assert selected_model and selected_model != "resumed-fixture"
    release.unlink()
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "held", "text": "delay"}, 202)
    until(lambda: service.session(sid)["state"] == "starting_turn", "held turn in flight")
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "pending", "text": "pending"}, 202)
    assert service.session(sid)["queue"] == ["pending"]
    before = service.records(sid)
    service.stop(crash=True)
    release.write_text("go")  # reconcile must have killed the orphan before this frees it
    service.env["CLOUDROOM_MODEL"] = "resumed-fixture"
    service.start()
    # The Cloudroom session and native identity survive; history is append-only.
    until(lambda: service.session(sid)["state"] == "idle", "resumed after crash", 30)
    assert service.session(sid)["native_id"] == native
    summary = next(s for s in service.request("GET", "/v1/dashboard")["sessions"] if s["id"] == sid)
    # Saved selections survive deployment-default changes; the dashboard must agree.
    assert summary["model"] == selected_model
    assert service.records(sid)[:len(before)] == before, "history changed across restart"
    # The uncertain in-flight turn is not resent; the queued prompt is delivered.
    assert service.session(sid)["receipts"]["held"]["state"] == "unknown_after_restart"
    until(lambda: service.session(sid)["receipts"]["pending"]["state"] == "completed", "queued work survives restart")
    assert service.session(closed)["state"] == "closed", "recovery revived a closed session"


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "app-server":
        codex()
    else:
        unittest.main()
