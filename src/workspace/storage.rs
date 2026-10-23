//! Disk policy and explicitly disposable npm download-cache cleanup, not history retention.
use crate::{config::Config, observability::now_ms, runtime::linux};
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::process::Command;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub agent_uid: u32,
    pub agent_gid: u32,
    pub cache_dir: PathBuf,
    /// Null in a container without cgroups or capabilities (see `container`).
    pub cgroup_root: Option<PathBuf>,
    /// Sandbox deployments give agents sudo: the sandbox itself is the security boundary.
    #[serde(default)]
    pub agent_sudo: bool,
    #[serde(default = "warning")]
    pub warning_bytes: u64,
    #[serde(default = "pause")]
    pub pause_bytes: u64,
    #[serde(default = "resume")]
    pub resume_bytes: u64,
}
fn warning() -> u64 {
    5_000_000_000
}
fn pause() -> u64 {
    2_000_000_000
}
fn resume() -> u64 {
    2_500_000_000
}

impl Policy {
    pub fn load(path: &Path) -> io::Result<Self> {
        let policy: Self = serde_json::from_slice(&fs::read(path)?)?;
        if policy.agent_uid == 0
            || policy.agent_gid == 0
            || !(0 < policy.pause_bytes
                && policy.pause_bytes < policy.resume_bytes
                && policy.resume_bytes < policy.warning_bytes)
        {
            return Err(io::Error::other("invalid disk protection policy"));
        }
        if policy.container() && !policy.agent_sudo {
            return Err(io::Error::other(
                "cgroups are required unless the sandbox is the security boundary (agent_sudo)",
            ));
        }
        // Neither the policy nor its ancestors may be replaced by agent writes.
        for part in path.ancestors() {
            let m = fs::symlink_metadata(part)?;
            if m.file_type().is_symlink() || m.uid() == policy.agent_uid || m.mode() & 0o022 != 0 {
                return Err(io::Error::other(
                    "disk policy must be protected from agent writes",
                ));
            }
        }
        Ok(policy)
    }

    /// A container without cgroups or capabilities, whose agents have sudo: Core runs as the agent account and
    /// contains work by marking its processes (runtime/linux.rs). Its /tmp and /var/tmp may be memory disks
    /// (gVisor mounts its own tmpfs on /tmp).
    pub fn container(&self) -> bool {
        self.cgroup_root.is_none()
    }

    fn check_filesystems(&self, config: &Config) -> io::Result<()> {
        let mount = fs::metadata(&config.repository)?;
        let temporary = (!self.container())
            .then_some([Path::new("/tmp"), Path::new("/var/tmp")])
            .into_iter()
            .flatten();
        for path in [
            &config.repository,
            &config.state_dir,
            &config.account_home,
            &self.cache_dir,
            Path::new("/code"),
        ]
        .into_iter()
        .chain(temporary)
        .chain(
            config
                .harnesses
                .values()
                .map(|profile| profile.home.as_path()),
        ) {
            if fs::metadata(path)?.dev() != mount.dev() {
                return Err(io::Error::other(
                    "agent paths must share the monitored filesystem",
                ));
            }
        }
        Ok(())
    }

    pub fn prepare(&self, config: &Config) -> io::Result<()> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::other("disk protection requires Linux"));
        }
        self.check_filesystems(config)?;
        if !self.container() && fs::metadata(&config.state_dir)?.uid() == self.agent_uid {
            return Err(io::Error::other(
                "history must belong to the protected service account",
            ));
        }
        let parent = self
            .cache_dir
            .parent()
            .ok_or_else(|| io::Error::other("invalid cache root"))?;
        // In a container Core already runs as the agent, so agent-owned ancestors protect nothing, as with
        // history above. There the cache may live inside the agent's /code volume, which some sandboxes (gVisor)
        // require: each mount gets its own device, and agent paths must share one.
        for part in parent.ancestors().filter(|_| !self.container()) {
            let m = fs::symlink_metadata(part)?;
            if m.file_type().is_symlink() || m.uid() == self.agent_uid || m.mode() & 0o022 != 0 {
                return Err(io::Error::other("cache root requires protected ancestors"));
            }
        }
        let Some(root) = &self.cgroup_root else {
            return Ok(());
        };
        let current = fs::read_to_string("/proc/self/cgroup")?;
        let current = current
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .ok_or_else(|| io::Error::other("cgroup v2 is required"))?;
        let owner = Path::new("/sys/fs/cgroup").join(current.trim_start_matches('/'));
        if *root != owner.join("agents") {
            return Err(io::Error::other(
                "agent cgroups must be inside the core's delegated service",
            ));
        }
        if !root.exists() {
            fs::create_dir(root)?;
        }
        if !root.join("cgroup.freeze").is_file() || !root.join("cgroup.kill").is_file() {
            return Err(io::Error::other(
                "workload freeze and kill controls are required",
            ));
        }
        Ok(())
    }

    /// `floor` replaces the pause threshold after a rescue (`Guard::rescue`).
    fn level(&self, available: u64, previous: Level, floor: Option<u64>) -> Level {
        let blocked = match floor {
            Some(floor) => available < floor,
            None => {
                available < self.pause_bytes
                    || (previous == Level::Blocked && available <= self.resume_bytes)
            }
        };
        // Low space ends a fifth above the warning line, so space hovering at the line
        // (a build writing and deleting files) does not repeat the warning.
        if blocked {
            Level::Blocked
        } else if available <= self.warning_bytes
            || (previous != Level::Normal && available <= self.warning_bytes / 5 * 6)
        {
            Level::LowSpace
        } else {
            Level::Normal
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Normal,
    LowSpace,
    Blocked,
}
#[derive(Clone, Serialize)]
pub struct Snapshot {
    pub enabled: bool,
    pub level: Level,
    pub workspace_available_bytes: Option<u64>,
    pub history_available_bytes: Option<u64>,
    pub workspace_total_bytes: Option<u64>,
    pub history_total_bytes: Option<u64>,
    pub sampled_at: Option<u64>,
    pub reason: &'static str,
}
impl Snapshot {
    fn unavailable(enabled: bool) -> Self {
        Self {
            enabled,
            level: if enabled {
                Level::Blocked
            } else {
                Level::Normal
            },
            workspace_available_bytes: None,
            history_available_bytes: None,
            workspace_total_bytes: None,
            history_total_bytes: None,
            sampled_at: None,
            reason: if enabled {
                "measurement_unavailable"
            } else {
                "unprotected_test_mode"
            },
        }
    }
}

/// How long a guard-loop measurement stays valid.
pub(crate) const FRESH: Duration = Duration::from_secs(5);

pub struct Guard {
    policy: Option<Policy>,
    history: PathBuf,
    value: Mutex<(Snapshot, Instant)>,
    writers: Mutex<Vec<Weak<linux::Workload>>>,
    floor: Mutex<Option<u64>>,
}
impl Guard {
    pub fn new(config: &Config) -> io::Result<Self> {
        if let Some(p) = &config.storage {
            p.prepare(config)?;
        }
        Ok(Self {
            policy: config.storage.clone(),
            history: config.state_dir.clone(),
            writers: Mutex::new(Vec::new()),
            floor: Mutex::new(None),
            value: Mutex::new((
                Snapshot::unavailable(config.storage.is_some()),
                Instant::now(),
            )),
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        let (value, sampled) = &*self.value.lock().unwrap();
        if !value.enabled || sampled.elapsed() <= FRESH {
            return value.clone();
        }
        // A busy core can delay the guard loop. Re-measure instead of blocking on a stale reading.
        // History shares the workspace filesystem (`check_filesystems`); failed checks stay blocked.
        let measured = (self.policy.as_ref())
            .filter(|_| value.reason == "disk_capacity")
            .and_then(|p| Some((p, disk(&self.history).ok()?)));
        let Some((p, d)) = measured else {
            return Snapshot::unavailable(true);
        };
        Snapshot {
            level: p.level(d.available_bytes, value.level, *self.floor.lock().unwrap()),
            workspace_available_bytes: Some(d.available_bytes),
            history_available_bytes: Some(d.available_bytes),
            sampled_at: Some(now_ms()),
            ..value.clone()
        }
    }
    pub fn blocks(&self) -> bool {
        self.snapshot().level == Level::Blocked
    }
    /// Frozen work never frees space. Once Cloudroom has stopped the command filling the disk, work continues
    /// until half of the space left is used, then pauses again. Space above `resume_bytes` ends the rescue.
    /// The pause line never drops below an eighth of `pause_bytes` (250 MB on a 20 GB box), so agents can't fill
    /// the space Core needs to save history. Returns false at that line: work stays paused until space is freed.
    pub fn rescue(&self) -> bool {
        let (snapshot, _) = &*self.value.lock().unwrap();
        let (Some(workspace), Some(history)) = (
            snapshot.workspace_available_bytes,
            snapshot.history_available_bytes,
        ) else {
            return false;
        };
        let (available, minimum) = (
            workspace.min(history),
            self.policy.as_ref().map_or(0, |p| p.pause_bytes / 8),
        );
        if available <= minimum {
            return false;
        }
        *self.floor.lock().unwrap() = Some((available / 2).max(minimum));
        true
    }
    pub(crate) fn spawn_writer(
        &self,
        command: &mut Command,
    ) -> io::Result<(tokio::process::Child, Option<Arc<linux::Workload>>)> {
        self.spawn(command, true)
    }

    /// Emergency operator access: runs while storage is blocked and is never frozen,
    /// so a person can read work back and free space.
    pub(crate) fn spawn_operator(
        &self,
        command: &mut Command,
    ) -> io::Result<(tokio::process::Child, Option<Arc<linux::Workload>>)> {
        self.spawn(command, false)
    }

    fn spawn(
        &self,
        command: &mut Command,
        gated: bool,
    ) -> io::Result<(tokio::process::Child, Option<Arc<linux::Workload>>)> {
        // Serialize admission with pause snapshots so a just-started writer is never missed.
        let mut writers = self.writers.lock().unwrap();
        if gated && self.blocks() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "storage unsafe; file writes are blocked",
            ));
        }
        let workload = self
            .policy
            .as_ref()
            .map(|policy| {
                let group = Arc::new(linux::Workload::create(policy)?);
                group.attach(command, policy)?;
                Ok::<_, io::Error>(group)
            })
            .transpose()?;
        let child = command.spawn()?;
        writers.retain(|writer| writer.strong_count() > 0);
        if let Some(group) = workload.as_ref().filter(|_| gated) {
            writers.push(Arc::downgrade(group));
        }
        Ok((child, workload))
    }

    pub(crate) async fn pause_writers(&self, paused: bool) -> io::Result<()> {
        let writers: Vec<_> = self
            .writers
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let mut result = Ok(());
        for writer in writers {
            if let Err(error) = writer.freeze(paused).await {
                result = Err(error);
            }
        }
        result
    }

    pub async fn refresh(&self, config: &Config, workspaces: &super::Workspaces) -> Snapshot {
        let Some(p) = &self.policy else {
            return self.snapshot();
        };
        let previous = self.value.lock().unwrap().0.level;
        let measured = async {
            p.check_filesystems(config)?;
            workspaces.check_filesystems()?;
            let (workspace, history) = (disk(workspaces.root())?, disk(&config.state_dir)?);
            let available = workspace.available_bytes.min(history.available_bytes);
            let level = {
                let mut floor = self.floor.lock().unwrap();
                if available > p.resume_bytes {
                    *floor = None;
                }
                p.level(available, previous, *floor)
            };
            Ok::<_, io::Error>(Snapshot {
                enabled: true,
                level,
                workspace_available_bytes: Some(workspace.available_bytes),
                history_available_bytes: Some(history.available_bytes),
                workspace_total_bytes: Some(workspace.total_bytes),
                history_total_bytes: Some(history.total_bytes),
                sampled_at: Some(now_ms()),
                reason: "disk_capacity",
            })
        }
        .await
        .unwrap_or_else(|_| Snapshot::unavailable(true));
        *self.value.lock().unwrap() = (measured.clone(), Instant::now());
        measured
    }
    pub async fn clean(&self) -> io::Result<()> {
        let Some(p) = &self.policy else {
            return Ok(());
        };
        // Cleanup proves quiescence through frozen cgroups, so containers without them skip it.
        let Some(groups) = &p.cgroup_root else {
            return Ok(());
        };
        let mut command = Command::new(std::env::current_exe()?);
        command
            .env_clear()
            .arg("--clean-npm-cache")
            .arg(&p.cache_dir)
            .arg(groups)
            .kill_on_drop(true);
        linux::as_agent(&mut command, p.agent_uid, p.agent_gid, None, true)?;
        let result = tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .map_err(|_| io::Error::other("cache cleanup timed out"))??;
        if !result.status.success() {
            return Err(io::Error::other("cache cleanup refused"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct Disk {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
}
/// A system call, not `df`: when agents fill the process table, starting `df` fails,
/// and an unmeasurable disk pauses all work.
#[cfg(target_os = "linux")]
pub(crate) fn disk(path: &Path) -> io::Result<Disk> {
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn statvfs(path: *const std::ffi::c_char, buf: *mut u64) -> i32;
    }
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)?;
    // 64-bit Linux `struct statvfs` begins f_bsize, f_frsize, f_blocks, f_bfree, f_bavail, each
    // 8 bytes. The buffer is larger than the whole struct.
    let mut buf = [0u64; 32];
    if unsafe { statvfs(path.as_ptr(), buf.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let [_, unit, blocks, free, available, ..] = buf;
    let bytes = |n: u64| {
        n.checked_mul(unit)
            .ok_or_else(|| io::Error::other("invalid filesystem measurement"))
    };
    Ok(Disk {
        total_bytes: bytes(blocks)?,
        used_bytes: bytes(blocks.saturating_sub(free))?,
        available_bytes: bytes(available)?,
    })
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn disk(_: &Path) -> io::Result<Disk> {
    Err(io::Error::other("disk measurement requires Linux"))
}

/// Runs only as the agent account, while workloads are frozen or absent. Never traverses
/// arbitrary projects, npm's executable _npx cache, histories, or symlinked directories.
#[cfg(target_os = "linux")]
pub fn clean_npm_cache(root: &Path, groups: &Path) -> io::Result<usize> {
    use std::{
        collections::HashSet,
        os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    };
    let open = |path: &Path| {
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(0x20000 | 0x10000)
            .open(path)
    }; // O_NOFOLLOW | O_DIRECTORY
    let mut dir = open(root)?;
    for name in ["npm", "_cacache", "content-v2", "sha512"] {
        let next = PathBuf::from(format!("/proc/self/fd/{}/{}", dir.as_raw_fd(), name));
        match open(&next) {
            Ok(d) => dir = d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        }
    }
    let uid = fs::metadata(root)?.uid();
    let mut active = HashSet::new();
    for proc in fs::read_dir("/proc")? {
        let proc = proc?;
        if !proc
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|b| b.is_ascii_digit())
        {
            continue;
        }
        let Ok(meta) = proc.metadata() else {
            continue;
        };
        if meta.uid() != uid || proc.file_name().to_string_lossy() == std::process::id().to_string()
        {
            continue;
        }
        let membership = fs::read_to_string(proc.path().join("cgroup"))?;
        let relative = membership
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .ok_or_else(|| io::Error::other("unknown agent containment"))?;
        let group = Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
        if !group.starts_with(groups)
            || !fs::read_to_string(group.join("cgroup.events"))?
                .lines()
                .any(|l| l == "frozen 1")
        {
            return Err(io::Error::other(
                "cleanup requires all agent processes to be quiescent",
            ));
        }
        // Mapped files can remain active after their file descriptor is closed.
        // Refuse this cleanup pass if any process maps content from the cache.
        let maps = fs::read_to_string(proc.path().join("maps"))?;
        if maps.contains(root.to_string_lossy().as_ref()) {
            return Err(io::Error::other("cache has active memory mappings"));
        }
        match fs::read_dir(proc.path().join("fd")) {
            Ok(fds) => {
                for fd in fds.flatten() {
                    if let Ok(m) = fs::metadata(fd.path()) {
                        active.insert((m.dev(), m.ino()));
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e), // Unverifiable active users mean no cleanup.
        }
    }
    let hex = |s: &str, len: usize| {
        s.len() == len
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let mut removed = 0;
    for a in fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))? {
        let a = a?;
        if !hex(&a.file_name().to_string_lossy(), 2) {
            continue;
        }
        let Ok(a) = open(&a.path()) else {
            continue;
        };
        for b in fs::read_dir(format!("/proc/self/fd/{}", a.as_raw_fd()))? {
            let b = b?;
            if !hex(&b.file_name().to_string_lossy(), 2) {
                continue;
            }
            let Ok(b) = open(&b.path()) else {
                continue;
            };
            for leaf in fs::read_dir(format!("/proc/self/fd/{}", b.as_raw_fd()))? {
                let leaf = leaf?;
                if !hex(&leaf.file_name().to_string_lossy(), 124) {
                    continue;
                }
                let m = fs::symlink_metadata(leaf.path())?;
                if m.is_file()
                    && m.uid() == uid
                    && m.nlink() == 1
                    && !active.contains(&(m.dev(), m.ino()))
                {
                    // A cache-shaped filename alone never makes customer data disposable.
                    let file = fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(0x20000)
                        .open(leaf.path())?;
                    let opened = file.metadata()?;
                    if (opened.dev(), opened.ino()) != (m.dev(), m.ino()) {
                        continue;
                    }
                    let digest = std::process::Command::new("/usr/bin/sha512sum")
                        .env_clear()
                        .stdin(file)
                        .output()?;
                    let digest = String::from_utf8_lossy(&digest.stdout);
                    let expected = format!(
                        "{}{}{}",
                        a_name(&a)?,
                        a_name(&b)?,
                        leaf.file_name().to_string_lossy()
                    );
                    if !digest
                        .split_whitespace()
                        .next()
                        .is_some_and(|d| d == expected)
                    {
                        continue;
                    }
                    fs::remove_file(leaf.path())?;
                    removed += 1;
                    if removed >= 512 {
                        return Ok(removed);
                    }
                }
            }
        }
    }
    Ok(removed)
}
#[cfg(target_os = "linux")]
fn a_name(file: &fs::File) -> io::Result<String> {
    use std::os::fd::AsRawFd;
    let path = fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
    path.file_name()
        .and_then(|s| s.to_str())
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("invalid cache directory"))
}
#[cfg(not(target_os = "linux"))]
pub fn clean_npm_cache(_: &Path, _: &Path) -> io::Result<usize> {
    Err(io::Error::other("cache cleanup requires Linux"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actual_free_space_warns_without_blocking_and_resumes_below_warning() {
        let policy: Policy = serde_json::from_str(
            r#"{"agent_uid":1001,"agent_gid":1001,"cache_dir":"/cache","cgroup_root":"/agents"}"#,
        )
        .unwrap();
        for (available, before, expected) in [
            (20_000_000_000, Level::Normal, Level::Normal),
            (5_000_000_001, Level::Normal, Level::Normal),
            (5_000_000_001, Level::LowSpace, Level::LowSpace),
            (6_000_000_000, Level::LowSpace, Level::LowSpace),
            (6_000_000_001, Level::LowSpace, Level::Normal),
            (5_000_000_000, Level::Normal, Level::LowSpace),
            (4_999_999_999, Level::Normal, Level::LowSpace),
            (3_000_000_000, Level::LowSpace, Level::LowSpace),
            (2_500_000_001, Level::Normal, Level::LowSpace),
            (2_500_000_001, Level::Blocked, Level::LowSpace),
            (2_500_000_000, Level::Blocked, Level::Blocked),
            (2_499_999_999, Level::Blocked, Level::Blocked),
            (2_000_000_001, Level::Normal, Level::LowSpace),
            (2_000_000_000, Level::Normal, Level::LowSpace),
            (1_999_999_999, Level::LowSpace, Level::Blocked),
            (512_000_000, Level::Normal, Level::Blocked),
            (0, Level::Normal, Level::Blocked),
            (5_000_000_000, Level::Blocked, Level::LowSpace),
            (5_000_000_001, Level::Blocked, Level::LowSpace),
            (6_000_000_001, Level::Blocked, Level::Normal),
        ] {
            let level = policy.level(available, before, None);
            assert_eq!(level, expected, "{available} bytes, previously {before:?}");
            let guard = Guard {
                policy: None,
                history: PathBuf::new(),
                writers: Mutex::new(Vec::new()),
                floor: Mutex::new(None),
                value: Mutex::new((
                    Snapshot {
                        enabled: true,
                        level,
                        workspace_available_bytes: Some(available),
                        history_available_bytes: Some(available),
                        reason: "disk_capacity",
                        ..Snapshot::unavailable(true)
                    },
                    Instant::now(),
                )),
            };
            assert_eq!(guard.blocks(), expected == Level::Blocked);
        }
    }

    #[test]
    fn a_rescue_resumes_work_until_half_the_space_left_is_used() {
        let policy: Policy = serde_json::from_str(
            r#"{"agent_uid":1001,"agent_gid":1001,"cache_dir":"/cache","cgroup_root":"/agents"}"#,
        )
        .unwrap();
        let floor = Some(900_000_000);
        assert_eq!(
            policy.level(1_800_000_000, Level::Blocked, floor),
            Level::LowSpace
        );
        assert_eq!(
            policy.level(900_000_000, Level::Blocked, floor),
            Level::LowSpace
        );
        assert_eq!(
            policy.level(899_999_999, Level::LowSpace, floor),
            Level::Blocked
        );
        let guard = Guard {
            policy: None,
            history: PathBuf::new(),
            writers: Mutex::new(Vec::new()),
            floor: Mutex::new(None),
            value: Mutex::new((
                Snapshot {
                    enabled: true,
                    level: Level::Blocked,
                    workspace_available_bytes: Some(1_900_000_000),
                    history_available_bytes: Some(1_800_000_000),
                    reason: "disk_capacity",
                    ..Snapshot::unavailable(true)
                },
                Instant::now(),
            )),
        };
        assert!(guard.rescue());
        assert_eq!(*guard.floor.lock().unwrap(), floor);
        // A 20 GB box's pause line stops at 250 MB, and work stays paused below it.
        let guard = Guard {
            policy: Some(policy),
            ..guard
        };
        for (available, rescued, floor) in [
            (400_000_000, true, Some(250_000_000)),
            (250_000_000, false, Some(250_000_000)),
            (4_000_000, false, Some(250_000_000)),
        ] {
            guard.value.lock().unwrap().0.workspace_available_bytes = Some(available);
            guard.value.lock().unwrap().0.history_available_bytes = Some(available);
            assert_eq!(guard.rescue(), rescued, "{available} bytes");
            assert_eq!(*guard.floor.lock().unwrap(), floor);
        }
    }

    #[test]
    fn missing_or_stale_measurements_do_not_authorize_writes() {
        let guard = Guard {
            policy: None,
            history: PathBuf::new(),
            writers: Mutex::new(Vec::new()),
            floor: Mutex::new(None),
            value: Mutex::new((Snapshot::unavailable(true), Instant::now())),
        };
        assert!(guard.blocks());
        let mut snapshot = Snapshot::unavailable(true);
        snapshot.level = Level::Normal;
        *guard.value.lock().unwrap() = (snapshot, Instant::now() - Duration::from_secs(6));
        assert!(guard.blocks());
    }
}
