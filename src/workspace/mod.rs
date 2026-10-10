pub mod files;
pub mod storage;

use axum::body::Body;
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Mutex,
};
use tokio::{io::AsyncWriteExt, process::Command};
use tokio_stream::StreamExt;

/// Reserved workspace ID for sessions that start in the workspace root itself.
pub const ROOT: &str = "root";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Workspace {
    pub id: String,
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
}

pub struct Workspaces {
    root: PathBuf,
    registry: PathBuf,
    policy: Option<storage::Policy>,
    filesystem: Option<u64>,
    allocation: Mutex<()>,
}

impl Workspaces {
    pub fn open(config: &crate::config::Config) -> io::Result<Self> {
        let mut root = if config.storage.is_some() {
            PathBuf::from("/code")
        } else {
            config.state_dir.join("code")
        };
        let registry = config.state_dir.join("workspaces");
        fs::create_dir_all(&registry)?;
        if config.storage.is_none() {
            fs::create_dir_all(&root)?;
            root = root.canonicalize()?;
        }
        Ok(Self {
            root,
            registry,
            policy: config.storage.clone(),
            filesystem: config
                .storage
                .as_ref()
                .map(|_| fs::metadata(&config.repository).map(|m| m.dev()))
                .transpose()?,
            allocation: Mutex::new(()),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    fn check_filesystem(&self, path: &Path) -> io::Result<()> {
        let Some(expected) = self.filesystem else {
            return Ok(());
        };
        // New workspaces may not exist yet; their nearest existing ancestor owns the allocation.
        for ancestor in path.ancestors() {
            match fs::symlink_metadata(ancestor) {
                Ok(info) if !info.file_type().is_symlink() && info.dev() == expected => {
                    return Ok(());
                }
                Ok(_) => {
                    return Err(io::Error::other(
                        "workspace is outside the monitored filesystem",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::other("workspace filesystem unavailable"))
    }

    pub(crate) fn check_filesystems(&self) -> io::Result<()> {
        if self.filesystem.is_some() {
            let info = fs::symlink_metadata(&self.root)?;
            if !info.is_dir() {
                return Err(io::Error::other("workspace root is unavailable"));
            }
            self.check_filesystem(&self.root)?;
            for workspace in self.list()? {
                self.check_filesystem(&workspace.path)?;
            }
        }
        Ok(())
    }

    fn validate(&self, workspace: &Workspace) -> io::Result<()> {
        self.check_filesystem(&workspace.path)?;
        valid_id(&workspace.id)?;
        if workspace.path.components().any(|p| {
            !matches!(
                p,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        }) {
            return Err(io::Error::other("Invalid workspace path"));
        }
        if let Some(parent) = &workspace.parent {
            valid_id(parent)?;
            if parent == &workspace.id {
                return Err(io::Error::other("Invalid workspace parent"));
            }
            let parent = self
                .read_registered(parent)?
                .ok_or_else(|| io::Error::other("Workspace parent missing"))?;
            if parent.parent.is_some()
                || parent.path.parent() != Some(self.root.as_path())
                || !workspace.path.starts_with(&parent.path)
            {
                return Err(io::Error::other("Invalid workspace directory"));
            }
        } else if workspace.path.parent() != Some(self.root.as_path()) {
            return Err(io::Error::other("Invalid workspace root"));
        }
        Ok(())
    }

    fn read_registered(&self, id: &str) -> io::Result<Option<Workspace>> {
        valid_id(id)?;
        // Read old reservations without requiring their abandoned archive import to finish.
        for extension in ["json", "pending"] {
            match fs::read(self.registry.join(format!("{id}.{extension}"))) {
                Ok(bytes) => {
                    let workspace: Workspace = serde_json::from_slice(&bytes)?;
                    if workspace.id != id {
                        return Err(io::Error::other("Workspace identity mismatch"));
                    }
                    return Ok(Some(workspace));
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    pub fn get(&self, id: &str) -> io::Result<Option<Workspace>> {
        let workspace = self.read_registered(id)?;
        if let Some(workspace) = &workspace {
            self.validate(workspace)?;
        }
        Ok(workspace)
    }

    pub fn list(&self) -> io::Result<Vec<Workspace>> {
        let mut result = std::collections::BTreeMap::new();
        for entry in fs::read_dir(&self.registry)? {
            let path = entry?.path();
            if matches!(
                path.extension().and_then(|v| v.to_str()),
                Some("json" | "pending")
            ) {
                let id = path
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .ok_or_else(|| io::Error::other("Invalid workspace ID"))?;
                if let Some(workspace) = self.get(id)? {
                    result.insert(id.to_owned(), workspace);
                }
            }
        }
        Ok(result.into_values().collect())
    }

    pub fn resolve(&self, id: &str, name: Option<&str>) -> io::Result<Workspace> {
        valid_id(id)?;
        let name = name.unwrap_or(id);
        valid_name(name)?;
        let _guard = self.allocation.lock().unwrap();
        if let Some(workspace) = self.get(id)? {
            return Ok(workspace);
        }
        let occupied = self.list()?;
        let unavailable = |path: &PathBuf| {
            occupied.iter().any(|w| w.path == *path) || fs::symlink_metadata(path).is_ok()
        };
        let mut path = self.root.join(name);
        if unavailable(&path) {
            path = self.root.join(format!("{name}-{id}"));
        }
        if unavailable(&path) {
            return Err(io::Error::other("Workspace destination is occupied"));
        }
        let workspace = Workspace {
            id: id.to_owned(),
            path,
            parent: None,
        };
        let temporary = self.registry.join(format!("{id}.tmp"));
        fs::write(&temporary, serde_json::to_vec(&workspace)?)?;
        fs::File::open(&temporary)?.sync_all()?;
        fs::rename(temporary, self.registry.join(format!("{id}.json")))?;
        fs::File::open(&self.registry)?.sync_all()?;
        Ok(workspace)
    }

    pub async fn ensure_directory(&self, workspace: &Workspace) -> io::Result<()> {
        self.check_filesystem(&workspace.path)?;
        if workspace.id != "legacy" {
            self.validate(workspace)?;
        }
        let path = if workspace.id == "legacy" {
            workspace.path.canonicalize()?
        } else {
            workspace.path.clone()
        };
        let mut command = Command::new("python3");
        command
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .args(["-c", include_str!("../sync/files.py")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(policy) = &self.policy {
            crate::runtime::linux::as_agent(
                &mut command,
                policy.agent_uid,
                policy.agent_gid,
                None,
                true,
            )?;
        }
        let mut child = command.spawn()?;
        let mut input = child.stdin.take().unwrap();
        input
            .write_all(&serde_json::to_vec(
                &serde_json::json!({"op":"mkdir","tree":{"root":path,"create":true}}),
            )?)
            .await?;
        input.write_all(b"\n").await?;
        drop(input);
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(5), child.wait_with_output())
                .await??;
        file_result(output)?;
        Ok(())
    }

    pub async fn checkout(&self, workspace: &Workspace) -> io::Result<serde_json::Value> {
        let git = async |args: &[&str]| -> io::Result<Option<String>> {
            let mut command = Command::new("git");
            command
                .env_clear()
                .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                .current_dir(&workspace.path)
                .args(args)
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            if let Some(policy) = &self.policy {
                crate::runtime::linux::as_agent(
                    &mut command,
                    policy.agent_uid,
                    policy.agent_gid,
                    None,
                    true,
                )?;
            }
            let output =
                tokio::time::timeout(std::time::Duration::from_secs(2), command.output()).await??;
            Ok(output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
                .filter(|v| !v.is_empty()))
        };
        let branch = git(&["symbolic-ref", "--quiet", "--short", "HEAD"]).await?;
        let head = git(&["rev-parse", "--verify", "HEAD"]).await?;
        Ok(serde_json::json!({"path":workspace.path,"branch":branch,"head":head}))
    }

    pub async fn install_transfer(
        &self,
        workspace: &Workspace,
        id: &str,
        entry: &crate::session::teleport::Entry,
        source: &Path,
        native: Option<(&crate::config::HarnessConfig, crate::runtime::Kind, &str)>,
        guard: &storage::Guard,
    ) -> io::Result<String> {
        self.ensure_directory(workspace).await?;
        let (root, relative) = match native {
            // Claude resumes by ID only from the project folder named after the cwd.
            Some((profile, crate::runtime::Kind::Claude, native_id)) => (
                profile.home.join("projects"),
                format!(
                    "{}/{native_id}.jsonl",
                    workspace
                        .path
                        .to_string_lossy()
                        .chars()
                        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
                        .collect::<String>()
                ),
            ),
            Some((profile, crate::runtime::Kind::Cursor, _)) => {
                (profile.home.join("chats"), format!(".teleport-{id}.jsonl"))
            }
            Some((profile, _, _)) => (
                profile.home.join("sessions"),
                format!("teleport/{id}/{}", entry.path),
            ),
            None => (
                workspace.path.clone(),
                format!(".cloudroom/teleport/{id}/{}/{}", entry.kind, entry.path),
            ),
        };
        let request = serde_json::json!({"op":"teleport","tree":{"root":root,"create":true},
            "path":relative,"size":entry.size,"sha256":entry.sha256,"executable":entry.executable,"symlink":entry.symlink,
            "project_path":(entry.kind == "project").then_some(&entry.path),
            "native":native.map(|(_,kind,native_id)| serde_json::json!({"harness":kind,"id":native_id,"cwd":workspace.path}))});
        let mut command = Command::new("python3");
        command
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .args(["-I", "-c", include_str!("../sync/files.py")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let (mut child, _workload) = guard.spawn_writer(&mut command)?;
        let mut input = child.stdin.take().unwrap();
        let mut file = fs::File::open(source)?;
        let result = async {
            use std::io::Read;
            input.write_all(&serde_json::to_vec(&request)?).await?;
            input.write_all(b"\n").await?;
            let mut buffer = vec![0; 64 * 1024];
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                input.write_all(&buffer[..count]).await?;
            }
            Ok::<_, io::Error>(())
        }
        .await;
        drop(input);
        let output = child.wait_with_output().await?;
        let installed = file_result(output)?;
        result?;
        let path = installed["path"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| io::Error::other("transfer path missing"))?;
        match native {
            Some((profile, crate::runtime::Kind::Cursor, _)) => {
                restore_cursor(&profile.home.join("chats"), &path, workspace, guard).await
            }
            _ => Ok(path),
        }
    }

    pub async fn attach(
        &self,
        workspace: &Workspace,
        request_id: &str,
        name: &str,
        kind: &str,
        body: Body,
        guard: &storage::Guard,
    ) -> io::Result<serde_json::Value> {
        if guard.blocks() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "storage unsafe; file writes are blocked",
            ));
        }
        self.ensure_directory(workspace).await?;
        valid_id(request_id)?;
        if kind != "image" && kind != "file" {
            return Err(io::Error::other("attachment kind must be image or file"));
        }
        if name.is_empty()
            || name.len() > 200
            || name.contains('/')
            || name.contains('\\')
            || name == "."
            || name == ".."
            || name.bytes().any(|b| b < 32 || b == b':')
        {
            return Err(io::Error::other("invalid attachment name"));
        }
        let relative = format!(".cloudroom/attachments/{request_id}/{name}");
        let limit = if kind == "image" {
            10 * 1024 * 1024
        } else {
            25 * 1024 * 1024
        };
        let mut bytes = Vec::new();
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(io::Error::other)?;
            if bytes.len() + chunk.len() > limit {
                return Err(io::ErrorKind::FileTooLarge.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut command = Command::new("python3");
        command
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .args(["-c", include_str!("../sync/files.py")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let (mut child, _workload) = guard.spawn_writer(&mut command)?;
        let mut input = child.stdin.take().unwrap();
        let request = serde_json::json!({"op":"attach", "tree":{"root":workspace.path},
            "path":relative, "limit":limit, "size":bytes.len()});
        let transfer = async {
            input.write_all(&serde_json::to_vec(&request)?).await?;
            input.write_all(b"\n").await?;
            input.write_all(&bytes).await
        }
        .await;
        drop(input);
        let output = child.wait_with_output().await?;
        let result = file_result(output)?;
        // An idempotent retry may close stdin before consuming the repeated body.
        transfer.or_else(|error| {
            if error.kind() == io::ErrorKind::BrokenPipe {
                Ok(())
            } else {
                Err(error)
            }
        })?;
        Ok(serde_json::json!({"id":request_id,"name":name,"kind":kind,
            "path":workspace.path.join(relative),"size":result["size"]}))
    }
}

fn file_result(output: std::process::Output) -> io::Result<serde_json::Value> {
    let result: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if !output.status.success() || result["ok"] != true {
        return Err(if result["error"] == "attachment_too_large" {
            io::ErrorKind::FileTooLarge.into()
        } else {
            result["errno"]
                .as_i64()
                .and_then(|errno| i32::try_from(errno).ok())
                .map(io::Error::from_raw_os_error)
                .unwrap_or_else(|| io::Error::other("Cloud folder file operation failed"))
        });
    }
    Ok(result)
}

/// Unpack a teleported Cursor snapshot into chats/<md5 of the workspace>/<chat ID>.
async fn restore_cursor(
    root: &Path,
    snapshot: &str,
    workspace: &Workspace,
    guard: &storage::Guard,
) -> io::Result<String> {
    let mut command = Command::new("python3");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .args([
            "-I",
            "-c",
            include_str!("../runtime/cursor-history.py"),
            "restore",
        ])
        .arg(root)
        .arg(&workspace.path)
        .stdin(fs::File::open(snapshot)?)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let (child, _workload) = guard.spawn_writer(&mut command)?;
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "Cursor session restore failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let restored: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    restored["path"]
        .as_str()
        .map(|path| format!("{path}/meta.json"))
        .ok_or_else(|| io::Error::other("Cursor session restore returned no path"))
}

pub fn transfer_destination(
    workspace: &Workspace,
    id: &str,
    entry: &crate::session::teleport::Entry,
) -> PathBuf {
    workspace.path.join(format!(
        ".cloudroom/teleport/{id}/{}/{}",
        entry.kind, entry.path
    ))
}

pub fn valid_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 80
        || name.starts_with('.')
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(io::Error::other("Invalid workspace name"));
    }
    Ok(())
}

pub fn valid_id(id: &str) -> io::Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(io::Error::other("Invalid workspace ID"));
    }
    Ok(())
}
