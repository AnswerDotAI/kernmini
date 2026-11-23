use crate::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::env;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub transport: String, pub ip: String, pub shell_port: u16, pub iopub_port: u16, pub stdin_port: u16, pub control_port: u16, pub hb_port: u16,
    #[serde(default)] pub key: String, pub signature_scheme: String
}

impl ConnectionInfo {
    pub fn read(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|error| Error::from(error).context(format!("reading {}", path.display())))?;
        serde_json::from_slice(&bytes).map_err(|error| Error::from(error).context(format!("parsing {}", path.display())))
    }

    /// Allocate 5 distinct ephemeral TCP ports on loopback with a fresh HMAC key.
    pub fn allocate_local() -> Result<Self> {
        let listeners = [
            TcpListener::bind("127.0.0.1:0")?, TcpListener::bind("127.0.0.1:0")?, TcpListener::bind("127.0.0.1:0")?,
            TcpListener::bind("127.0.0.1:0")?, TcpListener::bind("127.0.0.1:0")?,
        ];
        let mut ports = [0u16; 5];
        for (slot, listener) in ports.iter_mut().zip(&listeners) { *slot = listener.local_addr()?.port(); }
        drop(listeners);
        Ok(Self {
            transport: "tcp".into(), ip: "127.0.0.1".into(),
            shell_port: ports[0], iopub_port: ports[1], stdin_port: ports[2], control_port: ports[3], hb_port: ports[4],
            key: Uuid::new_v4().simple().to_string(), signature_scheme: "hmac-sha256".into(),
        })
    }

    pub fn address(&self, port: u16) -> Result<String> {
        if self.transport != "tcp" { return Err(Error::new(ErrorKind::Unavailable, "only TCP connection files are supported")); }
        if self.signature_scheme != "hmac-sha256" {
            return Err(Error::new(ErrorKind::Unavailable, format!("unsupported signature scheme {}", self.signature_scheme)));
        }
        Ok(format!("{}:{port}", self.ip))
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// RAII handle for a Jupyter connection file on disk; removes the file on drop.
#[derive(Debug)]
pub struct ConnectionFile {
    path: PathBuf,
    info: ConnectionInfo,
}

impl ConnectionFile {
    /// Allocate 5 distinct local ports and atomically write a temp connection file.
    pub fn create_local(prefix: &str) -> Result<Self> {
        Self::write_temp(ConnectionInfo::allocate_local()?, prefix)
    }

    /// Atomically write `info` to `{prefix}-kernel-{uuid}.json` in `env::temp_dir()`.
    pub fn write_temp(info: ConnectionInfo, prefix: &str) -> Result<Self> {
        let id = Uuid::new_v4().simple();
        let dir = env::temp_dir();
        let path = dir.join(format!("{prefix}-kernel-{id}.json"));
        let tmp = dir.join(format!(".{prefix}-kernel-{id}.tmp"));
        let bytes = serde_json::to_vec(&info)?;
        if let Err(error) = write_new(&tmp, &bytes).and_then(|()| fs::rename(&tmp, &path)) {
            let _ = fs::remove_file(&tmp);
            return Err(Error::from(error).context(format!("writing {}", path.display())));
        }
        Ok(Self { path, info })
    }

    pub fn path(&self) -> &Path { &self.path }
    pub fn info(&self) -> &ConnectionInfo { &self.info }
}

impl Drop for ConnectionFile {
    fn drop(&mut self) { let _ = fs::remove_file(&self.path); }
}
