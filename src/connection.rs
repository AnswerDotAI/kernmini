use serde::Deserialize;
use std::path::Path;

#[derive(Clone, Debug, Deserialize)]
pub struct ConnectionInfo {
    pub transport: String,
    pub ip: String,
    pub shell_port: u16,
    pub iopub_port: u16,
    pub stdin_port: u16,
    pub control_port: u16,
    pub hb_port: u16,
    #[serde(default)]
    pub key: String,
    pub signature_scheme: String,
}

impl ConnectionInfo {
    pub fn read(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|error| crate::Error::from(error).context(format!("reading {}", path.display())))?;
        serde_json::from_slice(&bytes).map_err(|error| crate::Error::from(error).context(format!("parsing {}", path.display())))
    }

    pub fn address(&self, port: u16) -> crate::Result<String> {
        if self.transport != "tcp" { return Err(crate::Error::new(crate::ErrorKind::Unavailable, "only TCP connection files are supported")); }
        if self.signature_scheme != "hmac-sha256" {
            return Err(crate::Error::new(crate::ErrorKind::Unavailable, format!("unsupported signature scheme {}", self.signature_scheme)));
        }
        Ok(format!("{}:{port}", self.ip))
    }
}
