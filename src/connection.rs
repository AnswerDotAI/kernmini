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
    pub fn read(path: impl AsRef<Path>) -> anyhow::Result<Self> { Ok(serde_json::from_slice(&std::fs::read(path)?)?) }

    pub fn address(&self, port: u16) -> anyhow::Result<String> {
        anyhow::ensure!(self.transport == "tcp", "only TCP connection files are supported in the current Rust slice");
        anyhow::ensure!(self.signature_scheme == "hmac-sha256", "unsupported signature scheme {}", self.signature_scheme);
        Ok(format!("{}:{port}", self.ip))
    }
}
