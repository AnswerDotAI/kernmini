use bytes::Bytes;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value, json};
use sha2::Sha256;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

const DELIMITER: &[u8] = b"<IDS|MSG>";
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug)]
pub enum WireError {
    MissingDelimiter,
    MissingParts,
    BadSignature,
    DuplicateSignature,
    Json(serde_json::Error),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingDelimiter => write!(f, "missing <IDS|MSG> delimiter"),
            Self::MissingParts => write!(f, "insufficient Jupyter message parts"),
            Self::BadSignature => write!(f, "invalid message signature"),
            Self::DuplicateSignature => write!(f, "duplicate message signature"),
            Self::Json(error) => write!(f, "invalid message JSON: {error}"),
        }
    }
}

impl std::error::Error for WireError {}

impl From<serde_json::Error> for WireError { fn from(error: serde_json::Error) -> Self { Self::Json(error) } }

#[derive(Clone, Debug)]
pub struct Message {
    pub identities: Vec<Bytes>,
    pub header: Map<String, Value>,
    pub parent_header: Map<String, Value>,
    pub metadata: Map<String, Value>,
    pub content: Value,
    pub buffers: Vec<Bytes>,
}

impl Message {
    pub fn msg_type(&self) -> &str { self.header.get("msg_type").and_then(Value::as_str).unwrap_or("") }

    pub fn msg_id(&self) -> &str { self.header.get("msg_id").and_then(Value::as_str).unwrap_or("") }
}

#[derive(Clone)]
pub struct Session {
    key: Arc<Vec<u8>>,
    seen: Arc<Mutex<HashSet<Vec<u8>>>>,
    session: Arc<String>,
    username: Arc<String>,
}

impl Session {
    pub fn new(key: impl Into<Vec<u8>>, username: impl Into<String>) -> Self {
        Self {
            key: Arc::new(key.into()),
            seen: Arc::new(Mutex::new(HashSet::new())),
            session: Arc::new(uuid::Uuid::new_v4().to_string()),
            username: Arc::new(username.into()),
        }
    }

    fn signature(&self, json_parts: &[Bytes]) -> Vec<u8> {
        if self.key.is_empty() { return vec![]; }
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts arbitrary key sizes");
        for part in json_parts { mac.update(part); }
        hex::encode(mac.finalize().into_bytes()).into_bytes()
    }

    pub fn decode(&self, frames: Vec<Bytes>) -> Result<Message, WireError> {
        let delimiter = frames.iter().position(|part| &part[..] == DELIMITER).ok_or(WireError::MissingDelimiter)?;
        if frames.len() < delimiter + 6 { return Err(WireError::MissingParts); }
        let signature = &frames[delimiter + 1];
        let json_parts = &frames[delimiter + 2..delimiter + 6];
        if !self.key.is_empty() {
            if self.signature(json_parts) != signature.as_ref() { return Err(WireError::BadSignature); }
            let mut seen = self.seen.lock().expect("wire replay lock poisoned");
            if !seen.insert(signature.to_vec()) { return Err(WireError::DuplicateSignature); }
        }
        Ok(Message {
            identities: frames[..delimiter].to_vec(),
            header: serde_json::from_slice(&json_parts[0])?,
            parent_header: serde_json::from_slice(&json_parts[1])?,
            metadata: serde_json::from_slice(&json_parts[2])?,
            content: serde_json::from_slice(&json_parts[3])?,
            buffers: frames[delimiter + 6..].to_vec(),
        })
    }

    pub fn encode(&self, message: &Message) -> Result<Vec<Bytes>, WireError> {
        let json_parts = [
            Bytes::from(serde_json::to_vec(&message.header)?),
            Bytes::from(serde_json::to_vec(&message.parent_header)?),
            Bytes::from(serde_json::to_vec(&message.metadata)?),
            Bytes::from(serde_json::to_vec(&message.content)?),
        ];
        let mut frames = message.identities.clone();
        frames.push(Bytes::from_static(DELIMITER));
        frames.push(Bytes::from(self.signature(&json_parts)));
        frames.extend(json_parts);
        frames.extend(message.buffers.clone());
        Ok(frames)
    }

    pub fn message(&self, msg_type: &str, content: Value, parent: Option<&Message>) -> Message {
        let header = json!({
            "msg_id": uuid::Uuid::new_v4().to_string(),
            "session": self.session.as_str(),
            "username": self.username.as_str(),
            "date": chrono::Utc::now().to_rfc3339(),
            "msg_type": msg_type,
            "version": "5.3",
        });
        Message {
            identities: vec![],
            header: header.as_object().unwrap().clone(),
            parent_header: parent.map(|message| message.header.clone()).unwrap_or_default(),
            metadata: Map::new(),
            content,
            buffers: vec![],
        }
    }

    pub fn reply(&self, request: &Message, msg_type: &str, content: Value) -> Message { self.message(msg_type, content, Some(request)) }
}
