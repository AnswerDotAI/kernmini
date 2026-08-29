use kernmini::{Session, WireError};
use serde_json::json;

#[test]
fn signed_round_trip_and_replay() {
    let session = Session::new(b"secret".to_vec(), "kernel");
    let message = session.message("execute_request", json!({"code": "1+1"}), None);
    let frames = session.encode(&message).unwrap();
    let decoded = session.decode(frames.clone()).unwrap();
    assert_eq!(decoded.msg_type(), "execute_request");
    assert_eq!(decoded.content["code"], "1+1");
    assert!(matches!(session.decode(frames), Err(WireError::DuplicateSignature)));
}
