use async_trait::async_trait;
use kernmini::{
    ExecuteOutcome, ExecuteRequest, ExecutionContext, KernelInfo, KernelInterrupter, LanguageError, LanguageSession, Result, Session,
    run_kernel_with_interrupter,
};
use serde_json::json;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
use tokio::time;
use zmtpmini::Dealer;

#[derive(Clone)]
struct StdinLanguage;

#[async_trait]
impl LanguageSession for StdinLanguage {
    fn kernel_info(&self) -> Result<KernelInfo> {
        Ok(KernelInfo { implementation: "stdin-test".into(), implementation_version: "0.1".into(), banner: String::new(), language_info: json!({"name": "test"})
        })
    }

    async fn execute(&self, request: ExecuteRequest, context: ExecutionContext) -> Result<ExecuteOutcome> {
        let res = tokio::task::spawn_blocking(move || context.input(request.code, false)).await.unwrap();
        Ok(match res {
            Ok(value) => ExecuteOutcome { result: Some(json!({"text/plain": value})), ..Default::default() }, Err(err) => ExecuteOutcome {
                error: Some(LanguageError { ename: format!("{:?}", err.kind()), evalue: err.to_string(), traceback: vec![] }), ..Default::default() }
        })
    }
}

fn free_port() -> u16 { TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }

async fn connect_retry(addr: SocketAddr, id: &[u8]) -> Dealer<tokio::net::TcpStream> {
    let deadline = time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(d) = Dealer::connect(addr, Some(id)).await { return d; }
        assert!(time::Instant::now() < deadline, "timed out connecting to {addr}");
        time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn stdin_input_interrupt_disconnect_and_reply() -> Result<()> {
    let (shell_port, iopub_port, stdin_port, control_port, hb_port) = (free_port(), free_port(), free_port(), free_port(), free_port());
    let conn_path = std::env::temp_dir().join(format!("kernmini-stdin-{}.json", std::process::id()));
    let conn = json!({
        "transport": "tcp", "ip": "127.0.0.1", "shell_port": shell_port, "iopub_port": iopub_port,
        "stdin_port": stdin_port, "control_port": control_port, "hb_port": hb_port,
        "key": "test", "signature_scheme": "hmac-sha256"
    });
    std::fs::write(&conn_path, conn.to_string())?;

    let interrupter = KernelInterrupter::default();
    let kernel_path = conn_path.clone();
    let kernel_interrupt = interrupter.clone();
    let kernel = tokio::spawn(async move { run_kernel_with_interrupter(&kernel_path, StdinLanguage, kernel_interrupt).await });

    let session = Session::new(b"test".to_vec(), "client");
    let shell_addr: SocketAddr = format!("127.0.0.1:{shell_port}").parse().unwrap();
    let stdin_addr: SocketAddr = format!("127.0.0.1:{stdin_port}").parse().unwrap();
    let control_addr: SocketAddr = format!("127.0.0.1:{control_port}").parse().unwrap();

    // Missing stdin peer: interrupting unblocks input() cleanly with Interrupted.
    let mut missing_shell = connect_retry(shell_addr, b"missing").await;
    let req = session.message("execute_request", json!({"code": "prompt", "allow_stdin": true}), None);
    missing_shell.send(session.encode(&req)?).await?;
    time::sleep(Duration::from_millis(50)).await;
    interrupter.interrupt();
    let reply = session.decode(time::timeout(Duration::from_secs(2), missing_shell.recv()).await.unwrap()?)?;
    assert_eq!(reply.msg_type(), "execute_reply");
    assert_eq!(reply.content["ename"], "Interrupted");

    // Connected stdin peer: interrupt, disconnect, and reply scenarios.
    for scenario in ["interrupt", "disconnect", "reply"] {
        let mut shell = connect_retry(shell_addr, scenario.as_bytes()).await;
        let mut stdin = connect_retry(stdin_addr, scenario.as_bytes()).await;
        let req = session.message("execute_request", json!({"code": "prompt", "allow_stdin": true}), None);
        shell.send(session.encode(&req)?).await?;
        let prompt = session.decode(time::timeout(Duration::from_secs(2), stdin.recv()).await.unwrap()?)?;
        assert_eq!(prompt.msg_type(), "input_request");
        match scenario {
            "interrupt" => interrupter.interrupt(),
            "disconnect" => drop(stdin),
            _ => {
                let input_reply = session.message("input_reply", json!({"value": "ok"}), Some(&prompt));
                stdin.send(session.encode(&input_reply)?).await?;
            }
        }
        let reply = session.decode(time::timeout(Duration::from_secs(2), shell.recv()).await.unwrap()?)?;
        assert_eq!(reply.msg_type(), "execute_reply");
        match scenario {
            "interrupt" => assert_eq!(reply.content["ename"], "Interrupted"), "disconnect" => assert_eq!(reply.content["ename"], "Closed"),
            _ => assert_eq!(reply.content["status"], "ok")
        }
    }

    let mut control = connect_retry(control_addr, b"ctrl").await;
    let shutdown = session.message("shutdown_request", json!({"restart": false}), None);
    control.send(session.encode(&shutdown)?).await?;
    let _ = time::timeout(Duration::from_secs(2), control.recv()).await;
    time::timeout(Duration::from_secs(5), kernel).await.unwrap().unwrap()?;
    let _ = std::fs::remove_file(&conn_path);
    Ok(())
}
