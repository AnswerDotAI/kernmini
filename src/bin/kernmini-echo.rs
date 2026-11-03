use kernmini::{Error, ErrorKind, ExecuteOutcome, ExecuteRequest, ExecutionContext, KernelInfo, LanguageError, LanguageEvent, LanguageSession, Result};
use serde_json::json;
use std::env;
use tokio::time;

#[derive(Clone)]
struct EchoSession;

#[async_trait::async_trait]
impl LanguageSession for EchoSession {
    fn kernel_info(&self) -> Result<KernelInfo> {
        Ok(KernelInfo { implementation: "echokernel".into(), implementation_version: "0.0.1".into(), banner: "echo".into(),
            language_info: json!({"name": "echo", "version": "1.0", "mimetype": "text/plain", "file_extension": ".txt"}) })
    }

    async fn execute(&self, request: ExecuteRequest, context: ExecutionContext) -> Result<ExecuteOutcome> {
        if request.code == "adapter-error" { return Err(Error::adapter("echo adapter failed")); }
        context.emit(LanguageEvent::Stream { name: "stdout".into(), text: format!("echo: {}\n", request.code) }).await?;
        if let Some(seconds) = request.code.strip_prefix("sleep:") {
            let seconds = seconds.parse().map_err(|error| Error::new(ErrorKind::InvalidInput, "invalid sleep duration").caused_by(error))?;
            time::sleep(std::time::Duration::from_secs_f64(seconds)).await;
        }
        let error = (request.code == "boom").then(|| LanguageError { ename: "EchoError".into(), evalue: request.code.clone(), traceback: vec![] });
        Ok(ExecuteOutcome { result: error.is_none().then(|| json!({"text/plain": request.code.to_uppercase()})), error, ..Default::default() })
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let connection_file = env::args().nth(1).ok_or_else(|| Error::new(ErrorKind::InvalidInput, "usage: kernmini-echo CONNECTION_FILE"))?;
    kernmini::run_kernel(connection_file, EchoSession).await
}
