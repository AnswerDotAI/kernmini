use kernmini::{ExecuteOutcome, ExecuteRequest, ExecutionContext, KernelInfo, Language, LanguageError, LanguageSession};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
struct EchoSession { execution_count: Arc<AtomicU64> }

#[async_trait::async_trait]
impl LanguageSession for EchoSession {
    fn kernel_info(&self) -> anyhow::Result<KernelInfo> {
        Ok(KernelInfo {
            implementation: "echokernel".into(),
            implementation_version: "0.0.1".into(),
            banner: "echo".into(),
            language_info: json!({"name": "echo", "version": "1.0", "mimetype": "text/plain", "file_extension": ".txt"}),
        })
    }

    fn execution_count(&self) -> u64 { self.execution_count.load(Ordering::Acquire) }

    async fn execute(&self, request: ExecuteRequest, context: ExecutionContext) -> anyhow::Result<ExecuteOutcome> {
        let execution_count = self.execution_count.fetch_add(1, Ordering::AcqRel) + 1;
        context.stream("stdout", format!("echo: {}\n", request.code));
        if let Some(seconds) = request.code.strip_prefix("sleep:") { tokio::time::sleep(std::time::Duration::from_secs_f64(seconds.parse()?)).await; }
        let error = (request.code == "boom").then(|| LanguageError { ename: "EchoError".into(), evalue: request.code.clone(), traceback: vec![] });
        Ok(ExecuteOutcome {
            execution_count,
            result: error.is_none().then(|| json!({"text/plain": request.code.to_uppercase()})),
            result_metadata: json!({}),
            error,
            user_expressions: json!({}),
            payload: json!([]),
        })
    }
}

struct EchoLanguage;

#[async_trait::async_trait]
impl Language for EchoLanguage {
    type Session = EchoSession;

    fn parent(&self) -> Self::Session { EchoSession { execution_count: Arc::new(AtomicU64::new(0)) } }

    async fn create_child(&self) -> anyhow::Result<Self::Session> { Ok(self.parent()) }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection_file = std::env::args().nth(1).ok_or_else(|| anyhow::anyhow!("usage: kernmini-echo CONNECTION_FILE"))?;
    kernmini::run_kernel(connection_file, EchoLanguage).await
}
