use crate::{
    CompleteRequest, DebugEventSender, ExecuteOutcome, ExecuteRequest, ExecutionContext, InspectRequest, KernelInfo, KernelInterrupter, Language,
    LanguageError, LanguageMessage, LanguageSession,
};
use async_trait::async_trait;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

pub(crate) fn py_to_json(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    let text: String = py.import("json")?.call_method1("dumps", (value,))?.extract()?;
    serde_json::from_str(&text).map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

pub(crate) fn json_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> { Ok(py.import("json")?.call_method1("loads", (value.to_string(),))?.unbind()) }

fn py_buffers(value: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
    if value.is_none() { return Ok(vec![]); }
    value.try_iter()?.map(|item| Ok(item?.cast::<PyBytes>()?.as_bytes().to_vec())).collect()
}

#[pyclass]
struct ExecutionSink {
    context: ExecutionContext,
    target: Option<Py<PyAny>>,
    locals: Option<pyo3_async_runtimes::TaskLocals>,
    interrupted: Option<Arc<AtomicBool>>,
    wake_sync: bool,
}

#[pymethods]
impl ExecutionSink {
    fn started(&self, py: Python<'_>, task: Py<PyAny>) -> PyResult<()> {
        let (Some(target), Some(locals), Some(interrupted)) = (&self.target, &self.locals, &self.interrupted) else { return Ok(()) };
        let target = target.clone_ref(py);
        let locals = locals.clone();
        let interrupted = interrupted.clone();
        let wake_sync = self.wake_sync;
        self.context
            .set_interrupt_handler(Arc::new(move || {
                Python::attach(|py| -> PyResult<()> {
                    let sync_thread = target.getattr(py, "_sync_thread_id")?;
                    if !sync_thread.is_none(py) {
                        let thread_id: libc::c_long = sync_thread.extract(py)?;
                        let changed = unsafe { pyo3::ffi::PyThreadState_SetAsyncExc(thread_id, pyo3::ffi::PyExc_KeyboardInterrupt) };
                        if changed > 1 {
                            unsafe { pyo3::ffi::PyThreadState_SetAsyncExc(thread_id, std::ptr::null_mut()); }
                            return Err(PyRuntimeError::new_err("interrupt matched multiple Python threads"));
                        }
                        #[cfg(unix)]
                        if wake_sync && changed == 1 && unsafe { libc::pthread_kill(thread_id as libc::pthread_t, libc::SIGINT) } != 0 {
                            return Err(PyRuntimeError::new_err("could not wake the interrupted Python thread"));
                        }
                    }
                    let cancel = task.getattr(py, "cancel")?;
                    locals.event_loop(py).call_method1("call_soon_threadsafe", (cancel,))?;
                    interrupted.store(true, AtomicOrdering::Release);
                    Ok(())
                })
                .map_err(|error| anyhow::anyhow!(error.to_string()))
            }))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn unlock(&self) -> bool { self.context.unlock() }

    fn open_subshell(&self, py: Python<'_>) -> PyResult<String> {
        let context = self.context.clone();
        py.detach(|| context.open_subshell()).map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn close_subshell(&self, py: Python<'_>, subshell_id: String) -> PyResult<()> {
        let context = self.context.clone();
        py.detach(|| context.close_subshell(subshell_id)).map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn parent(&self, py: Python<'_>) -> PyResult<Py<PyAny>> { json_to_py(py, &self.context.parent()) }

    fn publish(
        &self,
        py: Python<'_>,
        msg_type: String,
        content: &Bound<'_, PyAny>,
        metadata: &Bound<'_, PyAny>,
        identity: &Bound<'_, PyAny>,
        buffers: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let identity = if identity.is_none() { None } else { Some(identity.cast::<PyBytes>()?.as_bytes().to_vec()) };
        self.context.publish(msg_type, py_to_json(py, content)?, py_to_json(py, metadata)?, identity, py_buffers(buffers)?);
        Ok(())
    }

    fn stream(&self, name: String, text: String) { self.context.stream(name, text); }

    fn display(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        let event = event.cast::<PyDict>()?;
        let buffer_value = event.get_item("buffers")?;
        let buffers = buffer_value.as_ref().map(|value| py_buffers(value.as_any())).transpose()?.unwrap_or_default();
        let clean = event.copy()?;
        if buffer_value.is_some() { clean.del_item("buffers")? }
        self.context.display_buffers(py_to_json(py, clean.as_any())?, buffers);
        Ok(())
    }

    fn input(&self, py: Python<'_>, prompt: String, password: bool) -> PyResult<String> {
        let context = self.context.clone();
        py.detach(|| context.input(prompt, password)).map_err(|error| {
            if error.to_string() == "KeyboardInterrupt" { PyKeyboardInterrupt::new_err("") } else { PyRuntimeError::new_err(error.to_string()) }
        })
    }
}

#[pyclass]
struct StreamRouter { current: Py<PyAny> }

#[pymethods]
impl StreamRouter {
    fn __call__(&self, py: Python<'_>, name: String, text: String) -> PyResult<()> {
        let sink = self.current.call_method0(py, "get")?;
        if !sink.is_none(py) { sink.call_method1(py, "stream", (name, text))?; }
        Ok(())
    }
}

#[pyclass]
struct DisplayRouter { current: Py<PyAny> }

#[pyclass]
struct SignalRouter { interrupt: KernelInterrupter, target: Py<PyAny> }

#[pymethods]
impl SignalRouter {
    fn __call__(&self, py: Python<'_>, _signum: i32, _frame: Py<PyAny>) -> PyResult<()> {
        if !self.target.getattr(py, "_sync_thread_id")?.is_none(py) { return Err(PyKeyboardInterrupt::new_err("")); }
        self.interrupt.interrupt();
        Ok(())
    }
}

#[pyclass]
struct DebugRouter { sender: DebugEventSender }

#[pymethods]
impl DebugRouter {
    fn __call__(&self, py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<()> {
        (self.sender)(py_to_json(py, event)?);
        Ok(())
    }
}

#[pyclass]
struct InputRouter { current: Py<PyAny> }

#[pymethods]
impl InputRouter {
    fn __call__(&self, py: Python<'_>, prompt: String, password: bool) -> PyResult<String> {
        let sink = self.current.call_method0(py, "get")?;
        if sink.is_none(py) { return Err(PyRuntimeError::new_err("input requested outside an execution")); }
        sink.call_method1(py, "input", (prompt, password))?.extract(py)
    }
}

#[pymethods]
impl DisplayRouter {
    fn __call__(&self, py: Python<'_>, event: Py<PyAny>) -> PyResult<()> {
        let sink = self.current.call_method0(py, "get")?;
        if !sink.is_none(py) { sink.call_method1(py, "display", (event,))?; }
        Ok(())
    }
}

fn context_var(py: Python<'_>) -> PyResult<Py<PyAny>> { Ok(py.import("kernmini._bridge")?.getattr("_current")?.unbind()) }

fn shared_event_loop(py: Python<'_>) -> PyResult<Py<PyAny>> { Ok(py.import("loopmini")?.getattr("new_event_loop")?.call0()?.unbind()) }

#[pyfunction]
fn new_event_loop(py: Python<'_>) -> PyResult<Py<PyAny>> { shared_event_loop(py) }

struct ChildLoop { event_loop: Mutex<Option<Py<PyAny>>>, thread: Mutex<Option<JoinHandle<()>>> }

impl ChildLoop {
    fn new() -> Self { Self { event_loop: Mutex::new(None), thread: Mutex::new(None) } }

    fn stop(&self) {
        let Some(event_loop) = self.event_loop.lock().expect("child loop lock poisoned").take() else { return };
        Python::attach(|py| { if let Ok(stop) = event_loop.getattr(py, "stop") { let _ = event_loop.call_method1(py, "call_soon_threadsafe", (stop,)); } });
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        self.stop();
        let thread = self.thread.lock().expect("child thread lock poisoned").take();
        if let Some(thread) = thread {
            let joined = tokio::task::spawn_blocking(move || thread.join().is_ok()).await?;
            if !joined { anyhow::bail!("child Python session panicked during shutdown") }
        }
        Ok(())
    }
}

impl Drop for ChildLoop {
    fn drop(&mut self) {
        self.stop();
        if let Some(thread) = self.thread.lock().expect("child thread lock poisoned").take()
            && thread.thread().id() != std::thread::current().id()
        { std::thread::spawn(move || { let _ = thread.join(); }); }
    }
}

struct PyLanguageSession {
    target: Py<PyAny>,
    current: Py<PyAny>,
    locals: pyo3_async_runtimes::TaskLocals,
    _child_loop: Option<Arc<ChildLoop>>,
}

impl Clone for PyLanguageSession {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            target: self.target.clone_ref(py),
            current: self.current.clone_ref(py),
            locals: self.locals.clone(),
            _child_loop: self._child_loop.clone(),
        })
    }
}

impl PyLanguageSession {
    fn new(py: Python<'_>, target: Py<PyAny>) -> PyResult<Self> {
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
        Self::with_locals(py, target, locals, None)
    }

    fn with_locals(py: Python<'_>, target: Py<PyAny>, locals: pyo3_async_runtimes::TaskLocals, child_loop: Option<Arc<ChildLoop>>) -> PyResult<Self> {
        let current = context_var(py)?;
        if target.bind(py).hasattr("set_stream_sender")? {
            let sender = Py::new(py, StreamRouter { current: current.clone_ref(py) })?;
            target.call_method1(py, "set_stream_sender", (sender,))?;
        }
        if target.bind(py).hasattr("set_display_sender")? {
            let sender = Py::new(py, DisplayRouter { current: current.clone_ref(py) })?;
            target.call_method1(py, "set_display_sender", (sender,))?;
        }
        if target.bind(py).hasattr("set_input_sender")? {
            let sender = Py::new(py, InputRouter { current: current.clone_ref(py) })?;
            target.call_method1(py, "set_input_sender", (sender,))?;
        }
        if target.bind(py).hasattr("bind_kernel")? {
            let kernel = py.import("kernmini._bridge")?.call_method1("kernel_proxy", (target.clone_ref(py),))?;
            target.call_method1(py, "bind_kernel", (kernel,))?;
        }
        Ok(Self { target, current, locals, _child_loop: child_loop })
    }

    async fn request(&self, method: &str, content: Value) -> anyhow::Result<Value> {
        let target = Python::attach(|py| self.target.clone_ref(py));
        let locals = self.locals.clone();
        let future = Python::attach(|py| -> PyResult<_> {
            let bridge = py.import("kernmini._bridge")?;
            let awaitable = bridge.call_method1("request_async", (target, method, json_to_py(py, &content)?))?;
            pyo3_async_runtimes::into_future_with_locals(&locals, awaitable)
        })
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let result = future.await.map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Python::attach(|py| py_to_json(py, result.bind(py))).map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

struct PyLanguage { factory: Py<PyAny>, parent: PyLanguageSession }

#[async_trait]
impl Language for PyLanguage {
    type Session = PyLanguageSession;

    fn parent(&self) -> Self::Session { self.parent.clone() }

    fn supports_children(&self) -> bool { true }

    async fn create_child(&self) -> anyhow::Result<Self::Session> {
        let factory = Python::attach(|py| self.factory.clone_ref(py));
        let child_loop = Arc::new(ChildLoop::new());
        let thread_loop = child_loop.clone();
        let (created, result) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let mut created = Some(created);
            let outcome = Python::attach(|py| -> PyResult<()> {
                let asyncio = py.import("asyncio")?;
                let event_loop = shared_event_loop(py)?;
                asyncio.call_method1("set_event_loop", (&event_loop,))?;
                *thread_loop.event_loop.lock().expect("child loop lock poisoned") = Some(event_loop.clone_ref(py));
                let target = factory.call0(py)?;
                let locals = pyo3_async_runtimes::TaskLocals::new(event_loop.bind(py).clone()).copy_context(py)?;
                let session = PyLanguageSession::with_locals(py, target, locals, Some(thread_loop.clone()))?;
                if created.take().unwrap().send(Ok(session)).is_err() { return Ok(()); }
                drop(thread_loop);
                event_loop.call_method0(py, "run_forever")?;
                Ok(())
            });
            if let Err(error) = outcome
                && let Some(created) = created.take()
            { let _ = created.send(Err(error.to_string())); }
        });
        *child_loop.thread.lock().expect("child thread lock poisoned") = Some(thread);
        result.await.map_err(|_| anyhow::anyhow!("child Python session ended during startup"))?.map_err(anyhow::Error::msg)
    }
}

#[async_trait]
impl LanguageSession for PyLanguageSession {
    fn kernel_info(&self) -> anyhow::Result<KernelInfo> {
        Python::attach(|py| -> PyResult<KernelInfo> {
            let value = py_to_json(py, self.target.call_method0(py, "kernel_info")?.bind(py))?;
            Ok(KernelInfo {
                implementation: value["implementation"].as_str().unwrap_or("python").to_owned(),
                implementation_version: value["implementation_version"].as_str().unwrap_or("0").to_owned(),
                banner: value["banner"].as_str().unwrap_or("").to_owned(),
                language_info: value.get("language_info").cloned().unwrap_or_else(|| json!({})),
            })
        })
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    fn execution_count(&self) -> u64 { Python::attach(|py| self.target.getattr(py, "execution_count").and_then(|value| value.extract(py))).unwrap_or(0) }

    fn supports_debugger(&self) -> bool { Python::attach(|py| self.target.bind(py).hasattr("debug_request")).unwrap_or(false) }

    fn set_debug_sender(&self, sender: DebugEventSender) -> anyhow::Result<()> {
        Python::attach(|py| -> PyResult<()> {
            if self.target.bind(py).hasattr("debugger")? {
                self.target.getattr(py, "debugger")?.setattr(py, "event_callback", Py::new(py, DebugRouter { sender })?)?;
            }
            Ok(())
        })
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn execute(&self, request: ExecuteRequest, context: ExecutionContext) -> anyhow::Result<ExecuteOutcome> {
        let target = Python::attach(|py| self.target.clone_ref(py));
        let current = Python::attach(|py| self.current.clone_ref(py));
        let locals = self.locals.clone();
        let interrupted = Arc::new(AtomicBool::new(false));
        let future = Python::attach(|py| -> PyResult<_> {
            let sink = Py::new(
                py,
                ExecutionSink {
                    context: context.clone(),
                    target: Some(target.clone_ref(py)),
                    locals: Some(locals.clone()),
                    interrupted: Some(interrupted.clone()),
                    wake_sync: self._child_loop.is_none(),
                },
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("silent", request.silent)?;
            kwargs.set_item("store_history", request.store_history)?;
            kwargs.set_item("user_expressions", json_to_py(py, &request.user_expressions)?)?;
            kwargs.set_item("allow_stdin", request.allow_stdin)?;
            let bridge = py.import("kernmini._bridge")?;
            let awaitable = bridge.call_method("execute", (target, current, sink, request.code), Some(&kwargs))?;
            pyo3_async_runtimes::into_future_with_locals(&locals, awaitable)
        })
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

        let result = future.await;
        let value = Python::attach(|py| -> PyResult<Value> { py_to_json(py, result?.bind(py)) }).map_err(|error| anyhow::anyhow!(error.to_string()))?;

        if let Some(streams) = value.get("streams").and_then(Value::as_array) {
            for stream in streams { context.stream(stream["name"].as_str().unwrap_or("stdout"), stream["text"].as_str().unwrap_or("")); }
        }
        if let Some(displays) = value.get("display").and_then(Value::as_array) { for display in displays { context.display(display.clone()) } }
        let mut error = value.get("error").filter(|error| !error.is_null()).map(|error| LanguageError {
            ename: error["ename"].as_str().unwrap_or("Error").to_owned(),
            evalue: error["evalue"].as_str().unwrap_or("").to_owned(),
            traceback: error["traceback"].as_array().map(|lines| lines.iter().filter_map(Value::as_str).map(str::to_owned).collect()).unwrap_or_default(),
        });
        if interrupted.load(AtomicOrdering::Acquire) && error.as_ref().is_some_and(|error| error.ename == "CancelledError") {
            error = Some(LanguageError { ename: "KeyboardInterrupt".into(), evalue: "".into(), traceback: vec![] });
        }
        Ok(ExecuteOutcome {
            execution_count: value["execution_count"].as_u64().unwrap_or(0),
            result: value.get("result").filter(|result| !result.is_null()).cloned(),
            result_metadata: value.get("result_metadata").cloned().unwrap_or_else(|| json!({})),
            error,
            user_expressions: value.get("user_expressions").cloned().unwrap_or_else(|| json!({})),
            payload: value.get("payload").cloned().unwrap_or_else(|| json!([])),
        })
    }

    async fn complete(&self, request: CompleteRequest) -> anyhow::Result<Value> {
        self.request("complete", json!({"code": request.code, "cursor_pos": request.cursor_pos})).await
    }

    async fn inspect(&self, request: InspectRequest) -> anyhow::Result<Value> {
        self.request("inspect", json!({"code": request.code, "cursor_pos": request.cursor_pos, "detail_level": request.detail_level})).await
    }

    async fn is_complete(&self, code: String) -> anyhow::Result<Value> { self.request("is_complete", json!({"code": code})).await }

    async fn history(&self, request: Value) -> anyhow::Result<Value> { self.request("history", request).await }

    async fn comm_info(&self, request: Value) -> anyhow::Result<Value> { self.request("comm_info", request).await }

    async fn debug(&self, request: Value) -> anyhow::Result<Value> {
        let target = Python::attach(|py| self.target.clone_ref(py));
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> PyResult<Value> {
                let request = json_to_py(py, &request)?;
                py_to_json(py, target.call_method1(py, "debug_request", (request,))?.bind(py))
            })
        })
        .await?
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn message(&self, message: LanguageMessage, context: ExecutionContext) -> anyhow::Result<()> {
        let target = Python::attach(|py| self.target.clone_ref(py));
        let current = Python::attach(|py| self.current.clone_ref(py));
        let locals = self.locals.clone();
        let future = Python::attach(|py| -> PyResult<_> {
            let sink = Py::new(py, ExecutionSink { context, target: None, locals: None, interrupted: None, wake_sync: false })?;
            let buffers = PyList::new(py, message.buffers.iter().map(|buffer| PyBytes::new(py, buffer)))?;
            let bridge = py.import("kernmini._bridge")?;
            let awaitable = bridge.call_method1("message", (target, current, sink, message.msg_type, json_to_py(py, &message.content)?, buffers))?;
            pyo3_async_runtimes::into_future_with_locals(&locals, awaitable)
        })
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        future.await.map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(())
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        if let Some(child_loop) = &self._child_loop { child_loop.shutdown().await? }
        Ok(())
    }
}

#[pyfunction]
#[pyo3(signature = (connection_file, factory, own_process_group=false))]
fn run_kernel<'py>(py: Python<'py>, connection_file: String, factory: Py<PyAny>, own_process_group: bool) -> PyResult<Bound<'py, PyAny>> {
    #[cfg(unix)]
    let owns_process_group = own_process_group
        && unsafe {
            let pid = libc::getpid();
            (libc::getpgrp() == pid || libc::setpgid(0, 0) == 0) && libc::getpgrp() == pid
        };
    #[cfg(not(unix))]
    let owns_process_group = false;
    #[cfg(unix)]
    let parent_pid = unsafe { libc::getppid() };
    let target = factory.call0(py)?;
    let interrupt = KernelInterrupter::default();
    let signal = py.import("signal")?;
    let signal_router = Py::new(py, SignalRouter { interrupt: interrupt.clone(), target: target.clone_ref(py) })?;
    signal.call_method1("signal", (signal.getattr("SIGINT")?, signal_router))?;
    let language = PyLanguage { parent: PyLanguageSession::new(py, target)?, factory };
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        #[cfg(unix)]
        let result = if owns_process_group {
            tokio::select! {
                result = crate::run_kernel_with_interrupter(connection_file, language, interrupt) => result,
                _ = async {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        if unsafe { libc::getppid() } != parent_pid { break }
                    }
                } => Ok(()),
            }
        } else { crate::run_kernel_with_interrupter(connection_file, language, interrupt).await };
        #[cfg(not(unix))]
        let result = crate::run_kernel_with_interrupter(connection_file, language, interrupt).await;
        #[cfg(unix)]
        if owns_process_group {
            unsafe { libc::killpg(libc::getpid(), libc::SIGTERM); }
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
            unsafe { libc::killpg(libc::getpid(), libc::SIGKILL); }
        }
        result.map_err(|error| PyRuntimeError::new_err(format!("{error:#}")))?;
        Ok(())
    })
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all().worker_threads(2);
    pyo3_async_runtimes::tokio::init(runtime);
    crate::python_dap::register(module)?;
    module.add_function(wrap_pyfunction!(new_event_loop, module)?)?;
    module.add_function(wrap_pyfunction!(run_kernel, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
