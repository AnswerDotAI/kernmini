use crate::python::{json_to_py, py_to_json};
use crate::{DapClient, DapRequest};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

fn runtime_block_on<F, T>(py: Python<'_>, future: F) -> PyResult<T>
where
    F: Future<Output = anyhow::Result<T>> + Send,
    T: Send,
{ py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(future)).map_err(|error| PyRuntimeError::new_err(error.to_string())) }

fn timeout_duration(seconds: f64) -> PyResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 { return Err(PyRuntimeError::new_err("DAP timeout must be a non-negative finite number")); }
    Ok(Duration::from_secs_f64(seconds))
}

#[pyclass(name = "DapRequest")]
struct PyDapRequest { request: Mutex<Option<DapRequest>> }

impl PyDapRequest {
    fn wait(&self, py: Python<'_>, timeout: f64) -> PyResult<Py<PyAny>> {
        let request =
            self.request.lock().expect("DAP request lock poisoned").take().ok_or_else(|| PyRuntimeError::new_err("DAP request was already awaited"))?;
        let result = runtime_block_on(py, request.wait(timeout_duration(timeout)?))?;
        json_to_py(py, &result)
    }
}

#[pyclass(name = "DapClient")]
struct PyDapClient { client: Mutex<Option<DapClient>>, event_callback: Option<Py<PyAny>> }

impl PyDapClient {
    fn client(&self) -> PyResult<DapClient> {
        self.client.lock().expect("DAP client lock poisoned").clone().ok_or_else(|| PyRuntimeError::new_err("DAP client is not connected"))
    }
}

#[pymethods]
impl PyDapClient {
    #[new]
    #[pyo3(signature = (event_callback=None))]
    fn new(event_callback: Option<Py<PyAny>>) -> Self { Self { client: Mutex::new(None), event_callback } }

    fn connect(&self, py: Python<'_>, host: String, port: u16) -> PyResult<()> {
        let (client, mut events) = runtime_block_on(py, DapClient::connect((host, port)))?;
        if let Some(previous) = self.client.lock().expect("DAP client lock poisoned").replace(client) { previous.close(); }
        if let Some(callback) = self.event_callback.as_ref().map(|callback| callback.clone_ref(py)) {
            pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
                while let Some(event) = events.recv().await {
                    let result = Python::attach(|py| callback.call1(py, (json_to_py(py, &event)?,)).map(|_| ()));
                    if let Err(error) = result { eprintln!("DAP event callback failed: {error}"); }
                }
            });
        }
        Ok(())
    }

    fn close(&self) { if let Some(client) = self.client.lock().expect("DAP client lock poisoned").take() { client.close(); } }

    #[pyo3(signature = (request, timeout=10.0))]
    fn send_request(&self, py: Python<'_>, request: &Bound<'_, PyAny>, timeout: f64) -> PyResult<Py<PyAny>> {
        let request = py_to_json(py, request)?;
        let result = runtime_block_on(py, self.client()?.request(request, timeout_duration(timeout)?))?;
        json_to_py(py, &result)
    }

    fn send_request_async(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<(u64, Py<PyDapRequest>)> {
        let request = runtime_block_on(py, self.client()?.send(py_to_json(py, request)?))?;
        let seq = request.sequence();
        Ok((seq, Py::new(py, PyDapRequest { request: Mutex::new(Some(request)) })?))
    }

    #[pyo3(signature = (req_seq, waiter, timeout=10.0))]
    fn wait_for_response(&self, py: Python<'_>, req_seq: u64, waiter: PyRef<'_, PyDapRequest>, timeout: f64) -> PyResult<Py<PyAny>> {
        let sequence = waiter.request.lock().expect("DAP request lock poisoned").as_ref().map(DapRequest::sequence);
        if sequence != Some(req_seq) { return Err(PyRuntimeError::new_err("DAP request sequence does not match waiter")); }
        waiter.wait(py, timeout)
    }
}

impl Drop for PyDapClient { fn drop(&mut self) { if let Some(client) = self.client.get_mut().expect("DAP client lock poisoned").take() { client.close(); } } }

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDapClient>()?;
    module.add_class::<PyDapRequest>()?;
    Ok(())
}
