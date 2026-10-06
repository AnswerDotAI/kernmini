use crate::python::py_error;
use kernmini::ThreadWorker;
use pyo3::prelude::*;

struct PythonState(Option<Py<PyAny>>);

impl Drop for PythonState {
    fn drop(&mut self) { Python::attach(|_| drop(self.0.take())); }
}

/// Async access to a synchronous interpreter created and used on one dedicated thread.
#[pyclass(name = "ThreadWorker", module = "kernmini._native")]
struct PyThreadWorker { worker: ThreadWorker<PythonState> }

#[pymethods]
impl PyThreadWorker {
    #[staticmethod]
    #[pyo3(signature = (factory, *, name="kernmini-interpreter", stack_size=None))]
    fn start<'py>(py: Python<'py>, factory: Py<PyAny>, name: &str, stack_size: Option<usize>) -> PyResult<Bound<'py, PyAny>> {
        let context = py.import("contextvars")?.call_method0("copy_context")?.unbind();
        let mut builder = std::thread::Builder::new().name(name.to_owned());
        if let Some(size) = stack_size { builder = builder.stack_size(size); }
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let worker = ThreadWorker::start(builder, move || {
                Python::attach(|py| context.call_method1(py, "run", (factory,)).map(|state| PythonState(Some(state))))
                    .map_err(kernmini::Error::adapter)
            }).await.map_err(py_error)?;
            Python::attach(|py| Py::new(py, Self { worker }))
        })
    }

    /// Call `callback(state)` on the worker, carrying the caller's contextvars for output routing.
    fn call<'py>(&self, py: Python<'py>, callback: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let context = py.import("contextvars")?.call_method0("copy_context")?.unbind();
        let worker = self.worker.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            worker.call(move |state| Python::attach(|py| context.call_method1(py, "run", (callback, state.0.as_ref().unwrap()))))
                .await.map_err(py_error)?
        })
    }

    /// Release the worker's interpreter reference on its thread and wait for completion.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let worker = self.worker.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { worker.shutdown().await.map_err(py_error) })
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> { module.add_class::<PyThreadWorker>() }
