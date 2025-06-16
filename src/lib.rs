use futures::channel::mpsc;
use futures::StreamExt;
use netidx::{
    config::Config,
    pool::Pooled,
    publisher::{Publisher, PublisherBuilder, Val, Value},
    subscriber::{Dval, Event, SubId, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

macro_rules! py_callback {
    ($callback:expr, $id:expr, $val:expr) => {
        Python::with_gil(|py| {
            $callback
                .call(py, (format!("{:?}", $id), $val), None)
                .map_err(|e| PyErr::from(e))
        })?
    };
}

#[pyclass]
struct PySubscriber {
    subscriber: Arc<Mutex<Subscriber>>,
    subs: Arc<Mutex<HashMap<String, Dval>>>,
    tx: Arc<Mutex<mpsc::Sender<Pooled<Vec<(SubId, Event)>>>>>,
    rx: Arc<Mutex<mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>>>,
}

#[pymethods]
impl PySubscriber {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = get_runtime();
        let cfg = Config::load_default()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}")))?;
        let sub = runtime
            .block_on(async { SubscriberBuilder::new(cfg).build() })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}")))?;
        let (tx, rx) = mpsc::channel::<Pooled<Vec<(SubId, Event)>>>(1024);
        Ok(PySubscriber {
            subscriber: Arc::new(Mutex::new(sub)),
            subs: Arc::new(Mutex::new(HashMap::new())),
            tx: Arc::new(Mutex::new(tx)),
            rx: Arc::new(Mutex::new(rx)),
        })
    }

    fn subscribe<'py>(&self, py: Python<'py>, paths: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let subscriber = self.subscriber.clone();
        let subs = self.subs.clone();
        let tx = self.tx.clone();

        future_into_py(py, async move {
            let mut ret = HashMap::new();
            let sub = subscriber.lock().await;
            for p in paths {
                let dval = sub.subscribe(p.clone().into());
                ret.insert(p.clone(), format!("{:?}", dval.id()));
                dval.updates(UpdatesFlags::BEGIN_WITH_LAST, tx.lock().await.clone());
                subs.lock().await.insert(p.clone(), dval);
            }
            Ok(ret)
        })
    }

    fn receive<'py>(&self, py: Python<'py>, callback: PyObject) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let callback = callback.clone_ref(py);

        future_into_py(py, async move {
            let mut rx = rx.lock().await;
            while let Some(mut updates) = rx.next().await {
                for (id, ev) in updates.drain(..) {
                    if let Event::Update(v) = ev {
                        match v {
                            Value::F64(x) => py_callback!(callback, id, x),
                            Value::F32(x) => py_callback!(callback, id, x),
                            Value::I64(x) => py_callback!(callback, id, x),
                            Value::I32(x) => py_callback!(callback, id, x),
                            Value::U64(x) => py_callback!(callback, id, x),
                            Value::U32(x) => py_callback!(callback, id, x),
                            Value::Bool(x) => py_callback!(callback, id, x),
                            Value::String(ref x) => py_callback!(callback, id, x.to_string()),
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                    format!("Unsupported subscribe type"),
                                ));
                            }
                        };
                    }
                }
            }
            Ok(())
        })
    }
}

#[pyclass]
struct PyPublisher {
    publisher: Arc<Mutex<Publisher>>,
    pubs: Arc<Mutex<HashMap<String, Val>>>,
}

#[pymethods]
impl PyPublisher {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = get_runtime();
        let cfg = Config::load_default()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}")))?;
        let publisher = runtime
            .block_on(async { PublisherBuilder::new(cfg).build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}")))?;
        Ok(PyPublisher {
            publisher: Arc::new(Mutex::new(publisher)),
            pubs: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn publish<'py>(
        &self,
        py: Python<'py>,
        updates: Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let publisher = Arc::clone(&self.publisher);
        let pubs = Arc::clone(&self.pubs);

        let mut rust_updates: HashMap<String, Value> = HashMap::new();
        for (k, v) in updates.iter() {
            let key = k.extract::<String>()?;
            let val = if v.is_instance_of::<pyo3::types::PyFloat>() {
                Value::F64(v.extract::<f64>()?)
            } else if v.is_instance_of::<pyo3::types::PyInt>() {
                Value::I64(v.extract::<i64>()?)
            } else if v.is_instance_of::<pyo3::types::PyString>() {
                Value::String(v.extract::<String>()?.into())
            } else if v.is_instance_of::<pyo3::types::PyBool>() {
                Value::Bool(v.extract::<bool>()?)
            } else {
                Value::Error(format!("Unsupported publish type {}", v.get_type()).into())
            };
            rust_updates.insert(key, val);
        }

        future_into_py(py, async move {
            let mut batch = publisher.lock().await.start_batch();
            let mut pubs = pubs.lock().await;
            for (k, v) in rust_updates {
                if let Some(val) = pubs.get_mut(&k) {
                    val.update_changed(&mut batch, v.clone());
                } else {
                    let val = publisher
                        .lock()
                        .await
                        .publish(k.clone().into(), v)
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
                        })?;
                    pubs.insert(k, val);
                }
            }
            batch.commit(Some(Duration::from_secs(2))).await;
            Ok(())
        })
    }
}

#[pymodule]
fn pynetidx(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySubscriber>()?;
    m.add_class::<PyPublisher>()?;
    Ok(())
}
