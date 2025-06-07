use futures::channel::mpsc;
use futures::StreamExt;
use netidx::{
    config::Config,
    pool::Pooled,
    publisher::{Publisher, PublisherBuilder, Val, Value},
    subscriber::{Dval, Event, SubId, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use pyo3::types::PyDict;
use pyo3::{call, prelude::*};
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
use std::{collections::HashMap, sync::Arc};
use std::{fmt::format, time::Duration};
use tokio::sync::Mutex;

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
        let tx = self.tx.clone();
        let subs = self.subs.clone();

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

    fn receiving<'py>(&self, py: Python<'py>, callback: PyObject) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let callback = callback.clone_ref(py);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut rx = rx.lock().await;
            while let Some(mut updates) = rx.next().await {
                for (id, ev) in updates.drain(..) {
                    if let Event::Update(v) = ev {
                        match v {
                            Value::F64(x) => {
                                Python::with_gil(|py| {
                                    if let Err(e) =
                                        callback.call(py, (format!("{:?}", id), x), None)
                                    {
                                        eprintln!("Error calling callback: {:?}", e);
                                    }
                                });
                            }
                            _ => {
                                eprintln!("unsupported: {:?}", v);
                            }
                        }
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
        let publisher = &self.publisher;
        let pubs = &self.pubs;

        let mut rust_updates: HashMap<String, Value> = HashMap::new();
        for (k, v) in updates.iter() {
            let key = k.extract::<String>().unwrap();
            // Convert Python value to netidx Value
            let val = if v.is_instance_of::<pyo3::types::PyFloat>() {
                Value::F64(v.extract::<f64>().unwrap())
            } else if v.is_instance_of::<pyo3::types::PyInt>() {
                Value::I64(v.extract::<i64>().unwrap())
            } else if v.is_instance_of::<pyo3::types::PyString>() {
                Value::String(v.extract::<String>().unwrap().into())
            } else if v.is_instance_of::<pyo3::types::PyBool>() {
                Value::Bool(v.extract::<bool>().unwrap())
            } else {
                panic!("Unsupported type for publishing: {:?}", v.get_type().name())
            };
            rust_updates.insert(key, val);
        }

        let publisher = Arc::clone(publisher);
        let pubs = Arc::clone(pubs);
        future_into_py(py, async move {
            let mut batch = publisher.lock().await.start_batch();
            for (k, v) in rust_updates {
                pubs.lock()
                    .await
                    .entry(k.clone())
                    .and_modify(|val| val.update(&mut batch, v.clone()))
                    .or_insert(publisher.lock().await.publish(k.into(), v).unwrap());
            }
            batch.commit(Some(Duration::from_secs(2))).await;
            Ok(())
        })
    }
}

// ----------------------------------------------------------------------------

#[pymodule]
fn pynetidx(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySubscriber>()?;
    m.add_class::<PyPublisher>()?;
    Ok(())
}
