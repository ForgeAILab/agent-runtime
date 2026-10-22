//! Scoped benchmark observations. No journal data or process-global sink is retained.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Sample {
    pub id: u64,
    pub parent: Option<u64>,
    pub label: &'static str,
    pub nanos: u64,
    pub started_ns: u64,
    pub units: u64,
    pub bytes: u64,
}

#[derive(Debug, Default)]
struct Buffer {
    next: u64,
    origin: Option<Instant>,
    samples: Vec<Sample>,
    overflow: bool,
    events: BTreeMap<(Option<u64>, &'static str), Sample>,
}

/// Each observer owns its buffer; concurrent scopes never reset one another.
#[derive(Debug, Clone, Default)]
pub struct Observer(Arc<Mutex<Buffer>>);
#[derive(Debug, Clone)]
struct Context {
    observer: Observer,
    parent: Option<u64>,
}
thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
}
#[derive(Debug)]
struct Enter(Option<Context>);
impl Enter {
    fn new(context: Context) -> Self {
        Self(CURRENT.with(|current| current.replace(Some(context))))
    }
}
impl Drop for Enter {
    fn drop(&mut self) {
        CURRENT.with(|current| current.replace(self.0.take()));
    }
}
impl Observer {
    /// Installs context only while polling, including when the future changes threads.
    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        poll_context(
            Context {
                observer: self.clone(),
                parent: None,
            },
            future,
        )
        .await
    }
    /// Drain outside measured operations. Poisoning/overflow fails only the harness.
    pub fn drain(&self) -> Result<Vec<Sample>, &'static str> {
        let mut buffer = self.0.lock().map_err(|_| "observer buffer poisoned")?;
        if buffer.overflow {
            return Err("observer sample limit exceeded");
        }
        let mut samples = std::mem::take(&mut buffer.samples);
        samples.extend(std::mem::take(&mut buffer.events).into_values());
        Ok(samples)
    }
}
async fn poll_context<F: Future>(context: Context, future: F) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let _entered = Enter::new(context.clone());
        future.as_mut().poll(cx)
    })
    .await
}

/// A detached interval, used for a held gate without changing async ancestry.
#[derive(Debug)]
pub struct Timer(Option<(Observer, Sample, Instant)>);
pub fn timer(label: &'static str) -> Timer {
    let context = CURRENT.with(|current| current.borrow().clone());
    let Some(context) = context else {
        return Timer(None);
    };
    let Ok(mut buffer) = context.observer.0.lock() else {
        return Timer(None);
    };
    buffer.next += 1;
    let now = Instant::now();
    let origin = *buffer.origin.get_or_insert(now);
    let sample = Sample {
        id: buffer.next,
        parent: context.parent,
        label,
        nanos: 0,
        started_ns: now.duration_since(origin).as_nanos() as u64,
        units: 1,
        bytes: 0,
    };
    drop(buffer);
    Timer(Some((context.observer, sample, Instant::now())))
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((observer, mut sample, start)) = self.0.take() {
            sample.nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            if let Ok(mut buffer) = observer.0.lock() {
                if buffer.samples.len() < 2_000_000 {
                    buffer.samples.push(sample);
                } else {
                    buffer.overflow = true;
                }
            }
        }
    }
}
/// Counters aggregate within their exact parent span; units/bytes retain every event.
pub fn event(label: &'static str, units: u64, bytes: u64) {
    let context = CURRENT.with(|current| current.borrow().clone());
    let Some(context) = context else { return };
    if let Ok(mut buffer) = context.observer.0.lock() {
        let key = (context.parent, label);
        if let Some(sample) = buffer.events.get_mut(&key) {
            sample.units = sample.units.saturating_add(units);
            sample.bytes = sample.bytes.saturating_add(bytes);
        } else {
            buffer.next += 1;
            let id = buffer.next;
            let now = Instant::now();
            let origin = *buffer.origin.get_or_insert(now);
            buffer.events.insert(
                key,
                Sample {
                    id,
                    parent: context.parent,
                    label,
                    nanos: 0,
                    started_ns: now.duration_since(origin).as_nanos() as u64,
                    units,
                    bytes,
                },
            );
        }
    }
}
pub fn synchronous<T>(label: &'static str, action: impl FnOnce() -> T) -> T {
    let timer = timer(label);
    let _entered = timer.0.as_ref().map(|(observer, sample, _)| {
        Enter::new(Context {
            observer: observer.clone(),
            parent: Some(sample.id),
        })
    });
    action()
}
pub async fn instrument<F: Future>(label: &'static str, future: F) -> F::Output {
    let timer = timer(label);
    match timer.0.as_ref() {
        Some((observer, sample, _)) => {
            poll_context(
                Context {
                    observer: observer.clone(),
                    parent: Some(sample.id),
                },
                future,
            )
            .await
        }
        None => future.await,
    }
}

/// Captures the caller context before spawning a child task.
pub fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let context = CURRENT.with(|current| current.borrow().clone());
    async move {
        match context {
            Some(context) => poll_context(context, future).await,
            None => future.await,
        }
    }
}

#[derive(Debug)]
pub struct SyncTimer {
    _entered: Option<Enter>,
    _timer: Timer,
}
/// Synchronous functions may keep their ancestry until return; never hold across await.
pub fn enter_sync(label: &'static str) -> SyncTimer {
    let timer = timer(label);
    let entered = timer.0.as_ref().map(|(observer, sample, _)| {
        Enter::new(Context {
            observer: observer.clone(),
            parent: Some(sample.id),
        })
    });
    SyncTimer {
        _entered: entered,
        _timer: timer,
    }
}

/// v0.9.0 has no collection cursor; zero cursor charge is structural, not a heap estimate.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineState {
    pub memo_charge: usize,
    pub memo_budget: usize,
    pub memo_entries: usize,
    pub max_object_store_layers: usize,
    pub memo_closure_members: usize,
    pub external_proof_references: usize,
    pub live_leases: usize,
    pub local_dirty_sessions: usize,
    pub cursor_charge: usize,
}
