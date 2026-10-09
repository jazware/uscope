//! Connections a pool keeps open, spawned as hyper-util's client spawns
//! its own: each connection's future has its error mapped and its output
//! dropped with the futures crate's `map_err` and `map`, is boxed as a
//! trait object, and goes to tokio through an executor that is a trait
//! object too. Two connections are futures written by hand, as hyper's
//! are, and hold no async function. A third is an async function. Each
//! waits for a request that never comes. Once all three wait and the
//! workers are parked, the program reports its truth and stops at the
//! checkpoint `pooled`, then exits.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::{FutureExt, TryFutureExt};
use tokio::sync::oneshot;

/// A future the pool spawns.
type Boxed = Pin<Box<dyn Future<Output = ()> + Send>>;

/// What spawns the pool's futures, as hyper's `Executor` does.
trait Executor: Send + Sync {
    fn execute(&self, future: Boxed);
}

struct Spawner(tokio::runtime::Handle);

impl Executor for Spawner {
    fn execute(&self, future: Boxed) {
        self.0.spawn(future);
    }
}

/// Why a connection ended.
#[derive(Debug)]
struct Closed;

/// A connection written by hand: it waits for its next request.
struct Connection {
    requests: oneshot::Receiver<u32>,
    me: Option<u64>,
    at: Option<truth::At>,
}

impl Future for Connection {
    type Output = Result<u32, Closed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let me = *this.me.get_or_insert_with(truth::start);
        this.at.get_or_insert_with(|| truth::at(me, "request"));
        let Poll::Ready(request) = Pin::new(&mut this.requests).poll(cx) else {
            return Poll::Pending;
        };
        this.at = None;
        truth::end(me);
        Poll::Ready(request.map_err(|_| Closed))
    }
}

/// A connection written as an async function.
async fn connection(requests: oneshot::Receiver<u32>) -> Result<u32, Closed> {
    let me = truth::start();
    let request = {
        let _at = truth::at(me, "request");
        requests.await // AWAIT: request
    };
    truth::end(me);
    request.map_err(|_| Closed)
}

/// Spawns a connection's future the way the pool does.
fn spawn(executor: &dyn Executor, future: impl Future<Output = Result<u32, Closed>> + Send + 'static) {
    executor.execute(Box::pin(
        future
            .map_err(|closed| eprintln!("connection error: {closed:?}"))
            .map(|_| ()),
    ));
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("a runtime");
    let executor: Arc<dyn Executor> = Arc::new(Spawner(runtime.handle().clone()));
    let mut senders = Vec::new();
    for _ in 0..2 {
        let (sender, requests) = oneshot::channel();
        senders.push(sender);
        let written = Connection {
            requests,
            me: None,
            at: None,
        };
        spawn(executor.as_ref(), written);
    }
    let (sender, requests) = oneshot::channel();
    senders.push(sender);
    spawn(executor.as_ref(), connection(requests));
    while !(truth::parked_at("request") == 3 && truth::workers_parked(runtime.handle())) {
        std::thread::yield_now();
    }
    truth::checkpoint("pooled", Some(runtime.handle()));
    drop(senders);
    std::process::exit(0);
}
