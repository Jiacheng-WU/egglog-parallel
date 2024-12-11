//! This crate provides a fairly simple work-stealing thread pool with support for nested fork/join
//! parallelism.
//!
//! It is a lot simpler than rayon, and is not as optimized for some advanced patterns that rayon
//! works well for. This crate aims to provide better CPU utilization when there are deeply nested
//! parallel scopes. It does this by occasionally spawning new threads rather than block a worker
//! thread waiting for other tasks to complete. Essentially everything else is less optimized than
//! rayon: in particular there is no `join` primitive for avoiding heap allocations, and there are
//! many scenarios in which work stealing in rayon will provide better performance than what is
//! provided here (particuilarly if the amount of work per task is smaller).
use std::{
    cell::Cell,
    marker::PhantomData,
    sync::{atomic::AtomicUsize, Arc},
};

use concurrency::WaitGroupBuilder;
use crossbeam::channel::{Receiver, Sender};

#[cfg(test)]
mod tests;

/// A handle on a thread pool that allows for executing work that must complete within `'scope`
pub struct Scope<'scope> {
    tp: ThreadPoolHandle,
    wg: Option<WaitGroupBuilder>,
    // We want this lifetime to be invariant.
    _marker: PhantomData<Cell<&'scope mut ()>>,
}

impl<'scope> Scope<'scope> {
    pub fn handle(&self) -> &ThreadPoolHandle {
        &self.tp
    }
    pub fn spawn(&self, f: impl FnOnce() + Send + 'scope) {
        if self.tp.num_threads() == 0 {
            f();
            return;
        }
        let guard = self.wg.as_ref().unwrap().add();
        let work: LifetimeWork = Box::new(move || {
            // move `wg` into the closure.
            let _guard = guard;
            f();
        });
        // SAFETY: We will guarantee that `f` will finish while `scope` remains active because we
        // call wg.wait() in `drop`.
        unsafe {
            self.tp
                .sender
                .send(WorkData {
                    f: ignore_lifetime(work),
                })
                .expect("unexpected thread pool disconnection");
        }
    }
}

struct WorkData {
    f: Work,
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        let wg = self.wg.take().unwrap().build();
        if wg.ready() {
            return;
        }
        if IN_THREAD.with(|in_thread| in_thread.get()) {
            let already_waiting = WAITING.with(|waiting| waiting.get());
            WAITING.with(|waiting| waiting.set(true));
            if already_waiting {
                // We area already waiting. We want to detach this thead from the thread pool and
                // spawn a new one to handle the rest of the work in our stead.
                BAIL_OUT.with(|bail_out| bail_out.set(true));
                self.tp.spawn_worker_thread();
            } else {
                // Pull some more work off of the thread pool while we are waiting.
                while let Ok(work) = self.tp.receiver.try_recv() {
                    (work.f)();
                    if BAIL_OUT.with(|bail_out| bail_out.get()) {
                        // this thread has been detached. No need to keep pulling off the queue.
                        break;
                    }
                    if wg.ready() {
                        break;
                    }
                }
                wg.wait();
                WAITING.with(|waiting| waiting.set(false));
                return;
            }
        }
        // If we aren't in a worker thread, just go ahead and wait.
        wg.wait()
    }
}

unsafe fn ignore_lifetime(lifetime_work: LifetimeWork) -> Work {
    std::mem::transmute::<LifetimeWork, Work>(lifetime_work)
}

type LifetimeWork<'a> = Box<dyn FnOnce() + Send + 'a>;

type Work = Box<dyn FnOnce() + Send>;

#[derive(Clone)]
pub struct ThreadPoolHandle {
    num_threads: usize,
    total_spawned: Arc<AtomicUsize>,
    sender: Sender<WorkData>,
    receiver: Receiver<WorkData>,
}

impl Default for ThreadPoolHandle {
    fn default() -> Self {
        Self::with_threads(num_cpus::get())
    }
}

impl ThreadPoolHandle {
    pub fn with_threads(num_threads: usize) -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded::<WorkData>();
        let res = Self {
            total_spawned: Arc::new(AtomicUsize::new(0)),
            num_threads,
            sender,
            receiver,
        };
        for _ in 0..num_threads {
            res.spawn_worker_thread();
        }
        res
    }
    /// The target number of threads active in this thread pool.
    pub fn num_threads(&self) -> usize {
        self.num_threads
    }

    fn spawn_worker_thread(&self) {
        self.total_spawned
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let recv = self.receiver.clone();
        std::thread::spawn(move || {
            IN_THREAD.with(|in_thread| in_thread.set(true));
            while let Ok(work) = recv.recv() {
                (work.f)();
                if BAIL_OUT.with(|bail_out| bail_out.get()) {
                    break;
                }
            }
        });
    }

    /// Create a scope for (potentially) nested fork/join parallelism executed on thsi thread pool.
    pub fn scope<'a>(&'a self, f: impl FnOnce(&Scope<'a>) + Send) {
        let scope = Scope {
            tp: self.clone(),
            wg: Some(WaitGroupBuilder::new()),
            _marker: PhantomData,
        };
        f(&scope);
    }
}

thread_local! {
    static IN_THREAD: Cell<bool> = const { Cell::new(false) };
    static WAITING: Cell<bool> = const { Cell::new(false) };
    static BAIL_OUT: Cell<bool> = const { Cell::new(false) };
}
