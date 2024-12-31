//! A simple work-stealing thread pool
//!
//! While this isn't as full-featured as rayon, it employs a simpler push-based scheduling strategy
//! that scales better than work stealing for the kinds of work done in the main FJ loop in
//! `core-relations`.

use std::{cell::Cell, sync::Mutex, thread};

use crossbeam_channel::{Receiver, Sender};
use waitgroup::WaitGroup;

#[cfg(test)]
mod tests;
mod waitgroup;

type BoundedWork<'a> = Box<dyn FnOnce() + Send + 'a>;
type Work = Box<dyn FnOnce() + Send>;

/// A simple thread pool for running work in parallel.
pub struct ThreadPool {
    sender: Sender<Work>,
    recvr: Receiver<Work>,
}

impl ThreadPool {
    pub fn new(n_threads: usize, on_exit: impl Fn() + Clone + Send + 'static) -> ThreadPool {
        let (sender, recvr) = crossbeam_channel::unbounded::<Work>();
        for _ in 0..n_threads {
            let on_exit = on_exit.clone();
            let recvr = recvr.clone();
            thread::spawn(move || {
                IN_THREAD.set(true);
                while let Ok(work) = recvr.recv() {
                    work();
                }
                on_exit();
            });
        }
        ThreadPool { sender, recvr }
    }

    /// Create a new scope in which work with a bounded lifetime can be spawned.
    ///
    /// This method will not return until all work spawned on the resulting [`Scope`] object has
    /// completed. Note that work is allowed to finish after `f` returns, so any borrowed resources
    /// must be initialized outside of the call.
    ///
    /// ```compile_fail
    /// let tp = ThreadPool::new(10, || {});
    /// tp.scope(|scope| {
    ///     scope.spawn(|scope| {
    ///         let x = 1;
    ///         scope.spawn(|scope| {
    ///             // `x` can be dropped before this work finishes.
    ///             let y = 2;
    ///             println!("{}", x + y);
    ///         });
    ///     });
    /// });
    /// ```
    pub fn scope<'a>(&'a self, f: impl FnOnce(&Scope<'a>) + Send + 'a) {
        let scope = Scope {
            wg: WaitGroup::new(),
            tp: self,
            _marker: std::marker::PhantomData,
        };
        f(&scope);
        if scope.wg.ready() {
            scope.wg.wait();
            return;
        }
        if IN_THREAD.get() {
            // Do more pending work before blocking.
            while let Ok(work) = scope.tp.recvr.try_recv() {
                work();
                if scope.wg.ready() {
                    break;
                }
            }
        }
        scope.wg.wait();
    }

    /// A simple wrapper around [`ThreadPool::scope`] that adds one instance of `f` to the thread
    /// pool for each item in `iter`.
    ///
    /// NB: This is much less sophisticated than what rayon parallel iterators do.
    pub fn for_each<'a, T: Send, I: IntoIterator<Item = T> + Send>(
        &'a self,
        iter: I,
        f: impl Fn(T) + Send + Sync + 'a,
    ) {
        let f = &f;
        self.scope(|scope| {
            for item in iter {
                scope.spawn(move |_| f(item));
            }
        });
    }
}

fn _test() {}

pub struct Scope<'outer> {
    wg: WaitGroup,
    tp: &'outer ThreadPool,
    _marker: std::marker::PhantomData<Mutex<&'outer mut ()>>,
}

impl<'outer> Scope<'outer> {
    pub fn spawn(&self, f: impl FnOnce(&Scope<'outer>) + Send + 'outer) {
        let wg = self.wg.clone();
        let tp = self.tp;
        let work: BoundedWork = Box::new(move || {
            let scope = Scope {
                wg,
                tp,
                _marker: std::marker::PhantomData,
            };
            f(&scope);
        });
        // SAFETY: `work` has a handle on `self.wg`, and `self.wg` is waited on in a context living
        // longer than `outer` (by the `scope` method).
        unsafe {
            self.tp.sender.send(remove_lifetime(work)).unwrap();
        }
    }
}

/// A utility for casting away any lifetime bounds from some work, allowing callers to move it to
/// the global queue.
///
/// Callers must ensure that `work` is executed before its lifetime expires.
unsafe fn remove_lifetime(work: BoundedWork) -> Work {
    std::mem::transmute::<BoundedWork, Work>(work)
}

thread_local! {
    static IN_THREAD: Cell<bool> = const { Cell::new(false) };
}
