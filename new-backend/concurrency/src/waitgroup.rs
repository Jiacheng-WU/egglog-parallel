//! A simple wait-group implementation that allows for non-blocking queries of whether or not a
//! call to `wait` will block.

use std::sync::Arc;

use crate::Notification;

#[derive(Default)]
struct NotifyOnDone(Arc<Notification>);

impl Drop for NotifyOnDone {
    fn drop(&mut self) {
        self.0.notify();
    }
}

/// A structure that can be used to assign a variable number of tasks that a corresponding
/// [`Waiter`] can block on to complete.
#[derive(Default)]
pub struct WaitGroupBuilder {
    notifier: Arc<NotifyOnDone>,
}

pub struct WaitGroupHandle {
    _guard: Arc<NotifyOnDone>,
}

impl WaitGroupBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new task to be waited on in the waitgroup. The resulting [`Waiter`] will not be ready
    /// until all handles returned by this method have been dropped.
    pub fn add(&self) -> WaitGroupHandle {
        WaitGroupHandle {
            _guard: self.notifier.clone(),
        }
    }

    /// Build the waitgroup. The resulting `Waiter` can be used to query whether all
    /// [`WaitGroupHandle`]s have been dropped.
    pub fn build(self) -> Waiter {
        Waiter {
            done: self.notifier.0.clone(),
        }
    }
}

/// A handle used to signal to a corresponding [`WaitGroup`] that a task has completed.
pub struct Waiter {
    done: Arc<Notification>,
}

impl Waiter {
    pub fn ready(&self) -> bool {
        self.done.has_been_notified()
    }
    pub fn wait(&self) {
        self.done.wait();
    }
}
