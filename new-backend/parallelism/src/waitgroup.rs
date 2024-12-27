//! A simple wait-group implementation that allows for non-blocking queries of whether or not a
//! call to `wait` will block.

use std::{mem, sync::Arc};

use concurrency::Notification;

#[derive(Default)]
struct NotifyOnDone(Arc<Notification>);

impl Drop for NotifyOnDone {
    fn drop(&mut self) {
        self.0.notify();
    }
}

/// An object that allows for callers to wait for all copies of it to be destroyed.
#[derive(Default, Clone)]
pub struct WaitGroup {
    notifier: Arc<NotifyOnDone>,
}

impl WaitGroup {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether or not a call to [`WaitGroup::wait`] will block.
    pub fn ready(&self) -> bool {
        Arc::strong_count(&self.notifier) == 1
    }

    /// Block on all other clones of this waitgroup being destroyed.
    pub fn wait(self) {
        let notification = self.notifier.0.clone();
        mem::drop(self);
        notification.wait()
    }
}
