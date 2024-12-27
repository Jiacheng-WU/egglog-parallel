//! A simple work-stealing thread pool
//!
//! While this isn't as full-featured as rayon, it employs a simpler push-based scheduling strategy
//! that scales better than work stealing for the kinds of work done in the main FJ loop in
//! `core-relations`.

use crossbeam_channel::{Receiver, Sender};

type Work = Box<dyn FnOnce() + Send>;

pub struct ThreadPool {
    sender: Sender<Work>,
    recvr: Receiver<Work>,
}
