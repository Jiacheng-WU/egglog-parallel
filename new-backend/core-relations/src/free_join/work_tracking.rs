//! Utilities for tracking the outstanding work enqueued on the rayon thread-pool
//!
//! Work-tracking is done cooperatively with the free join execution module and used to vary the
//! morsel size to balance efficiency with available parallelism.

use std::{
    cmp,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
};

use once_cell::sync::Lazy;

static TARGET: Lazy<isize> = Lazy::new(|| {
    (std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(1)
        * 10) as isize
});
static OUTSTANDING_WORK: AtomicIsize = AtomicIsize::new(0);

pub(super) fn enter_work() {
    OUTSTANDING_WORK.fetch_add(1, Ordering::Relaxed);
}

pub(super) fn exit_work() {
    OUTSTANDING_WORK.fetch_sub(1, Ordering::Relaxed);
}

pub(super) enum Direction {
    Increase,
    Same,
    Decrease,
}

pub(super) fn get_direction() -> Direction {
    let outstanding_work = OUTSTANDING_WORK.load(Ordering::Relaxed);
    if outstanding_work < *TARGET {
        Direction::Decrease
    } else if outstanding_work >= *TARGET && outstanding_work < (*TARGET as f64 * 1.5f64) as isize {
        Direction::Same
    } else {
        Direction::Increase
    }
}

#[derive(Clone)]
pub(super) struct MorselSize(Arc<AtomicUsize>);

impl MorselSize {
    pub(super) fn new(start: usize) -> MorselSize {
        MorselSize(Arc::new(AtomicUsize::new(start)))
    }

    pub(super) fn get(&self) -> usize {
        let gain = match get_direction() {
            Direction::Increase => 1.2f64,
            Direction::Same => 1.0f64,
            Direction::Decrease => 0.85f64,
        };
        let cur = self.0.load(Ordering::Relaxed);

        let mut next = (cur as f64 * gain) as usize;
        next = cmp::max(next, 8);
        next = cmp::min(next, 512);

        self.0.store(next, Ordering::Relaxed);
        next
    }
}
