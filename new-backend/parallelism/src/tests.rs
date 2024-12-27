use std::{
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use concurrency::Notification;

use crate::{waitgroup::WaitGroup, ThreadPool};

#[test]
fn basic_nesting() {
    let tp = ThreadPool::new(40, || {});
    let items = [
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        [[11, 12, 13], [14, 15, 16], [17, 18, 19]],
    ];
    let expected: usize = items.iter().flatten().flatten().sum();
    let result = AtomicUsize::new(0);
    tp.for_each(items.iter(), |l1| {
        tp.for_each(l1.iter(), |l2| {
            tp.for_each(l2.iter(), |inner| {
                result.fetch_add(*inner, Ordering::AcqRel);
            })
        })
    });
    assert_eq!(expected, result.load(Ordering::Acquire));
}

#[test]
fn waitgroup_noop() {
    let wg = WaitGroup::new();
    assert!(wg.ready());
    wg.wait();
}

#[test]
fn waitgroup_serial() {
    let wg = WaitGroup::new();
    let h1 = wg.clone();
    let h2 = wg.clone();
    assert!(!wg.ready());
    mem::drop(h1);
    assert!(!wg.ready());
    mem::drop(h2);
    assert!(wg.ready());
    wg.wait();
}

#[test]
fn waitgroup_parallel() {
    let start = Arc::new(Notification::new());
    let wg = WaitGroup::new();
    let threads: Vec<_> = (0..20)
        .map(|_| {
            let h = wg.clone();
            let n = start.clone();
            thread::spawn(move || {
                n.wait();
                mem::drop(h);
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(100));
    assert!(!wg.ready());
    start.notify();
    wg.wait();
    for t in threads {
        t.join().unwrap();
    }
}
