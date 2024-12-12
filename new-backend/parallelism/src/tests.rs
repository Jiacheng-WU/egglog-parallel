use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use crate::ThreadPoolHandle;

#[test]
fn two_way_nested() {
    let tp = ThreadPoolHandle::with_threads(20);
    let ran = AtomicUsize::new(0);
    tp.scope(|s1| {
        for _ in 0..1000 {
            s1.spawn(|| {
                let start = ran.load(SeqCst);
                tp.scope(|s2| {
                    for _ in 0..100 {
                        s2.spawn(|| {
                            ran.fetch_add(1, SeqCst);
                        })
                    }
                });
                assert!(ran.load(SeqCst) >= start + 100);
            });
        }
    });
    assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 100_000);
}
