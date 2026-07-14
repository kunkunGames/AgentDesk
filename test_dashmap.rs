use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use dashmap::DashMap;

fn main() {
    let map: DashMap<String, Arc<AtomicU64>> = DashMap::new();
    map.insert("test".to_string(), Arc::new(AtomicU64::new(0)));

    let kind = "test";

    // Original way
    let counter1 = map.entry(kind.to_string()).or_insert_with(|| Arc::new(AtomicU64::new(0))).clone();

    // Fast way
    let counter2 = if let Some(c) = map.get(kind) {
        c.clone() // Note: in DashMap, value() is not needed on Ref, it implements Deref
    } else {
        map.entry(kind.to_string()).or_insert_with(|| Arc::new(AtomicU64::new(0))).clone()
    };

    println!("c1: {}, c2: {}", counter1.load(Ordering::Relaxed), counter2.load(Ordering::Relaxed));
}
