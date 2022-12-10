use std::time::Instant;

use rand::seq::SliceRandom;

use crate::HashMap;
use crate::HashtableLike;
const NUM: usize = 10000_0000;

// cargo test --package common-hashtable --lib --release -- b::test_hashmap --exact --nocapture 
// 4.3 1.2
// RUSTFLAGS="-C target-cpu=native" cargo test --package common-hashtable --lib --release -- b::test_hashmap --exact --nocapture
#[test]
fn test_hashmap() {
    let mut hashmap: HashMap<u64, u64> = HashMap::with_capacity(4);
    let mut values:Vec<u64> = (0..NUM as u64).collect();
    let mut rng = rand::thread_rng();
    // values.shuffle(&mut rng);
    
    let vs = values.clone();
    let mut t = Instant::now();
    unsafe {
        for key in vs.into_iter() {
            match hashmap.insert(key) {
                Ok(v) => {
                    v.write(key);
                }
                Err(v) => *v = key,
            }
        }
    }
    println!("insert elasped {:?} ms", t.elapsed().as_millis());

    let mut sum = 0;
    let vs = values.clone();
    
    let mut t = Instant::now();
    for key in vs.into_iter() {
        if let Some(v) = hashmap.get(&key) {
            sum += *v;
        }
    }
    println!(
        "find elasped {:?} ms, total: {}",
        t.elapsed().as_millis(),
        sum
    );
}


// cargo test --package common-hashtable --lib --release -- b::test_hashbrown --exact --nocapture
#[test]
fn test_hashbrown() {
    let mut hashmap: hashbrown::HashMap<u64, u64> = hashbrown::HashMap::with_capacity(4);
    let mut t = Instant::now();
    unsafe {
        for key in 0..NUM as u64 {
            hashmap.insert(key, key);
        }
    }
    println!("B insert elasped {:?} ms", t.elapsed().as_millis());

    let mut t = Instant::now();
    let mut sum = 0;
    for key in 0..NUM as u64 {
        if let Some(v) = hashmap.get(&key) {
            sum += *v;
        }
    }
    println!(
        "B find elasped {:?} ms, total: {}",
        t.elapsed().as_millis(),
        sum
    );
}
