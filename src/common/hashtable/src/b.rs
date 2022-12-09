use std::time::Instant;

use crate::HashMap;
use crate::HashtableLike;
const NUM: usize = 10000_0000;

//  cargo test --package common-hashtable --lib --release -- b::test_hashmap --exact --nocapture
#[test]
fn test_hashmap() {
    let mut hashmap: HashMap<u64, u64> = HashMap::with_capacity(4);
    let mut t = Instant::now();
    unsafe {
        for key in 0..NUM as u64 {
            match hashmap.insert(key) {
                Ok(v) => {
                    v.write(key);
                }
                Err(v) => *v = key,
            }
        }
    }
    println!("insert elasped {:?} ms", t.elapsed().as_millis());

    let mut t = Instant::now();
    let mut sum = 0;
    for key in 0..NUM as u64 {
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
