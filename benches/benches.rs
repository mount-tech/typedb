
#![feature(test)]

extern crate test;
extern crate kv_cab;

use kv_cab::{ KV, Value };

#[cfg(test)]
mod benches {
    use super::*;
    use test::Bencher;

    macro_rules! bench_teardown {
        ( $p:ident ) => {
            use std::{thread, time};

            thread::sleep(time::Duration::from_secs(2));
            let _ = std::fs::remove_file($p);
        }
    }

    #[bench]
    fn bench_get_int(b: &mut Bencher) {
        let test_cab_path = "./bench_get_many.cab";
        let mut test_store = KV::<String, Value>::new(test_cab_path).unwrap();

        let _ = test_store.insert("test".to_string(), Value::Int(1));

        b.iter(|| {
            let _ = test_store.get("test".to_string());
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_insert_int(b: &mut Bencher) {
        let test_cab_path = "./bench_insert_many.cab";
        let mut test_store = KV::<String, Value>::new(test_cab_path).unwrap();

        b.iter(|| {
            let _ = test_store.insert("test".to_string(), Value::Int(1));
        });

        bench_teardown!(test_cab_path);
    }
}
