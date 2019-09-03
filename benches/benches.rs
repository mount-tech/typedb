#![feature(test)]

extern crate test;
extern crate typedb;

use typedb::{RawKV, Value, KV};

#[cfg(test)]
mod benches {
    use super::*;
    use test::Bencher;

    macro_rules! bench_teardown {
        ($p:ident) => {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let _ = std::fs::remove_file($p);
        };
    }

    #[bench]
    fn bench_get_int(b: &mut Bencher) {
        let test_cab_path = "./tmp/bench_get_many.cab";
        let test_store = KV::<String, Value>::new(test_cab_path).unwrap();

        let _ = test_store.insert("test", &Value::Int(1));

        b.iter(|| {
            let _ = test_store.get("test");
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_raw_get_int(b: &mut Bencher) {
        let test_cab_path = "./tmp/bench_raw_get.persy";
        let test_store = RawKV::<String, u32>::new(test_cab_path, "traw").unwrap();

        let _ = test_store.insert("test".to_string(), 1);
        let test_string = "test".to_string();

        b.iter(|| {
            let _ = test_store.get(&test_string);
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_insert_int(b: &mut Bencher) {
        let test_cab_path = "./tmp/bench_insert_many.cab";
        let test_store = KV::<String, Value>::new(test_cab_path).unwrap();

        b.iter(|| {
            let _ = test_store.insert("test", &Value::Int(1));
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_insert_get_int(b: &mut Bencher) {
        let test_cab_path = "./tmp/bench_insert_get_many.cab";
        let test_store = KV::<String, Value>::new(test_cab_path).unwrap();

        b.iter(|| {
            let _ = test_store.insert("test", &Value::Int(1));
            let _ = test_store.get("test");
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_raw_insert_get_int(b: &mut Bencher) {
        let test_cab_path = "./tmp/bench_raw_insert_get.persy";
        let test_store = RawKV::<String, u32>::new(test_cab_path, "traw").unwrap();
        let test_string = "test".to_string();

        b.iter(|| {
            let _ = test_store.insert("test".to_string(), 1);
            let _ = test_store.get(&test_string);
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_pure_insert_get_int(b: &mut Bencher) {
        let test_cab_path = "./tmp/bench_pure_insert_get.persy";
        let test_store = RawKV::<u32, u32>::new(test_cab_path, "traw").unwrap();

        b.iter(|| {
            let _ = test_store.insert(1, 1);
            let _ = test_store.get(&1);
        });

        bench_teardown!(test_cab_path);
    }
}
