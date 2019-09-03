extern crate typedb;

use typedb::{Value, KV};

macro_rules! test_setup {
    ($p:ident, $i:ident) => {
        let _ = std::fs::remove_file($p);
        #[allow(unused_mut)]
        let mut $i = KV::<String, Value>::new($p).unwrap();
    };
}

macro_rules! test_teardown {
    ($p:ident) => {
        use std::{thread, time};

        thread::sleep(time::Duration::from_secs(1));
        let _ = std::fs::remove_file($p);
    };
}

#[test]
fn test_create() {
    let test_cab_path = "./test_create.cab";
    let _ = std::fs::remove_file(test_cab_path);

    assert!(KV::<String, Value>::new(test_cab_path).is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_create_wrong_type() {
    let test_cab_path = "./test_create_wrong_type.cab";
    let _ = std::fs::remove_file(test_cab_path);

    let test_store = KV::<String, Value>::new(test_cab_path).unwrap();
    assert!(test_store
        .insert("key", &Value::String("value".to_string()))
        .is_ok());

    assert!(KV::<i32, Value>::new(test_cab_path).is_err());

    test_teardown!(test_cab_path);
}

#[test]
fn test_insert_string() {
    let test_cab_path = "./text_insert_string.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store
        .insert("key", &Value::String("value".to_string()))
        .is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_insert_int() {
    let test_cab_path = "./test_insert_int.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store.insert("key", &Value::Int(0i32)).is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_insert_float() {
    let test_cab_path = "./test_insert_float.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store.insert("key", &Value::Float(0f32)).is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_string() {
    let test_cab_path = "./test_get_string.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store
        .insert("key", &Value::String("value".to_string()))
        .is_ok());

    assert_eq!(
        test_store.get("key").unwrap(),
        Some(Value::String("value".to_string()))
    );

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_int() {
    let test_cab_path = "./test_get_int.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store.insert("key", &Value::Int(0i32)).is_ok());

    assert_eq!(test_store.get("key").unwrap(), Some(Value::Int(0i32)));

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_float() {
    let test_cab_path = "./test_get_float.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store.insert("key", &Value::Float(0f32)).is_ok());

    assert_eq!(test_store.get("key").unwrap(), Some(Value::Float(0f32)));

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_none() {
    let test_cab_path = "./test_get_none.cab";
    test_setup!(test_cab_path, test_store);

    assert_eq!(test_store.get("none").unwrap(), None);

    test_teardown!(test_cab_path);
}

#[test]
fn test_remove() {
    let test_cab_path = "./test_remove.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store
        .insert("key", &Value::String("value".to_string()))
        .is_ok());

    assert!(test_store.remove("key").is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_remove_none() {
    let test_cab_path = "./test_remove_none.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store.remove("key").is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_keys() {
    let test_cab_path = "./test_keys.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store
        .insert("key", &Value::String("value".to_string()))
        .is_ok());
    assert!(test_store
        .insert("key2", &Value::String("value2".to_string()))
        .is_ok());

    assert!(test_store.get("key").unwrap().is_some());
    assert!(test_store.get("key2").unwrap().is_some());

    assert_eq!(test_store.keys().unwrap().len(), 2);
    assert!(test_store.remove("key").is_ok());
    assert_eq!(test_store.keys().unwrap().len(), 1);
    assert!(test_store.remove("key2").is_ok());
    assert_eq!(test_store.keys().unwrap().len(), 0);

    test_teardown!(test_cab_path);
}

#[test]
fn test_kv_all() {
    let test_cab_path = "./test_kv_all.cab";
    test_setup!(test_cab_path, test_store);

    assert!(test_store
        .insert("key", &Value::String("value".to_string()))
        .is_ok());
    assert!(test_store.get("key").unwrap().is_some());
    assert!(test_store.remove("key").is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_multi_instance() {
    const TEST_CAB_PATH: &'static str = "./test_multi_instance.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    {
        let test_store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();
        assert!(test_store
            .insert("key", &Value::String("value".to_string()))
            .is_ok());
    }
    {
        let test_store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();
        assert_eq!(
            test_store.get("key").unwrap(),
            Some(Value::String("value".to_string()))
        );
        assert!(test_store.remove("key").is_ok());
    }

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_instance() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    {
        let store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();

        let test_store = store.clone();
        let t_1 = thread::spawn(move || {
            assert!(test_store
                .insert("key", &Value::String("value".to_string()))
                .is_ok());
        });

        let test_store = store.clone();
        let t_2 = thread::spawn(move || {
            std::thread::sleep(std::time::Duration::new(1u64, 0u32));
            assert!(test_store.get("key").unwrap().is_some());
            assert!(test_store.remove("key").is_ok());
        });

        assert!(t_1.join().is_ok());
        assert!(t_2.join().is_ok());
    }

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_instance_insert() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance_insert.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    {
        let store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();

        let test_store = store.clone();
        let t_1 = thread::spawn(move || {
            for i in 0..100 {
                assert!(test_store.insert(&i, &Value::Int(i)).is_ok());
                assert!(test_store.get(&i).unwrap().is_some());
            }
        });

        let test_store = store.clone();
        let t_2 = thread::spawn(move || {
            for i in 100..200 {
                assert!(test_store.insert(&i, &Value::Int(i)).is_ok());
                assert!(test_store.get(&i).unwrap().is_some());
            }
        });

        assert!(t_2.join().is_ok());
        assert!(t_1.join().is_ok());
    }

    test_teardown!(TEST_CAB_PATH);
}

#[test]
// fails spuriously with 'assertion failed: !load.is_free()?'
fn test_multithread_many_instance_insert() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_many_instance_insert.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);
    {
        let store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();

        let spawn_worker = |r| {
            let own_store = store.clone();
            thread::spawn(move || {
                for i in r {
                    assert!(own_store.insert(&i, &Value::Int(i)).is_ok());
                }
            })
        };
        let t_1 = spawn_worker(0..100);
        let t_2 = spawn_worker(0..100);
        let t_3 = spawn_worker(100..200);
        let t_4 = spawn_worker(100..200);

        assert!(t_1.join().is_ok());
        assert!(t_2.join().is_ok());
        assert!(t_3.join().is_ok());
        assert!(t_4.join().is_ok());

        let mut sorted_keys = store.keys().unwrap();
        sorted_keys.sort();
        println!("key_count:{}\nkeys:{:?}", sorted_keys.len(), sorted_keys);

        for i in 0..20 {
            assert_eq!(store.get(&i).unwrap(), Some(Value::Int(i)));
        }
    }

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_instance_read_between() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance_read_between.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    {
        let store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();

        let test_store = store.clone();
        let t_1 = thread::spawn(move || {
            assert!(test_store
                .insert("key", &Value::String("value".to_string()))
                .is_ok());
        });

        let test_store = store.clone();
        let t_2 = thread::spawn(move || {
            std::thread::sleep(std::time::Duration::new(1u64, 0u32));
            assert_eq!(
                test_store.get("key").unwrap().unwrap(),
                Value::String("value".to_string())
            );
            assert!(test_store.remove("key").is_ok());
        });

        assert!(t_1.join().is_ok());
        assert!(t_2.join().is_ok());
    }

    test_teardown!(TEST_CAB_PATH);
}
