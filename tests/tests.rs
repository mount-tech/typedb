extern crate kv_cab;

use kv_cab::{ KV, Value };

macro_rules! test_setup {
    ( $p:ident, $i:ident ) => {
        let _ = std::fs::remove_file($p);
        let mut $i = KV::<Value>::new($p);
    }
}

macro_rules! test_teardown {
    ( $p:ident ) => {
        use std::{thread, time};

        thread::sleep(time::Duration::from_secs(1));
        let _ = std::fs::remove_file($p);
    }
}

#[test]
fn test_create() {
    let test_cab_path ="./test_create.cab";

    let _ = std::fs::remove_file(test_cab_path);
    let _ = KV::<Value>::new(test_cab_path);

    test_teardown!(test_cab_path);
}

#[test]
fn test_insert_string() {
    let test_cab_path = "./text_insert_string.cab";
    test_setup!(test_cab_path, test_store);

    let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
    assert_eq!(res, Ok(true));

    test_teardown!(test_cab_path);
}

#[test]
fn test_insert_int() {
    let test_cab_path = "./test_insert_int.cab";
    test_setup!(test_cab_path, test_store);

    let res = test_store.insert("key".to_string(), Value::Int(0i32));
    assert_eq!(res, Ok(true));

    test_teardown!(test_cab_path);
}

#[test]
fn test_insert_float() {
    let test_cab_path = "./test_insert_float.cab";
    test_setup!(test_cab_path, test_store);

    let res = test_store.insert("key".to_string(), Value::Float(0f32));
    assert_eq!(res, Ok(true));

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_string() {
    let test_cab_path = "./test_get_string.cab";
    test_setup!(test_cab_path, test_store);

    {
        let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::String("value".to_string())));
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_int() {
    let test_cab_path = "./test_get_int.cab";
    test_setup!(test_cab_path, test_store);

    {
        let res = test_store.insert("key".to_string(), Value::Int(0i32));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::Int(0i32)));
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_float() {
    let test_cab_path = "./test_get_float.cab";
    test_setup!(test_cab_path, test_store);

    {
        let res = test_store.insert("key".to_string(), Value::Float(0f32));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::Float(0f32)));
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_none() {
    let test_cab_path = "./test_get_none.cab";
    test_setup!(test_cab_path, test_store);

    assert_eq!(test_store.get("none".to_string()), None);

    test_teardown!(test_cab_path);
}

#[test]
fn test_remove() {
    let test_cab_path = "./test_remove.cab";
    test_setup!(test_cab_path, test_store);

    {
        let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(res, Ok(true));
    }

    {
        let res = test_store.remove("key".to_string());
        assert_eq!(res, Ok(true));
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_remove_none() {
    let test_cab_path = "./test_remove_none.cab";
    test_setup!(test_cab_path, test_store);

    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));

    test_teardown!(test_cab_path);
}

#[test]
fn test_keys() {
    let test_cab_path = "./test_keys.cab";
    test_setup!(test_cab_path, test_store);

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    let _ = test_store.insert("key2".to_string(), Value::String("value2".to_string()));

    assert!(test_store.keys().len() == 2);
    let _ = test_store.remove("key".to_string());
    assert!(test_store.keys().len() == 1);
    let _ = test_store.remove("key2".to_string());
    assert!(test_store.keys().len() == 0);

    test_teardown!(test_cab_path);
}

#[test]
fn test_kv_all() {
    let test_cab_path = "./test_kv_all.cab";
    test_setup!(test_cab_path, test_store);

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());

    test_teardown!(test_cab_path);
}

#[test]
fn test_multi_instance() {
    let test_cab_path = "./test_multi_instance.cab";
    {
        let mut test_store = KV::<Value>::new(test_cab_path);
        let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    }
    {
        let mut test_store = KV::<Value>::new(test_cab_path);
        assert!(test_store.get("key".to_string()) == Some(Value::String("value".to_string())));
        let _ = test_store.remove("key".to_string());
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_multithread_instance() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance.cab";

    let t_1 = thread::spawn(|| {
        let mut test_store = KV::<Value>::new(TEST_CAB_PATH);
        let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    });

    let t_2 = thread::spawn(|| {
        let mut test_store = KV::<Value>::new(TEST_CAB_PATH);
        let _ = test_store.get("key".to_string());
        let _ = test_store.remove("key".to_string());
    });

    assert!(t_2.join().is_ok());
    assert!(t_1.join().is_ok());

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_instance_insert() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance_insert.cab";

    let t_1 = thread::spawn(|| {
        let mut test_store = KV::<Value>::new(TEST_CAB_PATH);
        for i in 0..1000 {
            let _ = test_store.insert(format!("{}", i), Value::Int(i));
        }
    });

    let t_2 = thread::spawn(|| {
        let mut test_store = KV::<Value>::new(TEST_CAB_PATH);
        for i in 1000..2000 {
            let _ = test_store.insert(format!("{}", i), Value::Int(i));
        }
    });

    assert!(t_2.join().is_ok());
    assert!(t_1.join().is_ok());

    test_teardown!(TEST_CAB_PATH);
}
