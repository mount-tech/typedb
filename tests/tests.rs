extern crate kv_cab;

use kv_cab::{ KV, Value };

macro_rules! test_setup {
    ( $p:ident, $i:ident ) => {
        let _ = std::fs::remove_file($p);
        let mut $i = KV::<String, Value>::new($p).unwrap();
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

    assert!(KV::<String, Value>::new(test_cab_path).is_ok());

    test_teardown!(test_cab_path);
}

#[test]
fn test_create_wrong_type() {
    let test_cab_path ="./test_create_wrong_type.cab";
    let _ = std::fs::remove_file(test_cab_path);

    let mut test_store = KV::<String, Value>::new(test_cab_path).unwrap();
    let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
    assert_eq!(res, Ok(true));

    assert!(KV::<i32, Value>::new(test_cab_path).is_err());

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
        assert_eq!(test_store.get("key".to_string()).unwrap(), Some(Value::String("value".to_string())));
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
        assert_eq!(test_store.get("key".to_string()).unwrap(), Some(Value::Int(0i32)));
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
        assert_eq!(test_store.get("key".to_string()).unwrap(), Some(Value::Float(0f32)));
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_get_none() {
    let test_cab_path = "./test_get_none.cab";
    test_setup!(test_cab_path, test_store);

    assert_eq!(test_store.get("none".to_string()).unwrap(), None);

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

    assert_eq!(test_store.insert("key".to_string(), Value::String("value".to_string())), Ok(true));
    assert_eq!(test_store.insert("key2".to_string(), Value::String("value2".to_string())), Ok(true));

    assert!(test_store.get("key".to_string()).unwrap().is_some());
    assert!(test_store.get("key2".to_string()).unwrap().is_some());

    assert_eq!(test_store.keys().unwrap().len(), 2);
    assert_eq!(test_store.remove("key".to_string()), Ok(true));
    assert_eq!(test_store.keys().unwrap().len(), 1);
    assert_eq!(test_store.remove("key2".to_string()), Ok(true));
    assert_eq!(test_store.keys().unwrap().len(), 0);

    test_teardown!(test_cab_path);
}

#[test]
fn test_kv_all() {
    let test_cab_path = "./test_kv_all.cab";
    test_setup!(test_cab_path, test_store);

    assert_eq!(test_store.insert("key".to_string(), Value::String("value".to_string())), Ok(true));
    assert!(test_store.get("key".to_string()).unwrap().is_some());
    assert_eq!(test_store.remove("key".to_string()), Ok(true));

    test_teardown!(test_cab_path);
}

#[test]
fn test_multi_instance() {
    let test_cab_path = "./test_multi_instance.cab";
    let _ = std::fs::remove_file(test_cab_path);

    {
        let mut test_store = KV::<String, Value>::new(test_cab_path).unwrap();
        assert_eq!(test_store.insert("key".to_string(), Value::String("value".to_string())), Ok(true));
    }
    {
        let mut test_store = KV::<String, Value>::new(test_cab_path).unwrap();
        assert_eq!(test_store.get("key".to_string()).unwrap(),Some(Value::String("value".to_string())));
        assert_eq!(test_store.remove("key".to_string()), Ok(true));
    }

    test_teardown!(test_cab_path);
}

#[test]
fn test_multithread_instance() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    let t_1 = thread::spawn(|| {
        let mut test_store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();
        assert_eq!(test_store.insert("key".to_string(), Value::String("value".to_string())), Ok(true));
    });

    let t_2 = thread::spawn(|| {
        let mut test_store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();
        std::thread::sleep(std::time::Duration::new(1u64, 0u32));
        assert!(test_store.get("key".to_string()).unwrap().is_some());
        assert_eq!(test_store.remove("key".to_string()), Ok(true));
    });

    assert!(t_2.join().is_ok());
    assert!(t_1.join().is_ok());

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_instance_insert() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance_insert.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    let t_1 = thread::spawn(|| {
        let mut test_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();
        for i in 0..1000 {
            assert_eq!(test_store.insert(i, Value::Int(i)), Ok(true));
            assert!(test_store.get(i).unwrap().is_some());
        }
    });

    let t_2 = thread::spawn(|| {
        let mut test_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();
        for i in 1000..2000 {
            assert_eq!(test_store.insert(i, Value::Int(i)), Ok(true));
            assert!(test_store.get(i).unwrap().is_some());
        }
    });

    assert!(t_2.join().is_ok());
    assert!(t_1.join().is_ok());

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_many_instance_insert() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_many_instance_insert.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);
    let mut check_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();

    let t_1 = thread::spawn(|| {
        let mut test_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();
        for i in 0..1000 {
            assert_eq!(test_store.insert(i, Value::Int(i)), Ok(true));
            assert!(test_store.get(i).unwrap().is_some());
        }
    });

    let t_2 = thread::spawn(|| {
        let mut test_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();
        for i in 0..1000 {
            assert_eq!(test_store.insert(i, Value::Int(i)), Ok(true));
            assert!(test_store.get(i).unwrap().is_some());
        }
    });

    let t_3 = thread::spawn(|| {
        let mut test_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();
        for i in 1000..2000 {
            assert_eq!(test_store.insert(i, Value::Int(i)), Ok(true));
            assert!(test_store.get(i).unwrap().is_some());
        }
    });

    let t_4 = thread::spawn(|| {
        let mut test_store = KV::<i32, Value>::new(TEST_CAB_PATH).unwrap();
        for i in 1000..2000 {
            assert_eq!(test_store.insert(i, Value::Int(i)), Ok(true));
            assert!(test_store.get(i).unwrap().is_some());
        }
    });

    assert!(t_1.join().is_ok());
    assert!(t_2.join().is_ok());
    assert!(t_3.join().is_ok());
    assert!(t_4.join().is_ok());

    for i in 0..2000 {
        assert_eq!(check_store.get(i).unwrap(), Some(Value::Int(i)));
    }

    test_teardown!(TEST_CAB_PATH);
}

#[test]
fn test_multithread_instance_read_between() {
    const TEST_CAB_PATH: &'static str = "./test_multithread_instance_read_between.cab";
    let _ = std::fs::remove_file(TEST_CAB_PATH);

    let t_1 = thread::spawn(|| {
        let mut test_store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();
        assert_eq!(test_store.insert("key".to_string(), Value::String("value".to_string())), Ok(true));
    });

    let t_2 = thread::spawn(|| {
        let mut test_store = KV::<String, Value>::new(TEST_CAB_PATH).unwrap();
        std::thread::sleep(std::time::Duration::new(1u64, 0u32));
        assert_eq!(test_store.get("key".to_string()).unwrap().unwrap(), Value::String("value".to_string()));
        assert_eq!(test_store.remove("key".to_string()), Ok(true));
    });

    assert!(t_2.join().is_ok());
    assert!(t_1.join().is_ok());

    test_teardown!(TEST_CAB_PATH);
}
