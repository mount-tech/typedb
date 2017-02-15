extern crate kv_cab;

use kv_cab::{ KV, Value };

#[test]
fn test_create() {
    let test_cab_path ="./test_create.cab";
    let _ = KV::<Value>::new(test_cab_path);

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_insert_string() {
    let test_cab_path = "./test_insert.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
    assert_eq!(res, Ok(true));

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_insert_int() {
    let test_cab_path = "./test_insert.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.insert("key".to_string(), Value::Int(0i32));
    assert_eq!(res, Ok(true));

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_insert_float() {
    let test_cab_path = "./test_insert.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.insert("key".to_string(), Value::Float(0f32));
    assert_eq!(res, Ok(true));

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_string() {
    let test_cab_path = "./test_get.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::String("value".to_string())));
    }

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_int() {
    let test_cab_path = "./test_get.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::Int(0i32));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::Int(0i32)));
    }

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_float() {
    let test_cab_path = "./test_get.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::Float(0f32));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::Float(0f32)));
    }

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_none() {
    let test_cab_path = "./test_get_none.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    assert_eq!(test_store.get("none".to_string()), None);

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_remove() {
    let test_cab_path = "./test_remove.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(res, Ok(true));
    }

    {
        let res = test_store.remove("key".to_string());
        assert_eq!(res, Ok(true));
    }

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_remove_none() {
    let test_cab_path = "./test_remove_none.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_keys() {
    let test_cab_path = "./test_keys.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    let _ = test_store.insert("key2".to_string(), Value::String("value2".to_string()));

    assert!(test_store.keys().len() == 2);
    let _ = test_store.remove("key".to_string());
    assert!(test_store.keys().len() == 1);
    let _ = test_store.remove("key2".to_string());
    assert!(test_store.keys().len() == 0);

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_kv_all() {
    let test_cab_path = "./test_kv_all.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());

    let _ = std::fs::remove_file(test_cab_path);
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
    let _ = std::fs::remove_file(test_cab_path);
}
