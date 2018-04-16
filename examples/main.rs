extern crate typedb;

use typedb::{Value, KV};

fn main() {
    let cab_path = "./db.cab";
    let mut test_store = KV::<String, Value>::new(cab_path).unwrap();

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    println!("{:?}", test_store.get(&"key".to_string()).unwrap());
    let _ = test_store.remove(&"key".to_string());

    let _ = KV::<String, Value>::new(cab_path)
        .unwrap()
        .insert("key".to_string(), Value::String("value".to_string()));

    let _ = KV::<String, Value>::new(cab_path)
        .unwrap()
        .remove(&"key".to_string());

    let _ = std::fs::remove_file(cab_path);
}
