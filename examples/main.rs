use typedb::{Value, KV};

fn main() {
    let cab_path = "./db.cab";
    let test_store = KV::<String, Value>::new(cab_path).unwrap();

    let _ = test_store.insert("key", &Value::String("value".to_string()));
    println!("{:?}", test_store.get("key").unwrap());
    let _ = test_store.remove("key");

    let _ = KV::<String, Value>::new(cab_path)
        .unwrap()
        .insert("key", &Value::String("value".to_string()));

    let _ = KV::<String, Value>::new(cab_path).unwrap().remove("key");

    let _ = std::fs::remove_file(cab_path);
}
