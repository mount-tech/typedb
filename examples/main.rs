extern crate kv_cab;

use kv_cab::KV;

fn main() {
    let cab_path = "./db.cab";
    let test_store = KV::new(cab_path);
    let _ = test_store.insert("key".to_string(), "value".to_string());
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());

    {
        let test_store = KV::new(cab_path);
        let _ = test_store.insert("key".to_string(), "value".to_string());
    }
    {
        let test_store = KV::new(cab_path);
        let _ = test_store.remove("key".to_string());
    }

    let _ = std::fs::remove_file(cab_path);
}
