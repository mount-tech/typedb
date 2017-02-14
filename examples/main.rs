extern crate kv_cab;

use kv_cab::{ KV, Value };

fn main() {
    let cab_path = "./db.cab";
    let mut test_store = KV::<Value>::new(cab_path);

    {
        let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    }

    {
        println!("{:?}", test_store.get("key".to_string()));
    }
    
    {
        let _ = test_store.remove("key".to_string());
    }

    {
        let _ = KV::<Value>::new(cab_path).insert("key".to_string(), Value::String("value".to_string()));
    }

    {
        let _ = KV::<Value>::new(cab_path).remove("key".to_string());
    }

    let _ = std::fs::remove_file(cab_path);
}
