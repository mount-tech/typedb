kv_cab
========

Simple persistent HashMap/Key-value store.

Usage:
```rust
extern crate kv_cab;

use kv_cab::{ KV, Value };

fn main() {
    let mut test_store = KV::<String, Value>::new("./db.cab");

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    println!("{:?}", test_store.get("key".to_string()));
    let _ = test_store.remove("key".to_string());
}
```
