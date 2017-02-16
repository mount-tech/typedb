kv_cab
========

Simple persistent HashMap

Usage:
```rust
let cab = KV::<Value>::new("./db.cab");
let _ = cab.insert("key".to_string(), Value::String("value".to_string()));
cab.get("key".to_string());
let _ = cab.remove("key".to_string())
```
