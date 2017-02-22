kv_cab
========

[![crates.io version](https://img.shields.io/crates/v/kv_cab.svg)](https://crates.io/crates/kv_cab)

Simple persistent generic HashMap/Key-value store.

[Documentation](https://docs.rs/kv_cab)

Basic usage:
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

Usage with user defined Key and Value types:
```rust
extern crate kv_cab;
extern crate rustc_serialize;

use kv_cab::KV;

#[derive(Clone, RustcEncodable, RustcDecodable, PartialEq, Eq, Hash)]
enum MyKey {
    String(String),
    Int(i32),
}

#[derive(Clone, RustcEncodable, RustcDecodable, Debug)]
enum MyValue {
    String(String),
    Int(i32),
}

fn main() {
    let mut test_store = KV::<MyKey, MyValue>::new("./db.cab");

    let _ = test_store.insert(MyKey::Int(1i32), MyValue::String("value".to_string()));
    println!("{:?}", test_store.get(MyKey::Int(1i32)));
    let _ = test_store.remove(MyKey::Int(1i32));
}
```
