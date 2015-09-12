#![feature(vec_push_all)]
#![feature(convert)]

mod db;

use db::KV;

fn main() {
    //TODO start the dbms
    let test_store = KV::new();
    let _ = test_store.insert("key".to_string(), "value".to_string());
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());
}

#[test]
fn test_main() {
    main();
}
