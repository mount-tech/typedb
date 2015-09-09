#![feature(vec_push_all)]
#![feature(convert)]

mod db;

use db::KV;

fn main() {
    //TODO start the dbms
    let test_store = KV::new();
    let _ = test_store.insert("key", "value");
    test_store.get("key");
    let _ = test_store.remove("key");
}

#[test]
fn test_main() {
    main();
}
