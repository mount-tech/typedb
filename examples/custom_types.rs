use serde_derive::{Deserialize, Serialize};
use typedb::{key, value, KV};

key!(
    enum MyKey {
        String(String),
        Int(i32),
    }
);

value!(
    enum MyValue {
        String(String),
        Int(i32),
    }
);

fn main() {
    let cab_path = "./db.cab";
    let test_store = KV::<MyKey, MyValue>::new(cab_path).unwrap();

    let _ = test_store.insert(&MyKey::Int(1i32), &MyValue::String("value".to_string()));
    println!("{:?}", test_store.get(&MyKey::Int(1i32)));
    let _ = test_store.remove(&MyKey::Int(1i32));

    // clean up the cab
    let _ = std::fs::remove_file(cab_path);
}
