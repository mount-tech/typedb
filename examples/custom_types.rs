extern crate typedb;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use typedb::KV;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum MyKey {
    String(String),
    Int(i32),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum MyValue {
    String(String),
    Int(i32),
}

fn main() {
    let cab_path = "./db.cab";
    let mut test_store = KV::<MyKey, MyValue>::new(cab_path).unwrap();

    let _ = test_store.insert(MyKey::Int(1i32), MyValue::String("value".to_string()));
    println!("{:?}", test_store.get(MyKey::Int(1i32)));
    let _ = test_store.remove(MyKey::Int(1i32));


    // clean up the cab
    let _ = std::fs::remove_file(cab_path);
}
