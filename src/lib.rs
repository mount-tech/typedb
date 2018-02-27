/*!
Simple persistent generic HashMap/Key-value store, using file locking to limit
writing between threads.

This is in a beta state at the moment.

Basic usage:

```
extern crate typedb;

use typedb::{ KV, Value };

fn main() {
    let mut test_store = KV::<String, Value>::new("./db.cab").unwrap();

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    println!("{:?}", test_store.get("key".to_string()));
    let _ = test_store.remove("key".to_string());
}
```

Usage with user defined Key and Value types:

```
#[macro_use]
extern crate typedb;
#[macro_use]
extern crate serde_derive;

use typedb::KV;

key!(MyKey:
    String(String),
    Int(i32),
);

value!(MyValue:
    String(String),
    Int(i32),
);

fn main() {
    let mut test_store = KV::<MyKey, MyValue>::new("./db.cab").unwrap();

    let _ = test_store.insert(MyKey::Int(1i32), MyValue::String("value".to_string()));
    println!("{:?}", test_store.get(MyKey::Int(1i32)));
    let _ = test_store.remove(MyKey::Int(1i32));
}
```

*/

#![deny(missing_docs)]

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate persy;

/// Macros for simplifying custom key and value types definition
pub mod macros;

use std::collections::HashMap;
use std::hash::Hash;

use bincode::{serialize, deserialize};
use serde::ser::Serialize;
use serde::de::Deserialize;

use persy::{Persy, Config, PRes, PersyError};

/// A default value type to use with KV
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum Value {
    /// Cab default Value type for strings
    String(String),
    /// Cab default Value type for intergers
    Int(i32),
    /// Cab default Value type for floats
    Float(f32),
    /// Cab default Value type for a sub map
    Map(HashMap<String, Value>),
    /// Cab default Value type for list
    List(Vec<Value>),
}

/// Type alias for results from KV
type KVResult = PRes<bool>;

/// Type alias for PersyError
type KVError = PersyError;

/// The type that represents the key-value store
pub struct KV<K, V> {
    cab: HashMap<K, V>,
    persy: Persy,
}

impl<K, V> KV<K, V>
where
    K: Clone + Serialize + for<'kde> Deserialize<'kde> + Eq + Hash,
    V: Clone + Serialize + for<'vde> Deserialize<'vde>,
{
    /// Creates a new instance of the KV store
    pub fn new(p: &'static str) -> Result<KV<K, V>, PersyError> {
        // create the KV instance
        Persy::create(p)?;
        let persy = Persy::open(p, Config::new())?;

        let mut store = KV {
            cab: HashMap::new(),
            persy: persy,
        };

        match store.load_from_persist() {
            Ok(f) => trace!("{}", f),
            Err(e) => {
                return Err(e);
            }
        };

        Ok(store)
    }

    /// Inserta a key, value pair into the key-value store
    pub fn insert(&mut self, key: K, value: V) -> KVResult {
        // make sure mem version up to date
        self.load_from_persist()?;
        // insert into the HashMap
        self.cab.insert(key, value);
        // persist
        self.write_to_persist()
    }

    /// Get the value from a key
    pub fn get(&mut self, key: K) -> Result<Option<V>, KVError> {
        // make sure mem version up to date
        self.load_from_persist()?;
        // get the value from the cab
        match self.cab.get(&key) {
            Some(v) => Ok(Some((*v).clone())),
            None => Ok(None),
        }
    }

    /// Removes a key and associated value from the key-value Store
    pub fn remove(&mut self, key: K) -> KVResult {
        // make sure mem version up to date
        self.load_from_persist()?;
        // remove from the HashMap
        self.cab.remove(&key);
        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&mut self) -> Result<Vec<K>, KVError> {
        // make sure mem version up to date
        self.load_from_persist()?;
        // create a vec from the cabs keys
        Ok(self.cab.keys().map(|k| k.clone()).collect())
    }

    /// Writes the key-value Store to file
    fn write_to_persist(&mut self) -> KVResult {
        // attempt to write to the cab
        let mut tx = self.persy.begin()?;
        self.persy.create_segment(&mut tx, "tdb")?;

        // serialize the cab as a u8 vec
        let byte_vec: Vec<u8> = match serialize(&mut self.cab) {
            Ok(bv) => bv,
            Err(e) => {
                error!("serialize: {}", e);
                return Err(PersyError::Err("Couldn't encode".to_string()));
            }
        };

        self.persy.insert_record(&mut tx, "tdb", &byte_vec)?;

        let prepared = self.persy.prepare_commit(tx)?;
        self.persy.commit(prepared)?;

        Ok(true)
    }

    /// Loads key-value store from file
    fn load_from_persist(&mut self) -> KVResult {
        for rec in self.persy.scan_records("tdb")? {
            let byte_vec = rec.content;
            // deserialize u8 vec back into HashMap
            match deserialize(byte_vec.as_slice()) {
                Ok(f) => {
                    // assign read HashMap back to self
                    self.cab = f;
                }
                Err(e) => {
                    error!("{}", e);
                    continue;
                }
            };
        }

        Ok(true)
    }
}
