/*!
Simple persistent generic HashMap/Key-value store, using file locking to limit
writing between threads.

This is in a beta state at the moment.

Basic usage:

```
use typedb::{ KV, Value };

fn main() {
    let mut test_store = KV::<String, Value>::new("./basic.cab").unwrap();

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    println!("{:?}", test_store.get(&"key".to_string()));
    let _ = test_store.remove(&"key".to_string());
}
```

Usage with user defined Key and Value types:

```
use typedb::{KV, key, value};
use serde_derive::{ Serialize, Deserialize };

key!(
enum MyKey {
    String(String),
    Int(i32),
});

value!(
enum MyValue {
    String(String),
    Int(i32),
});

fn main() {
    let mut test_store = KV::<MyKey, MyValue>::new("./types.cab").unwrap();

    let _ = test_store.insert(MyKey::Int(1i32), MyValue::String("value".to_string()));
    println!("{:?}", test_store.get(&MyKey::Int(1i32)));
    let _ = test_store.remove(&MyKey::Int(1i32));
}
```

*/

#![deny(missing_docs)]

extern crate bincode;
#[macro_use]
extern crate log;
extern crate persy;
extern crate serde;
#[macro_use]
extern crate serde_derive;

/// Macros for simplifying custom key and value types definition
pub mod macros;

use std::collections::HashMap;
use std::hash::Hash;

use bincode::{deserialize, serialize};
use serde::de::Deserialize;
use serde::ser::Serialize;

use persy::{Config, Persy, PersyError};

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
type KVResult = Result<bool, PersyError>;

/// Type alias for PersyError
type KVError = PersyError;

const SEGMENT_NAME: &str = "tdb";

/// The type that represents the key-value store
pub struct KV<K, V> {
    cab: HashMap<K, V>,
    persy: Persy,
    id: Option<persy::PersyId>,
}

impl<K, V> KV<K, V>
where
    K: Clone + Serialize + for<'kde> Deserialize<'kde> + Eq + Hash,
    V: Clone + Serialize + for<'vde> Deserialize<'vde>,
{
    /// Creates a new instance of the KV store
    pub fn new(p: &'static str) -> Result<KV<K, V>, PersyError> {
        // create and open the persy instance
        match Persy::create(p) {
            Ok(o) => o,
            Err(PersyError::Io(ref e)) if e.to_string() == "File exists (os error 17)" => (),
            Err(e) => return Err(e),
        }
        let persy = Persy::open(p, Config::new())?;

        // if the segment doesn't exist create
        if !persy.exists_segment(SEGMENT_NAME)? {
            let mut tx = persy.begin()?;
            persy.create_segment(&mut tx, SEGMENT_NAME)?;
            let prepared = persy.prepare_commit(tx)?;
            persy.commit(prepared)?;
        }

        let mut store = KV {
            cab: HashMap::new(),
            persy,
            id: None,
        };

        store.load_from_persist()?;

        Ok(store)
    }

    /// Inserta a key, value pair into the key-value store
    pub fn insert(&mut self, key: K, value: V) -> KVResult {
        // insert into the HashMap
        self.cab.insert(key, value);
        // persist
        self.write_to_persist()
    }

    /// Get the value from a key
    pub fn get(&mut self, key: &K) -> Result<Option<V>, KVError> {
        // get the value from the cab
        Ok(self.cab.get(&key).map(|v| (*v).clone()))
    }

    /// Removes a key and associated value from the key-value Store
    pub fn remove(&mut self, key: &K) -> KVResult {
        // remove from the HashMap
        self.cab.remove(&key);
        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&mut self) -> Result<Vec<K>, KVError> {
        // create a vec from the cabs keys
        Ok(self.cab.keys().cloned().collect())
    }

    /// Writes the key-value Store to file
    fn write_to_persist(&mut self) -> KVResult {
        // attempt to write to the cab
        let mut tx = self.persy.begin()?;

        // serialize the cab as a u8 vec
        let byte_vec: Vec<u8> = match serialize(&self.cab) {
            Ok(bv) => bv,
            Err(e) => {
                error!("serialize: {}", e);
                return Err(PersyError::Custom(e));
            }
        };

        match &self.id {
            Some(ref x) => self
                .persy
                .update_record(&mut tx, SEGMENT_NAME, x, &byte_vec)?,
            None => self.id = Some(self.persy.insert_record(&mut tx, SEGMENT_NAME, &byte_vec)?),
        }

        let prepared = self.persy.prepare_commit(tx)?;
        self.persy.commit(prepared)?;

        Ok(true)
    }

    /// Loads key-value store from file
    fn load_from_persist(&mut self) -> KVResult {
        if self.persy.exists_segment(SEGMENT_NAME)? {
            let deserbv = |byte_vec: &Vec<u8>| {
                // deserialize u8 vec back into HashMap
                match deserialize(byte_vec.as_slice()) {
                    Ok(f) => {
                        // assign read HashMap back to self
                        Ok(f)
                    }
                    Err(e) => {
                        error!("{}", e);
                        Err(PersyError::Custom(e))
                    }
                }
            };
            if let Some(ref x) = &self.id {
                if let Some(byte_vec) = self.persy.read_record(SEGMENT_NAME, x)? {
                    self.cab = deserbv(&byte_vec)?;
                    return Ok(true);
                }
            }
            for (id, byte_vec) in self.persy.scan(SEGMENT_NAME)? {
                self.cab = deserbv(&byte_vec)?;
                self.id = Some(id);
            }
        }

        Ok(true)
    }
}
