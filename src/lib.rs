/*!
Simple persistent generic HashMap/Key-value store, using file locking to limit writing between threads.

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
extern crate fs2;

/// Macros for simplifying custom key and value types definition
pub mod macros;

use std::collections::HashMap;
use std::fs::{ File, OpenOptions };
use std::io::prelude::*;
use std::io::SeekFrom;
use std::hash::Hash;


use bincode::{serialize, deserialize, Infinite};
use serde::ser::Serialize;
use serde::de::Deserialize;


use fs2::FileExt;

/// The maximum number of retries the cab will make
const MAX_RETRIES:i32 = 10;

/// Definition of a macro for retrying an operation
macro_rules! retry {
    ($i:ident, $b:block) => (
        for $i in 0..MAX_RETRIES {
            if $i != 0 {
                std::thread::park_timeout(std::time::Duration::new(0u64, 1000u32));
            }

            $b
        }
    )
}

/// Macro for what to do on last retry
macro_rules! last_retry {
    ($i:ident, $b:stmt) => (
        if $i >= MAX_RETRIES - 1 {
            $b;
        }
    )
}

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
type KVResult = Result<bool, KVError>;

/// Errors that KV might have
#[derive(Debug, PartialEq)]
pub enum KVError {
    /// Could not deserialize the cab from disk
    CouldntDecode,
    /// Couldn't serialize the hashmap for writing to disk
    CouldntEncode,
    /// Could not write to the cab on disk
    CouldntWrite,
    /// Failed to read the cab from disk
    FailedToRead,
    /// Could not flush the data to disk
    CouldntFlush,
    /// Could not open the file from disk
    CouldntOpen,
    /// Could not load the file from disk
    CouldntLoad,
    /// Failed to write lock the cab
    CouldntWriteLock,
    /// Failed to read lock the cab
    CouldntReadLock,
    /// Failed to unlock the cab
    CouldntUnlock,
    /// Failed to set SeekFrom::Start
    CouldntSeekToStart,
}

/// The type that represents the key-value store
pub struct KV<K,V> {
    cab: HashMap<K, V>,
    file: File,
}

impl<K: Clone + Serialize + Deserialize + Eq + Hash, V: Clone + Serialize + Deserialize> KV<K,V> {
    /// Creates a new instance of the KV store
    pub fn new(p:&'static str) -> Result<KV<K,V>, KVError> {
        // create the KV instance
        let mut store = KV {
            cab: HashMap::new(),
            file: KV::<K, V>::retry_get_file(p).unwrap(),
        };

        match store.load_from_persist(false) {
            Ok(f) => trace!("{}", f),
            Err(e) => {
                return Err(e);
            },
        };

        Ok(store)
    }

    /// Retry to get a reference to the cab file at path p and create if doesn't exist
    fn retry_get_file(p:&'static str) -> Result<File, KVError> {
        retry!(i, {
            match OpenOptions::new().read(true).write(true).create(true).open(p) {
                Ok(f) => { return Ok(f); },
                Err(e) => {
                    error!("{}", e);
                    last_retry!(i, return Err(KVError::CouldntOpen));
                    continue;
                }
            };
        });

        Err(KVError::CouldntOpen)
    }

    /// Inserta a key, value pair into the key-value store
    pub fn insert(&mut self, key: K, value: V) -> KVResult {
        // check that can write to the cab
        if let Err(e) = self.file.lock_exclusive() {
            error!("{}", e);
            return Err(KVError::CouldntWriteLock);
        }
        // make sure mem version up to date
        if let Err(e) = self.load_from_persist(true) {
            return Err(e);
        }
        // insert into the HashMap
        self.cab.insert(key, value);
        // persist
        self.write_to_persist()
    }

    /// Get the value from a key
    pub fn get(&mut self, key: K) -> Result<Option<V>, KVError> {
        // make sure mem version up to date
        if let Err(e) = self.load_from_persist(false) {
            return Err(e);
        }
        // get the value from the cab
        match self.cab.get(&key) {
            Some(v) => Ok(Some((*v).clone())),
            None => Ok(None),
        }
    }

    /// Removes a key and associated value from the key-value Store
    pub fn remove(&mut self, key: K) -> KVResult {
        // check that can write to the cab
        if let Err(e) = self.file.lock_exclusive() {
            error!("{}", e);
            return Err(KVError::CouldntWriteLock);
        }
        // make sure mem version up to date
        if let Err(e) = self.load_from_persist(true) {
            return Err(e);
        }
        // remove from the HashMap
        self.cab.remove(&key);
        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&mut self) -> Result<Vec<K>, KVError> {
        // make sure mem version up to date
        if let Err(e) = self.load_from_persist(false) {
            return Err(e);
        }
        // create a vec from the cabs keys
        Ok(self.cab.keys().map(|k| k.clone()).collect())
    }

    /// Writes the key-value Store to file
    fn write_to_persist(&mut self) -> KVResult {
        // attempt to write to the cab
        retry!(i, {
            // check that can write to the cab
            if let Err(e) = self.file.lock_exclusive() {
                error!("{}", e);
                last_retry!(i, return Err(KVError::CouldntWriteLock));
                continue;
            }

            // serialize the cab as a u8 vec
            let byte_vec: Vec<u8> = match serialize(&mut self.cab, Infinite) {
                Ok(bv) => bv,
                Err(e) => {
                    error!("serialize: {}", e);
                    return Err(KVError::CouldntEncode);
                },
            };

            // seek to the start
            if let Err(e) = self.file.seek(SeekFrom::Start(0)) {
                error!("{}", e);
                last_retry!(i, return Err(KVError::CouldntSeekToStart));
                continue;
            }

            // write the bytes to it
            if let Err(e) = self.file.write_all(byte_vec.as_slice()) {
                error!("file.write_all/retry: {}", e);
                last_retry!(i, return Err(KVError::CouldntWrite));
                continue;
            }

            // flush to disk
            if let Err(e) = self.file.flush() {
                error!("{}", e);
                last_retry!(i, return Err(KVError::CouldntFlush));
                continue;
            }

            // unlock the file after write has completed
            if let Err(e) = self.file.unlock() {
                error!("{}", e);
                last_retry!(i, return Err(KVError::CouldntUnlock));
                continue;
            }
            return Ok(true);
        });

        Err(KVError::CouldntWrite)
    }

    /// Loads key-value store from file
    fn load_from_persist(&mut self, already_locked:bool) -> KVResult {
        retry!(i, {
            // byte vec to read into
            let mut byte_vec = Vec::new();

            // wait/lock the cab and read the bytes
            if !already_locked {
                if let Err(e) = self.file.lock_shared() {
                    error!("{}", e);
                    return Err(KVError::CouldntReadLock);
                }
            }

            // seek to the start
            if let Err(e) = self.file.seek(SeekFrom::Start(0)) {
                error!("{}", e);
                last_retry!(i, return Err(KVError::CouldntSeekToStart));
                continue;
            }

            // read the file into the buffer
            match self.file.read_to_end(&mut byte_vec) {
                Ok(count) => {
                    // don't attempt to deserialize as empty
                    if count == 0 {
                        if !already_locked {
                            if let Err(e) = self.file.unlock() {
                                error!("{}", e);
                                last_retry!(i, return Err(KVError::CouldntUnlock));
                            }
                        }
                        return Ok(true);
                    }
                },
                Err(e) => {
                    error!("{}", e);
                    return Err(KVError::FailedToRead);
                },
            }

            // deserialize u8 vec back into HashMap
            match deserialize(byte_vec.as_slice()) {
                Ok(f) => {
                    // assign read HashMap back to self
                    self.cab = f;
                    if !already_locked {
                        if let Err(e) = self.file.unlock() {
                            error!("{}", e);
                            last_retry!(i, return Err(KVError::CouldntUnlock));
                        }
                    }
                    return Ok(true);
                },
                Err(e) => {
                    error!("{}", e);
                    last_retry!(i, return Err(KVError::CouldntDecode));
                    continue;
                },
            };
        });

        Err(KVError::CouldntLoad)
    }
}
