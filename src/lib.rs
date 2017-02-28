#![feature(test)]

extern crate test;
extern crate bincode;
extern crate rustc_serialize;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::io::prelude::*;
use std::hash::Hash;

use bincode::SizeLimit;
use bincode::rustc_serialize::{ encode, decode };

use rustc_serialize::{ Encodable, Decodable };

/// The maximum number of retries the cab will make
const MAX_RETRIES:i32 = 5;

/// A default value type to use with KV
#[derive(Clone, RustcEncodable, RustcDecodable, PartialEq, Debug)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f32),
    Map(HashMap<String, Value>),
}

/// Type alias for results from KV
type KVResult = Result<bool, &'static str>;

/// The type that represents the key-value store
pub struct KV<K,V> {
    cab: HashMap<K, V>,
    file: File,
}

impl<K: Clone + Encodable + Decodable + Eq + Hash, V: Clone + Encodable + Decodable> KV<K,V> {
    /// Creates a new instance of the KV store
    pub fn new(p:&'static str) -> KV<K,V> {
        // if the cab doesn't exist create it
        if !Path::new(p).exists() {
            for i in 0..MAX_RETRIES {
                match File::create(p) {
                    Ok(_) => break,
                    Err(e) => {
                        error!("{}", e);
                        if i >= MAX_RETRIES - 1 {
                            panic!("Could not create file after retries");
                        }
                        continue;
                    },
                }
            }
        }

        // Retry to get a reference to the cab file at path p
        fn retry_get_file(p:&'static str) -> File {
            for _ in 0..MAX_RETRIES {
                // make sure it is writeable on open
                let mut perms = match fs::metadata(p) {
                    Ok(f) => f.permissions(),
                    Err(e) => {
                        error!("{}", e);
                        continue;
                    },
                };
                perms.set_readonly(false);
                match fs::set_permissions(p, perms) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("{}", e);
                        continue;
                    }
                }

                match OpenOptions::new().read(true).write(true).open(p) {
                    Ok(f) => { return f; },
                    Err(e) => {
                        error!("{}", e);
                        continue;
                    }
                };
            }
            panic!("retry_get_file: Failed to get file");
        }

        // create the KV instance
        let mut store = KV {
            cab: HashMap::new(),
            file: retry_get_file(p),
        };

        // lock the cab for writes
        store.lock_cab(true);

        match store.load_from_persist() {
            Ok(f) => trace!("{}", f),
            Err(e) => {
                warn!("{}", e);
            },
        };

        store
    }

    /// Inserta a key, value pair into the key-value store
    pub fn insert(&mut self, key: K, value: V) -> KVResult {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // insert into the HashMap
        self.cab.insert(key, value);
        // persist
        self.write_to_persist()
    }

    /// Get the value from a key
    pub fn get(&mut self, key: K) -> Option<V> {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // get the value from the cab
        match self.cab.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None
        }
    }

    /// Removes a key and associated value from the key-value Store
    pub fn remove(&mut self, key: K) -> KVResult {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // remove from the HashMap
        self.cab.remove(&key);
        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&mut self) -> Vec<K> {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // create a vec from the cabs keys
        self.cab.keys().map(|k| k.clone()).collect()
    }

    /// Locks/unlocks cab for writing purposes
    fn lock_cab(&mut self, readonly:bool) {
        for i in 0..MAX_RETRIES {
            // set not readonly while writing
            let mut perms = match self.file.metadata() {
                Ok(f) => f.permissions(),
                Err(e) => {
                    error!("{}", e);
                    if i >= MAX_RETRIES - 1 {
                        panic!("Could not set permissions after retries");
                    }
                    continue;
                },
            };
            perms.set_readonly(readonly);


            match self.file.set_permissions(perms) {
                Ok(_) => {
                    let _ = self.file.sync_all();
                    break;
                },
                Err(e) => {
                    error!("{}", e);
                    if i >= MAX_RETRIES - 1 {
                        panic!("Could not set permissions after retries");
                    }
                    continue;
                },
            }
        }
    }

    /// Waits for the cab to become free
    fn wait_for_free(&mut self, lock:bool) -> KVResult {
        loop {
            // check if the cab is being written to
            let _ = self.file.sync_all();
            let metadata = match self.file.metadata() {
                Ok(m) => m,
                Err(_) => return Err("File doesn't exist or is not readeable"),
            };

            if metadata.permissions().readonly() {
                if lock {
                    self.lock_cab(false);
                }
                break;
            }
        }

        Ok(true)
    }

    /// Writes the key-value Store to file
    fn write_to_persist(&mut self) -> KVResult {
        // encode the cab as a u8 vec
        let byte_vec: Vec<u8> = match encode(&mut self.cab, SizeLimit::Infinite) {
            Ok(bv) => bv,
            Err(e) => {
                error!("encode: {}", e);
                return Err("Could not encode cab");
            },
        };

        if !self.wait_for_free(true).is_ok() {
            return Err("File doesn't exist or is not readeable");
        }
        // attempt to write to the cab
        for i in 0..MAX_RETRIES {
            // write the bytes to it
            match self.file.write_all(byte_vec.as_slice()) {
                Ok(_) => (),
                Err(e) => {
                    error!("file.write_all/retry: {}", e);
                    if i >= MAX_RETRIES - 1 {
                        panic!("Could not write to file after retries");
                    }
                    continue;
                },
            }

            // flush to disk
            let _ = self.file.flush();
            self.lock_cab(true);
            // leave the retry loop as successful
            break;
        }

        Ok(true)
    }

    /// Loads key-value store from file
    fn load_from_persist(&mut self) -> KVResult {
        // byte vec to read into
        let mut byte_vec = Vec::new();

        // wait/lock the cab and read the bytes
        if !self.wait_for_free(false).is_ok() {
            return Err("File doesn't exist or is not readeable");
        }
        let _ = self.file.read_to_end(&mut byte_vec);

        // decode u8 vec back into HashMap
        let decoded: HashMap<K, V> = match decode(byte_vec.as_slice()) {
            Ok(f) => f,
            Err(e) => {
                warn!("{}", e);
                return Err("Couldn't decode cab");
            },
        };
        // assign read HashMap back to self
        self.cab = decoded;

        Ok(true)
    }
}

#[cfg(test)]
mod benches {
    use super::*;
    use test::Bencher;

    macro_rules! bench_teardown {
        ( $p:ident ) => {
            use std::{thread, time};

            thread::sleep(time::Duration::from_secs(2));
            let _ = std::fs::remove_file($p);
        }
    }

    #[bench]
    fn bench_get_int(b: &mut Bencher) {
        let test_cab_path = "./bench_get_many.cab";
        let mut test_store = KV::<String, Value>::new(test_cab_path);

        let _ = test_store.insert("test".to_string(), Value::Int(1));

        b.iter(|| {
            test_store.get("test".to_string());
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_insert_int(b: &mut Bencher) {
        let test_cab_path = "./bench_insert_many.cab";
        let mut test_store = KV::<String, Value>::new(test_cab_path);

        b.iter(|| {
            let _ = test_store.insert("test".to_string(), Value::Int(1));
        });

        bench_teardown!(test_cab_path);
    }
}
