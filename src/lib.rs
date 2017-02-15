#![feature(test)]
extern crate test;

extern crate bincode;
extern crate rustc_serialize;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::thread;
use std::fs::File;
use std::io::prelude::*;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};

use rustc_serialize::{ Encodable, Decodable };

#[derive(Clone, RustcEncodable, RustcDecodable, PartialEq, Debug)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f32),
    Map(HashMap<String, Value>),
}

pub struct KV<V> {
    cab: HashMap<String, V>,
    path: &'static str,
}

impl<V: Clone + Encodable + Decodable> KV<V> {
    /// create a new instance of the KV store
    pub fn new(p:&'static str) -> KV<V> {
        let mut store = KV {
            cab: HashMap::new(),
            path: p,
        };

        match store.load_from_persist() {
            Ok(f) => trace!("{}", f),
            Err(e) => warn!("{}", e),
        };

        store
    }

    /// insert a key, value pair into the KV Store
    pub fn insert(&mut self, key: String, value: V) -> Result<bool, &str> {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // insert into the HashMap
        self.cab.insert(key, value);
        // persist
        self.write_to_persist()
    }

    /// get a value from a key
    pub fn get(&mut self, key: String) -> Option<V> {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // get the value from the cab
        match self.cab.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None
        }
    }

    /// remove a key and associated value from the KV Store
    pub fn remove(&mut self, key: String) -> Result<bool, &str> {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // remove from the HashMap
        self.cab.remove(&key);
        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&mut self) -> Vec<String> {
        // make sure mem version up to date
        let _ = self.load_from_persist();
        // create a vec from the cabs keys
        self.cab.keys().map(|k| k.clone()).collect()
    }

    /// Write the KV Store to file
    fn write_to_persist(&mut self) -> Result<bool, &str> {
        let path = self.path.clone();

        // encode the cab as a u8 vec
        let byte_vec: Vec<u8> = match encode(&mut self.cab, SizeLimit::Infinite) {
            Ok(bv) => bv,
            Err(e) => {
                warn!("{}", e);
                return Err("Could not encode cab");
            },
        };

        let write_thread = thread::spawn(move || {
            // create the file
            let mut f = File::create(path).unwrap();
            // write the bytes to it
            f.write_all(byte_vec.as_slice()).unwrap();
            let _ = f.flush();
        });

        // currently wait for thread to finish writing the file
        let _ = write_thread.join();

        Ok(true)
    }

    /// Load from file
    fn load_from_persist(&mut self) -> Result<bool, &str> {
        let mut f = match File::open(self.path) {
            Ok(f) => f,
            Err(e) => {
                warn!("{}", e);
                return Err("Couldn't open cab");
            },
        };

        let mut byte_vec = Vec::new();
        let _ = f.read_to_end(&mut byte_vec);

        // decode u8 vec back into HashMap
        let decoded: HashMap<String, V> = match decode(byte_vec.as_slice()) {
            Ok(f) => f,
            Err(e) => {
                warn!("{}", e);
                return Err("Couldn't decode cab");
            },
        }; 
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
        let mut test_store = KV::<Value>::new(test_cab_path);

        let _ = test_store.insert("test".to_string(), Value::Int(1));

        b.iter(|| {
            test_store.get("test".to_string());
        });

        bench_teardown!(test_cab_path);
    }

    #[bench]
    fn bench_insert_int(b: &mut Bencher) {
        let test_cab_path = "./bench_insert_many.cab";
        let mut test_store = KV::<Value>::new(test_cab_path);

        b.iter(|| {
            let _ = test_store.insert("test".to_string(), Value::Int(1));
        });

        bench_teardown!(test_cab_path);
    }
}
