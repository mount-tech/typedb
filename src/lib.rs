extern crate bincode;
extern crate rustc_serialize;

use std::collections::HashMap;
use std::cell::RefCell;
use std::thread;
use std::fs::File;
use std::io::prelude::*;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};

pub struct KV {
    cab: RefCell<HashMap<String, String>>,
    path:&'static str,
}

impl KV {
    /// create a new instance of the KV store
    pub fn new(p:&'static str) -> KV {
        let store = KV {
            cab: RefCell::new(HashMap::new()),
            path: p,
        };

        let _ = match store.load_from_persist() {
            Ok(f) => f,
            Err(e) => {
                println!("{}", e);
                File::create(p).is_ok()
            },
        };

        store
    }

    /// insert a key, value pair into the KV Store
    pub fn insert(&self, key: String, value: String) -> Result<bool, &str> {
        // insert into the HashMap
        {
            let mut m = self.cab.borrow_mut();
            m.insert(key, value);
        }

        // persist
        self.write_to_persist()
    }

    /// get a value from a key
    pub fn get(&self, key: String) -> Option<String> {
        let m = self.cab.borrow();
        match m.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None
        }
    }

    /// remove a key and associated value from the KV Store
    pub fn remove(&self, key: String) -> Result<bool, &str> {
        // remove from the HashMap
        {
            let mut m = self.cab.borrow_mut();
            m.remove(&key);
        }

        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&self) -> Vec<String> {
        let m = self.cab.borrow();
        
        let keys = m.keys().map(|k| k.clone()).collect();

        keys
    }

    /// Write the KV Store to file
    fn write_to_persist(&self) -> Result<bool, &str> {
        let m = self.cab.borrow_mut();
        let path = self.path.clone();
        
        let byte_vec: Vec<u8> = match encode(&*m, SizeLimit::Infinite) {
            Ok(bv) => bv,
            Err(e) => {
                print!("{}", e);
                return Err("Error: Could not write to persist");
            },
        };

        println!("{:?}\n", byte_vec);
        
        let _ = thread::spawn(move || {
            // create the file
            let mut f = match File::create(path) {
                Ok(f) => f,
                Err(_) => panic!("Couldn't create file"),
            };

            // write the bytes to it
            match f.write_all(byte_vec.as_slice()) {
                Ok(_) => (),
                Err(_) => panic!("Couldn't write to file"),
            };

            let _ = f.flush();
        }).join();

        Ok(true)
    }

    /// Load from file
    fn load_from_persist(&self) -> Result<bool, &str> {
        let mut f = match File::open(self.path) {
            Ok(f) => f,
            Err(_) => return Err("Couldn't open cab"),
        };

        let mut byte_vec = Vec::new();
        let _ = f.read_to_end(&mut byte_vec);

        println!("{:?}\n", byte_vec);

        let decoded: HashMap<String, String> = match decode(byte_vec.as_slice()) {
            Ok(f) => f,
            Err(e) => {
                println!("{}", e);
                return Err("Couldn't decode cab");
            },
        }; 
        *self.cab.borrow_mut() = decoded;

        Ok(true)
    }
}

#[test]
fn test_create() {
    let _ = KV::new("./test_create.cab");
}

#[test]
fn test_insert() {
    let test_store = KV::new("./test_insert.cab");

    let res = test_store.insert("key".to_string(), "value".to_string());
    assert_eq!(res, Ok(true));
}

#[test]
fn test_get() {
    let test_store = KV::new("./test_get.cab");

    let res = test_store.insert("key".to_string(), "value".to_string());
    assert_eq!(res, Ok(true));

    assert_eq!(test_store.get("key".to_string()), Some("value".to_string()));
}

#[test]
fn test_get_none() {
    let test_store = KV::new("./test_get_none.cab");

    assert_eq!(test_store.get("none".to_string()), None);
}

#[test]
fn test_remove() {
    let test_store = KV::new("./test_remove.cab");
    
    let res = test_store.insert("key".to_string(), "value".to_string());
    assert_eq!(res, Ok(true));
    
    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));
}

#[test]
fn test_remove_none() {
    let test_store = KV::new("./test_remove_none.cab");

    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));
}

#[test]
fn test_keys() {
    let test_store = KV::new("./test_keys.cab");

    let _ = test_store.insert("key".to_string(), "value".to_string());
    let _ = test_store.insert("key2".to_string(), "value2".to_string());

    assert!(test_store.keys().len() == 2);
    let _ = test_store.remove("key".to_string());
    assert!(test_store.keys().len() == 1);
    let _ = test_store.remove("key2".to_string());
    assert!(test_store.keys().len() == 0);
}

#[test]
fn test_kv_all() {
    let test_store = KV::new("./test_kv_all.cab");
    let _ = test_store.insert("key".to_string(), "value".to_string());
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());
}

#[test]
fn test_multi_instance() {
    {
        let test_store = KV::new("./test_multi_instance.cab");
        let _ = test_store.insert("key".to_string(), "value".to_string());
    }
    {
        let test_store = KV::new("./test_multi_instance.cab");
        println!("{:?}", test_store.get("key".to_string()));
        assert!(test_store.get("key".to_string()) == Some("value".to_string()));
        let _ = test_store.remove("key".to_string());
    }
}
