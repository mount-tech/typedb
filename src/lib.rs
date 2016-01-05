#![feature(convert)]

use std::collections::HashMap;
use std::cell::RefCell;
use std::thread;
use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;

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

        let _ = store.load_from_persist();

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

    /// Write the KV Store to file
    fn write_to_persist(&self) -> Result<bool, &str> {
        let m = self.cab.borrow();
        let path = self.path.clone();
        
        let mut byte_vec = Vec::new();

        for (k, v) in m.iter() {
            let rec = format!("{}|{},", k, v);
            byte_vec.extend_from_slice(rec.as_bytes());
        }
        
        let data = Arc::new(byte_vec);

        let _ = thread::spawn(move || {
            let byte_slice = data.clone();
            
            // create the file
            let mut f = match File::create(path) {
                Ok(f) => f,
                Err(_) => panic!("Couldn't create file"),
            };

            // write the bytes to it
            match f.write_all(byte_slice.as_slice()) {
                Ok(_) => (),
                Err(_) => panic!("Coun't write to file"),
            };

            let _ = f.flush();
        }).join();

        Ok(true)
    }

    /// Load from file
    fn load_from_persist(&self) -> Result<bool, &str> {
        let mut f = match File::open(self.path) {
            Ok(f) => f,
            Err(_) => return Err("Couldn't load from persistance"),
        };

        let mut file_string = String::new();
        let _ = f.read_to_string(&mut file_string);

        let _ = file_string.split(',').map(|s| {
            let local_s = s.to_string();
            let l_s: Vec<&str> = local_s.split('|').collect();

            let key = l_s[0].to_string();
            let value = l_s[1].to_string();

            let _ = self.insert(key, value); 
        });

        Ok(true)
    }
}

#[test]
fn test_create() {
    let _ = KV::new("./db.cab");
}

#[test]
fn test_insert() {
    let test_store = KV::new("./db.cab");

    let res = test_store.insert("key".to_string(), "value".to_string());
    assert_eq!(res, Ok(true));
}

#[test]
fn test_get() {
    let test_store = KV::new("./db.cab");

    let res = test_store.insert("key".to_string(), "value".to_string());
    assert_eq!(res, Ok(true));

    assert_eq!(test_store.get("key".to_string()), Some("value".to_string()));
}

#[test]
fn test_get_none() {
    let test_store = KV::new("./db.cab");

    assert_eq!(test_store.get("key".to_string()), None);
}

#[test]
fn test_remove() {
    let test_store = KV::new("./db.cab");
    
    let res = test_store.insert("key".to_string(), "value".to_string());
    assert_eq!(res, Ok(true));
    
    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));
}

#[test]
fn test_remove_none() {
    let test_store = KV::new("./db.cab");

    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));
}

#[test]
fn test_kv_all() {
    let test_store = KV::new("./db.cab");
    let _ = test_store.insert("key".to_string(), "value".to_string());
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());
}
