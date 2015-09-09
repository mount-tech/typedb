use std::collections::HashMap;
use std::cell::RefCell;
use std::thread;
use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;

pub struct KV {
    cab: RefCell<HashMap<&'static str, &'static str>> 
}

impl KV {
    /// create a new instance of the KV store
    pub fn new() -> KV {
        KV {
            cab: RefCell::new(HashMap::new())
        }
    }

    /// insert a key, value pair into the KV Store
    pub fn insert(&self, key:&'static str, value:&'static str) -> Result<bool, &str> {
        // insert into the HashMap
        {
            let mut m = self.cab.borrow_mut();
            m.insert(key, value);
        }

        // persist
        self.write_to_persist()
    }

    /// get a value from a key
    pub fn get(&self, key:&'static str) -> Option<&'static str> {
        let m = self.cab.borrow();
        match m.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None
        }
    }

    /// remove a key and associated value from the KV Store
    pub fn remove(&self, key:&'static str) -> Result<bool, &str> {
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
        
        let mut byte_vec = Vec::new();

        for (k, v) in m.iter() {
            let rec = format!("{}|{},", k, v);
            byte_vec.push_all(rec.as_bytes());
        }
        
        let data = Arc::new(byte_vec);

        let _ = thread::spawn(move || {
            let byte_slice = data.clone();
            
            // create the file
            let mut f = match File::create("db.cab") {
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
}

#[test]
fn test_create() {
    let _ = KV::new();
}

#[test]
fn test_insert() {
    let test_store = KV::new();

    let res = test_store.insert("key", "value");
    assert_eq!(res, Ok(true));
}

#[test]
fn test_get() {
    let test_store = KV::new();

    let res = test_store.insert("key", "value");
    assert_eq!(res, Ok(true));

    assert_eq!(test_store.get("key"), Some("value"));
}

#[test]
fn test_get_none() {
    let test_store = KV::new();

    assert_eq!(test_store.get("key"), None);
}

#[test]
fn test_remove() {
    let test_store = KV::new();
    
    let res = test_store.insert("key", "value");
    assert_eq!(res, Ok(true));
    
    let res = test_store.remove("key");
    assert_eq!(res, Ok(true));
}

#[test]
fn test_remove_none() {
    let test_store = KV::new();

    let res = test_store.remove("key");
    assert_eq!(res, Ok(true));
}

#[test]
fn test_kv_all() {
    let test_store = KV::new();
    let _ = test_store.insert("key", "value");
    test_store.get("key");
    let _ = test_store.remove("key");
}
