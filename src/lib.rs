extern crate bincode;
extern crate rustc_serialize;

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
    path:&'static str,
}

impl<V: Clone + Encodable + Decodable> KV<V> {
    /// create a new instance of the KV store
    pub fn new(p:&'static str) -> KV<V> {
        let mut store = KV {
            cab: HashMap::new(),
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
    pub fn insert(&mut self, key: String, value: V) -> Result<bool, &str> {
        // insert into the HashMap
        self.cab.insert(key, value);
        // persist
        self.write_to_persist()
    }

    /// get a value from a key
    pub fn get(&mut self, key: String) -> Option<V> {
        match self.cab.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None
        }
    }

    /// remove a key and associated value from the KV Store
    pub fn remove(&mut self, key: String) -> Result<bool, &str> {
        // remove from the HashMap
        self.cab.remove(&key);
        // persist
        self.write_to_persist()
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&mut self) -> Vec<String> {
        self.cab.keys().map(|k| k.clone()).collect()
    }

    /// Write the KV Store to file
    fn write_to_persist(&mut self) -> Result<bool, &str> {
        let path = self.path.clone();
        
        let byte_vec: Vec<u8> = match encode(&mut self.cab, SizeLimit::Infinite) {
            Ok(bv) => bv,
            Err(e) => {
                print!("{}", e);
                return Err("Error: Could not write to persist");
            },
        };

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
    fn load_from_persist(&mut self) -> Result<bool, &str> {
        let mut f = match File::open(self.path) {
            Ok(f) => f,
            Err(_) => return Err("Couldn't open cab"),
        };

        let mut byte_vec = Vec::new();
        let _ = f.read_to_end(&mut byte_vec);

        let decoded: HashMap<String, V> = match decode(byte_vec.as_slice()) {
            Ok(f) => f,
            Err(e) => {
                println!("{}", e);
                return Err("Couldn't decode cab");
            },
        }; 
        self.cab = decoded;

        Ok(true)
    }
}

#[test]
fn test_create() {
    let test_cab_path ="./test_create.cab";
    let _ = KV::<Value>::new(test_cab_path);
   
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_insert_string() {
    let test_cab_path = "./test_insert.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
    assert_eq!(res, Ok(true));
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_insert_int() {
    let test_cab_path = "./test_insert.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.insert("key".to_string(), Value::Int(0i32));
    assert_eq!(res, Ok(true));
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_insert_float() {
    let test_cab_path = "./test_insert.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.insert("key".to_string(), Value::Float(0f32));
    assert_eq!(res, Ok(true));
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_string() {
    let test_cab_path = "./test_get.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::String("value".to_string())));
    }
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_int() {
    let test_cab_path = "./test_get.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::Int(0i32));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::Int(0i32)));
    }
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_float() {
    let test_cab_path = "./test_get.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    {
        let res = test_store.insert("key".to_string(), Value::Float(0f32));
        assert_eq!(res, Ok(true));
    }

    {
        assert_eq!(test_store.get("key".to_string()), Some(Value::Float(0f32)));
    }
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_get_none() {
    let test_cab_path = "./test_get_none.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    assert_eq!(test_store.get("none".to_string()), None);

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_remove() {
    let test_cab_path = "./test_remove.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);
    
    {
        let res = test_store.insert("key".to_string(), Value::String("value".to_string()));
        assert_eq!(res, Ok(true));
    }
    
    {
        let res = test_store.remove("key".to_string());
        assert_eq!(res, Ok(true));
    }
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_remove_none() {
    let test_cab_path = "./test_remove_none.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let res = test_store.remove("key".to_string());
    assert_eq!(res, Ok(true));
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_keys() {
    let test_cab_path = "./test_keys.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    let _ = test_store.insert("key2".to_string(), Value::String("value2".to_string()));

    assert!(test_store.keys().len() == 2);
    let _ = test_store.remove("key".to_string());
    assert!(test_store.keys().len() == 1);
    let _ = test_store.remove("key2".to_string());
    assert!(test_store.keys().len() == 0);
    
    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_kv_all() {
    let test_cab_path = "./test_kv_all.cab";
    let mut test_store = KV::<Value>::new(test_cab_path);

    let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    test_store.get("key".to_string());
    let _ = test_store.remove("key".to_string());

    let _ = std::fs::remove_file(test_cab_path);
}

#[test]
fn test_multi_instance() {
    let test_cab_path = "./test_multi_instance.cab";
    {
        let mut test_store = KV::<Value>::new(test_cab_path);
        let _ = test_store.insert("key".to_string(), Value::String("value".to_string()));
    }
    {
        let mut test_store = KV::<Value>::new(test_cab_path);
        assert!(test_store.get("key".to_string()) == Some(Value::String("value".to_string())));
        let _ = test_store.remove("key".to_string());
    }
    let _ = std::fs::remove_file(test_cab_path);
}
