/*!
Simple persistent generic HashMap/Key-value store, using file locking to limit
writing between threads.

This is in a beta state at the moment.

Basic usage:

```
use typedb::{KV, Value};

fn main() {
    let mut test_store = KV::<String, Value>::new("./basic.cab").unwrap();

    let _ = test_store.insert("key", &Value::String("value".to_string()));
    println!("{:?}", test_store.get("key"));
    let _ = test_store.remove("key");

#   let _ = std::fs::remove_file("./basic.cab");
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

    let _ = test_store.insert(&MyKey::Int(1i32), &MyValue::String("value".to_string()));
    println!("{:?}", test_store.get(&MyKey::Int(1i32)));
    let _ = test_store.remove(&MyKey::Int(1i32));

#   let _ = std::fs::remove_file("./types.cab");
}
```

Raw usage:

```
fn main() {
    let mut test_store = typedb::RawKV::<String, String>::new("./raw.cab", "runint").unwrap();

    let _ = test_store.insert("key".to_string(), "value".to_string());
    println!("{:?}", test_store.get(&"key".to_string()));
    let _ = test_store.remove("key".to_string());

#   let _ = std::fs::remove_file("./raw.cab");
}
```

*/

#![deny(missing_docs)]

extern crate bincode;
extern crate persy;
extern crate serde;
#[macro_use]
extern crate serde_derive;

/// Macros for simplifying custom key and value types definition
pub mod macros;

use std::{borrow::Borrow, borrow::Cow, collections::HashMap, hash::Hash, io, marker::PhantomData};

use bincode::{deserialize, serialize};
use persy::{ByteVec, Config, IndexType, PRes, Persy, PersyError};
use serde::{de::Deserialize, ser::Serialize};

/// A default value type to use with KV
#[allow(missing_docs)]
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f32),
    Map(HashMap<String, Value>),
    List(Vec<Value>),
}

/// The type that represents a raw key-value store
#[derive(Clone)]
pub struct RawKV<K, V>(Persy, Cow<'static, str>, PhantomData<(K, V)>);

impl<K, V> RawKV<K, V>
where
    K: IndexType,
    V: IndexType,
{
    /// Creates a new instance of the raw KV store
    pub fn new(p: &str, idxn: impl Into<Cow<'static, str>>) -> PRes<RawKV<K, V>> {
        Self::new_intern(p, idxn.into())
    }

    fn new_intern(p: &str, idxn: Cow<'static, str>) -> PRes<RawKV<K, V>> {
        // create the KV instance
        match Persy::create(p) {
            Ok(_) => {}
            Err(PersyError::Io(ref e)) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }

        let persy = Persy::open(p, Config::new())?;
        let ridxn = idxn.as_ref();

        if !persy.exists_index(ridxn)? {
            let mut tx = persy.begin()?;
            persy.create_index::<K, V>(&mut tx, ridxn, persy::ValueMode::REPLACE)?;
            let prepared = persy.prepare_commit(tx)?;
            persy.commit(prepared)?;
        }

        Ok(RawKV(persy, idxn, PhantomData))
    }

    /// Inserta a key, value pair into the key-value store
    pub fn insert(&self, key: K, value: V) -> PRes<()> {
        let mut tx = self.0.begin()?;
        self.0.put::<K, V>(&mut tx, self.1.as_ref(), key, value)?;
        let prepared = self.0.prepare_commit(tx)?;
        self.0.commit(prepared)
    }

    /// Get the value from a key
    pub fn get(&self, key: &K) -> PRes<Option<V>> {
        Ok(self
            .0
            .get::<K, V>(self.1.as_ref(), key)?
            .and_then(Self::flatten_value))
    }

    /// Removes a key and associated value from the key-value store
    pub fn remove(&self, key: K) -> PRes<()> {
        let mut tx = self.0.begin()?;
        self.0.remove::<K, V>(&mut tx, self.1.as_ref(), key, None)?;
        let prepared = self.0.prepare_commit(tx)?;
        self.0.commit(prepared)
    }

    /// Returns an iterator visiting all key-value pairs in arbitrary order
    pub fn iter(&self) -> PRes<impl Iterator<Item = (K, V)>> {
        Ok(self
            .0
            .range::<K, V, _>(self.1.as_ref(), ..)?
            .filter_map(|(k, v)| Self::flatten_value(v).map(|v2| (k, v2))))
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&self) -> PRes<impl Iterator<Item = K>> {
        Ok(self.iter()?.map(|(k, _)| k))
    }

    fn flatten_value(v: persy::Value<V>) -> Option<V> {
        use persy::Value;
        match v {
            Value::CLUSTER(x) => x.into_iter().next(),
            Value::SINGLE(x) => Some(x),
        }
    }
}

/// The type that represents the key-value store
#[derive(Clone)]
pub struct KV<K, V>(RawKV<ByteVec, ByteVec>, PhantomData<(K, V)>);

/// This function casts bincode errors into equivalent persy errors
fn map_encoderr(e: bincode::Error) -> PersyError {
    use bincode::ErrorKind;
    match *e {
        ErrorKind::Io(x) => x.into(),
        ErrorKind::InvalidUtf8Encoding(x) => x.into(),
        x => PersyError::Custom(Box::new(x)),
    }
}

fn encode_kv<T: Serialize>(x: &T) -> PRes<ByteVec> {
    Ok(ByteVec(serialize(x).map_err(map_encoderr)?))
}

fn decode_kv<'a, T: Deserialize<'a>>(x: &'a ByteVec) -> PRes<T> {
    Ok(deserialize(x.0.as_slice()).map_err(map_encoderr)?)
}

impl<K, V> KV<K, V>
where
    K: Clone + Serialize + for<'kde> Deserialize<'kde> + Eq + Hash,
    V: Clone + Serialize + for<'vde> Deserialize<'vde>,
{
    /// Creates a new instance of the KV store
    pub fn new(p: &str) -> PRes<KV<K, V>> {
        // create the KV instance
        match Persy::create(p) {
            Ok(o) => o,
            Err(PersyError::Io(ref e)) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }

        let persy = Persy::open(p, Config::new())?;

        // see issue #6: "On LOAD, check if file format is old or new, deserialize accordingly."
        if !persy.exists_index("tdbi")? {
            let mut cab = HashMap::<K, V>::new();
            let mut tx = persy.begin()?;

            if persy.exists_segment("tdb")? {
                // import data from previous storage format
                for (_, byte_vec) in persy.scan("tdb")? {
                    // deserialize u8 vec back into HashMap
                    cab = deserialize(byte_vec.as_slice()).map_err(map_encoderr)?;
                }
                persy.drop_segment(&mut tx, "tdb")?;
            }

            persy.create_index::<ByteVec, ByteVec>(&mut tx, "tdbi", persy::ValueMode::REPLACE)?;
            for (key, value) in &cab {
                persy.put::<ByteVec, ByteVec>(
                    &mut tx,
                    "tdbi",
                    encode_kv(&key)?,
                    encode_kv(&value)?,
                )?;
            }

            let prepared = persy.prepare_commit(tx)?;
            persy.commit(prepared)?;
        }

        Ok(KV(RawKV(persy, "tdbi".into(), PhantomData), PhantomData))
    }

    /// Inserta a key, value pair into the key-value store
    pub fn insert<Q>(&self, key: &Q, value: &V) -> PRes<()>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Serialize + ?Sized,
    {
        self.0.insert(encode_kv(&key)?, encode_kv(&value)?)
    }

    /// Get the value from a key
    pub fn get<Q>(&self, key: &Q) -> PRes<Option<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Serialize + ?Sized,
    {
        self.0
            .get(&encode_kv(&key)?)?
            .map(|v| decode_kv::<V>(&v))
            .map_or(Ok(None), |r| r.map(Some))
    }

    /// Removes a key and associated value from the key-value Store
    pub fn remove<Q>(&self, key: &Q) -> PRes<()>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Serialize + ?Sized,
    {
        self.0.remove(encode_kv(&key)?)
    }

    /// get all the keys contained in the KV Store
    pub fn keys(&self) -> PRes<Vec<K>> {
        self.0.keys()?.map(|k| decode_kv::<K>(&k)).collect()
    }
}
