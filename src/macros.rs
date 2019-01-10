/// Macro for simplifying custom Key type definition
#[macro_export]
macro_rules! key {
    ($type:item) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        $type
    };
}

/// Macro for simplifying custom Value type definition
#[macro_export]
macro_rules! value {
    ($type:item) => {
        #[derive(Clone, Serialize, Deserialize, Debug)]
        $type
    };
}

#[test]
fn test_macro_key() {
    key!(
    enum MyKey {
        String(String),
        Int(i32)
    });
}

#[test]
fn test_macro_value() {
    value!(
    enum MyValue {
        String(String),
        Int(i32)
    });
}
