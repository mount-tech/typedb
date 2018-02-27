/// Macro for simplifying custom Key type definition
#[macro_export]
macro_rules! key {
    ($name:ident: $($type:tt)*) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        enum $name {
            $($type)*
        }
    };
}

/// Macro for simplifying custom Value type definition
#[macro_export]
macro_rules! value {
    ($name:ident: $($type:tt)*) => {
        #[derive(Clone, Serialize, Deserialize, Debug)]
        enum $name {
            $($type)*
        }
    };
}

#[test]
fn test_macro_key() {
    key!(MyKey: String(String), Int(i32));
}

#[test]
fn test_macro_value() {
    value!(MyValue: String(String), Int(i32));
}
