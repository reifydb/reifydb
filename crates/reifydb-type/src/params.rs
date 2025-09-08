// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::collections::HashMap;

use crate::Value;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Params {
	#[default]
	None,
	Positional(Vec<Value>),
	Named(HashMap<String, Value>),
}

impl Params {
	pub fn get_positional(&self, index: usize) -> Option<&Value> {
		match self {
			Params::Positional(values) => values.get(index),
			_ => None,
		}
	}

	pub fn get_named(&self, name: &str) -> Option<&Value> {
		match self {
			Params::Named(map) => map.get(name),
			_ => None,
		}
	}

	pub fn empty() -> Params {
		Params::None
	}
}

impl From<()> for Params {
	fn from(_: ()) -> Self {
		Params::None
	}
}

impl From<Vec<Value>> for Params {
	fn from(values: Vec<Value>) -> Self {
		Params::Positional(values)
	}
}

impl From<HashMap<String, Value>> for Params {
	fn from(map: HashMap<String, Value>) -> Self {
		Params::Named(map)
	}
}

impl<const N: usize> From<[Value; N]> for Params {
	fn from(values: [Value; N]) -> Self {
		Params::Positional(values.to_vec())
	}
}

#[macro_export]
macro_rules! params {
    // Empty params
    () => {
        $crate::Params::None
    };

    // Empty named parameters
    {} => {
        $crate::Params::None
    };

    // Named parameters with mixed keys: params!{ name: value, "key": value }
    { $($key:tt : $value:expr),+ $(,)? } => {
        {
            let mut map = ::std::collections::HashMap::new();
            $(
                map.insert($crate::params_key!($key), $crate::IntoValue::into_value($value));
            )*
            $crate::Params::Named(map)
        }
    };

    // Empty positional parameters
    [] => {
        $crate::Params::None
    };

    // Positional parameters: params![value1, value2, ...]
    [ $($value:expr),+ $(,)? ] => {
        {
            let values = vec![
                $($crate::IntoValue::into_value($value)),*
            ];
            $crate::Params::Positional(values)
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! params_key {
	($key:ident) => {
		stringify!($key).to_string()
	};
	($key:literal) => {
		$key.to_string()
	};
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::IntoValue;

	#[test]
	fn test_params_macro_positional() {
		let params = params![42, true, "hello"];
		match params {
			Params::Positional(values) => {
				assert_eq!(values.len(), 3);
				assert_eq!(values[0], Value::Int4(42));
				assert_eq!(values[1], Value::Bool(true));
				assert_eq!(
					values[2],
					Value::Utf8("hello".to_string())
				);
			}
			_ => panic!("Expected positional params"),
		}
	}

	#[test]
	fn test_params_macro_named() {
		let params = params! {
		    name: true,
		    other: 42,
		    message: "test"
		};
		match params {
			Params::Named(map) => {
				assert_eq!(map.len(), 3);
				assert_eq!(
					map.get("name"),
					Some(&Value::Bool(true))
				);
				assert_eq!(
					map.get("other"),
					Some(&Value::Int4(42))
				);
				assert_eq!(
					map.get("message"),
					Some(&Value::Utf8("test".to_string()))
				);
			}
			_ => panic!("Expected named params"),
		}
	}

	#[test]
	fn test_params_macro_named_with_strings() {
		let params = params! {
		    "string_key": 100,
		    ident_key: 200,
		    "another-key": "value"
		};
		match params {
			Params::Named(map) => {
				assert_eq!(map.len(), 3);
				assert_eq!(
					map.get("string_key"),
					Some(&Value::Int4(100))
				);
				assert_eq!(
					map.get("ident_key"),
					Some(&Value::Int4(200))
				);
				assert_eq!(
					map.get("another-key"),
					Some(&Value::Utf8("value".to_string()))
				);
			}
			_ => panic!("Expected named params"),
		}
	}

	#[test]
	fn test_params_macro_empty() {
		let params = params!();
		assert_eq!(params, Params::None);

		let params2 = params! {};
		assert_eq!(params2, Params::None);

		let params3 = params![];
		assert_eq!(params3, Params::None);
	}

	#[test]
	fn test_params_macro_with_values() {
		let v1 = Value::Int8(100);
		let v2 = 200i64.into_value();

		let params = params![v1, v2, 300];
		match params {
			Params::Positional(values) => {
				assert_eq!(values.len(), 3);
				assert_eq!(values[0], Value::Int8(100));
				assert_eq!(values[1], Value::Int8(200));
				assert_eq!(values[2], Value::Int4(300));
			}
			_ => panic!("Expected positional params"),
		}
	}

	#[test]
	fn test_params_macro_trailing_comma() {
		let params1 = params![1, 2, 3,];
		let params2 = params! { a: 1, b: 2};

		match params1 {
			Params::Positional(values) => {
				assert_eq!(values.len(), 3)
			}
			_ => panic!("Expected positional params"),
		}

		match params2 {
			Params::Named(map) => assert_eq!(map.len(), 2),
			_ => panic!("Expected named params"),
		}
	}
}
