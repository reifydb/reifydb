// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{collections::HashMap, fmt, str::FromStr};

use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use crate::{
	Blob, BorrowedFragment, OrderedF32, OrderedF64, RowNumber, Type, Value,
	parse_bool, parse_date, parse_datetime, parse_float, parse_interval,
	parse_time, parse_uuid4, parse_uuid7,
	value::{
		IdentityId,
		number::{parse_primitive_int, parse_primitive_uint},
	},
};

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

impl Serialize for Params {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			Params::None => serializer.serialize_none(),
			Params::Positional(values) => {
				values.serialize(serializer)
			}
			Params::Named(map) => map.serialize(serializer),
		}
	}
}

// Helper function to parse value from type/value format
fn parse_typed_value(
	type_str: &str,
	value_val: &serde_json::Value,
) -> Result<Value, String> {
	// Always expect string values for consistency
	let str_val = value_val.as_str().ok_or_else(|| {
		format!("expected string value for type {}", type_str)
	})?;

	// Parse the type string to Type enum
	let value_type = match Type::from_str(type_str) {
		Ok(Type::Undefined) => return Ok(Value::Undefined),
		Ok(t) => t,
		Err(_) => return Ok(Value::Undefined),
	};

	// Use the appropriate parse function based on type
	// If parsing fails, return Value::Undefined
	let fragment = BorrowedFragment::new_internal(str_val);

	let parsed_value = match value_type {
		Type::Boolean => parse_bool(fragment)
			.map(Value::Boolean)
			.unwrap_or(Value::Undefined),
		Type::Float4 => parse_float::<f32>(fragment)
			.ok()
			.and_then(|f| OrderedF32::try_from(f).ok())
			.map(Value::Float4)
			.unwrap_or(Value::Undefined),
		Type::Float8 => parse_float::<f64>(fragment)
			.ok()
			.and_then(|f| OrderedF64::try_from(f).ok())
			.map(Value::Float8)
			.unwrap_or(Value::Undefined),
		Type::Int1 => parse_primitive_int::<i8>(fragment)
			.map(Value::Int1)
			.unwrap_or(Value::Undefined),
		Type::Int2 => parse_primitive_int::<i16>(fragment)
			.map(Value::Int2)
			.unwrap_or(Value::Undefined),
		Type::Int4 => parse_primitive_int::<i32>(fragment)
			.map(Value::Int4)
			.unwrap_or(Value::Undefined),
		Type::Int8 => parse_primitive_int::<i64>(fragment)
			.map(Value::Int8)
			.unwrap_or(Value::Undefined),
		Type::Int16 => parse_primitive_int::<i128>(fragment)
			.map(Value::Int16)
			.unwrap_or(Value::Undefined),
		Type::Utf8 => Value::Utf8(str_val.to_string()),
		Type::Uint1 => parse_primitive_uint::<u8>(fragment)
			.map(Value::Uint1)
			.unwrap_or(Value::Undefined),
		Type::Uint2 => parse_primitive_uint::<u16>(fragment)
			.map(Value::Uint2)
			.unwrap_or(Value::Undefined),
		Type::Uint4 => parse_primitive_uint::<u32>(fragment)
			.map(Value::Uint4)
			.unwrap_or(Value::Undefined),
		Type::Uint8 => parse_primitive_uint::<u64>(fragment)
			.map(Value::Uint8)
			.unwrap_or(Value::Undefined),
		Type::Uint16 => parse_primitive_uint::<u128>(fragment)
			.map(Value::Uint16)
			.unwrap_or(Value::Undefined),
		Type::Date => parse_date(fragment)
			.map(Value::Date)
			.unwrap_or(Value::Undefined),
		Type::DateTime => parse_datetime(fragment)
			.map(Value::DateTime)
			.unwrap_or(Value::Undefined),
		Type::Time => parse_time(fragment)
			.map(Value::Time)
			.unwrap_or(Value::Undefined),
		Type::Interval => parse_interval(fragment)
			.map(Value::Interval)
			.unwrap_or(Value::Undefined),
		Type::RowNumber => parse_primitive_uint::<u64>(fragment)
			.map(|id| Value::RowNumber(RowNumber::from(id)))
			.unwrap_or(Value::Undefined),
		Type::Uuid4 => parse_uuid4(fragment)
			.map(Value::Uuid4)
			.unwrap_or(Value::Undefined),
		Type::Uuid7 => parse_uuid7(fragment)
			.map(Value::Uuid7)
			.unwrap_or(Value::Undefined),
		Type::IdentityId => parse_uuid7(fragment)
			.map(|uuid7| Value::IdentityId(IdentityId::from(uuid7)))
			.unwrap_or(Value::Undefined),
		Type::Blob => Blob::from_hex(fragment)
			.map(Value::Blob)
			.unwrap_or(Value::Undefined),
		Type::Undefined => Value::Undefined,
		Type::Int | Type::Uint | Type::Decimal => {
			unimplemented!()
		}
	};

	Ok(parsed_value)
}

impl<'de> Deserialize<'de> for Params {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct ParamsVisitor;

		impl<'de> Visitor<'de> for ParamsVisitor {
			type Value = Params;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str(
					"null, an array for positional parameters, or an object for named parameters",
				)
			}

			fn visit_none<E>(self) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(Params::None)
			}

			fn visit_unit<E>(self) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(Params::None)
			}

			fn visit_seq<A>(
				self,
				mut seq: A,
			) -> Result<Self::Value, A::Error>
			where
				A: de::SeqAccess<'de>,
			{
				let mut values = Vec::new();

				while let Some(value) =
					seq.next_element::<serde_json::Value>()?
				{
					// Check if it's in the {"type": "Bool",
					// "value": "true"} format
					if let Some(obj) = value.as_object() {
						if obj.contains_key("type")
							&& obj.contains_key(
								"value",
							) {
							let type_str = obj["type"]
								.as_str()
								.ok_or_else(|| de::Error::custom("type must be a string"))?;
							let value_val =
								&obj["value"];

							let parsed_value = parse_typed_value(type_str, value_val)
								.map_err(de::Error::custom)?;
							values.push(
								parsed_value,
							);
							continue;
						}
					}

					// Otherwise try to deserialize as a
					// normal Value
					let val = Value::deserialize(value)
						.map_err(de::Error::custom)?;
					values.push(val);
				}

				Ok(Params::Positional(values))
			}

			fn visit_map<A>(
				self,
				mut map: A,
			) -> Result<Self::Value, A::Error>
			where
				A: de::MapAccess<'de>,
			{
				let mut result_map = HashMap::new();

				while let Some(key) =
					map.next_key::<String>()?
				{
					let value: serde_json::Value =
						map.next_value()?;

					// Check if it's in the {"type": "Bool",
					// "value": "true"} format
					if let Some(obj) = value.as_object() {
						if obj.contains_key("type")
							&& obj.contains_key(
								"value",
							) {
							let type_str = obj["type"]
								.as_str()
								.ok_or_else(|| de::Error::custom("type must be a string"))?;
							let value_val =
								&obj["value"];

							let parsed_value = parse_typed_value(type_str, value_val)
								.map_err(de::Error::custom)?;
							result_map.insert(
								key,
								parsed_value,
							);
							continue;
						}
					}

					// Otherwise try to deserialize as a
					// normal Value
					let val = Value::deserialize(value)
						.map_err(de::Error::custom)?;
					result_map.insert(key, val);
				}

				Ok(Params::Named(result_map))
			}
		}

		deserializer.deserialize_any(ParamsVisitor)
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
				assert_eq!(values[1], Value::Boolean(true));
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
					Some(&Value::Boolean(true))
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
