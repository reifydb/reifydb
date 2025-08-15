// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, fmt, str::FromStr};

use reifydb_core::{
	Blob, BorrowedSpan, OrderedF32, OrderedF64, RowId, Type, Value,
	value::{
		boolean::parse_bool,
		number::{parse_float, parse_int, parse_uint},
		temporal::{
			parse_date, parse_datetime, parse_interval, parse_time,
		},
		uuid::{parse_uuid4, parse_uuid7},
	},
};
#[cfg(test)]
use reifydb_core::{
	Date, DateTime, Interval, Time, Uuid4, Uuid7, value::IdentityId,
};
use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
	Auth(AuthRequest),
	Command(CommandRequest),
	Query(QueryRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub statements: Vec<String>,
	pub params: Option<WsParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub statements: Vec<String>,
	pub params: Option<WsParams>,
}

#[derive(Debug)]
pub enum WsParams {
	Positional(Vec<Value>),
	Named(HashMap<String, Value>),
}

impl Serialize for WsParams {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			WsParams::Positional(values) => {
				values.serialize(serializer)
			}
			WsParams::Named(map) => map.serialize(serializer),
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
	let span = BorrowedSpan::new(str_val);

	let parsed_value =
		match value_type {
			Type::Bool => parse_bool(span)
				.map(Value::Bool)
				.unwrap_or(Value::Undefined),
			Type::Float4 => parse_float::<f32>(span)
				.ok()
				.and_then(|f| OrderedF32::try_from(f).ok())
				.map(Value::Float4)
				.unwrap_or(Value::Undefined),
			Type::Float8 => parse_float::<f64>(span)
				.ok()
				.and_then(|f| OrderedF64::try_from(f).ok())
				.map(Value::Float8)
				.unwrap_or(Value::Undefined),
			Type::Int1 => parse_int::<i8>(span)
				.map(Value::Int1)
				.unwrap_or(Value::Undefined),
			Type::Int2 => parse_int::<i16>(span)
				.map(Value::Int2)
				.unwrap_or(Value::Undefined),
			Type::Int4 => parse_int::<i32>(span)
				.map(Value::Int4)
				.unwrap_or(Value::Undefined),
			Type::Int8 => parse_int::<i64>(span)
				.map(Value::Int8)
				.unwrap_or(Value::Undefined),
			Type::Int16 => parse_int::<i128>(span)
				.map(Value::Int16)
				.unwrap_or(Value::Undefined),
			Type::Utf8 => Value::Utf8(str_val.to_string()),
			Type::Uint1 => parse_uint::<u8>(span)
				.map(Value::Uint1)
				.unwrap_or(Value::Undefined),
			Type::Uint2 => parse_uint::<u16>(span)
				.map(Value::Uint2)
				.unwrap_or(Value::Undefined),
			Type::Uint4 => parse_uint::<u32>(span)
				.map(Value::Uint4)
				.unwrap_or(Value::Undefined),
			Type::Uint8 => parse_uint::<u64>(span)
				.map(Value::Uint8)
				.unwrap_or(Value::Undefined),
			Type::Uint16 => parse_uint::<u128>(span)
				.map(Value::Uint16)
				.unwrap_or(Value::Undefined),
			Type::Date => parse_date(span)
				.map(Value::Date)
				.unwrap_or(Value::Undefined),
			Type::DateTime => parse_datetime(span)
				.map(Value::DateTime)
				.unwrap_or(Value::Undefined),
			Type::Time => parse_time(span)
				.map(Value::Time)
				.unwrap_or(Value::Undefined),
			Type::Interval => parse_interval(span)
				.map(Value::Interval)
				.unwrap_or(Value::Undefined),
			Type::RowId => parse_uint::<u64>(span)
				.map(|id| Value::RowId(RowId::from(id)))
				.unwrap_or(Value::Undefined),
			Type::Uuid4 => parse_uuid4(span)
				.map(Value::Uuid4)
				.unwrap_or(Value::Undefined),
			Type::Uuid7 => parse_uuid7(span)
				.map(Value::Uuid7)
				.unwrap_or(Value::Undefined),
			Type::IdentityId => parse_uuid7(span)
				.map(|uuid7| {
					Value::IdentityId(reifydb_core::value::IdentityId::from(uuid7))
				})
				.unwrap_or(Value::Undefined),
			Type::Blob => Blob::from_hex(span)
				.map(Value::Blob)
				.unwrap_or(Value::Undefined),
			Type::Undefined => Value::Undefined,
		};

	Ok(parsed_value)
}

impl<'de> Deserialize<'de> for WsParams {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct WsParamsVisitor;

		impl<'de> Visitor<'de> for WsParamsVisitor {
			type Value = WsParams;

			fn expecting(
				&self,
				formatter: &mut fmt::Formatter,
			) -> fmt::Result {
				formatter.write_str(
					"an array for positional parameters or an object for named parameters",
				)
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

				Ok(WsParams::Positional(values))
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

				Ok(WsParams::Named(result_map))
			}
		}

		deserializer.deserialize_any(WsParamsVisitor)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn roundtrip(params: WsParams) {
		// Convert to the new format for serialization
		let json_value = match &params {
			WsParams::Positional(values) => {
				let array: Vec<serde_json::Value> =
					values.iter()
						.map(|v| {
							let value_str = match v {
                            Value::Undefined => String::new(),
                            Value::Blob(b) => hex::encode(&**b),
                            _ => v.to_string(),
                        };
							serde_json::json!({
							    "type": v.get_type().to_string(),
							    "value": value_str
							})
						})
						.collect();
				serde_json::Value::Array(array)
			}
			WsParams::Named(map) => {
				let obj: serde_json::Map<
					String,
					serde_json::Value,
				> =
					map.iter()
						.map(|(k, v)| {
							let value_str = match v {
                            Value::Undefined => String::new(),
                            Value::Blob(b) => hex::encode(&**b),
                            _ => v.to_string(),
                        };
							(
								k.clone(),
								serde_json::json!({
								   "type": v.get_type().to_string(),
								   "value": value_str
								}),
							)
						})
						.collect();
				serde_json::Value::Object(obj)
			}
		};

		let serialized = serde_json::to_string(&json_value).unwrap();
		let deserialized: WsParams =
			serde_json::from_str(&serialized).unwrap();

		match (&params, &deserialized) {
			(
				WsParams::Positional(v1),
				WsParams::Positional(v2),
			) => {
				assert_eq!(v1.len(), v2.len());
				for (val1, val2) in v1.iter().zip(v2.iter()) {
					match (val1, val2) {
						(
							Value::Uuid4(_),
							Value::Uuid4(_),
						) => (), // UUIDs will be
						// different
						(
							Value::Uuid7(_),
							Value::Uuid7(_),
						) => (), // UUIDs will be
						// different
						(
							Value::IdentityId(_),
							Value::IdentityId(_),
						) => (), // IdentityIds will be
						// different
						(
							Value::DateTime(_),
							Value::DateTime(_),
						) => (), // DateTimes may be
						// different
						_ => assert_eq!(val1, val2),
					}
				}
			}
			(WsParams::Named(m1), WsParams::Named(m2)) => {
				assert_eq!(m1.len(), m2.len());
				for (k, v1) in m1.iter() {
					let v2 = m2.get(k).expect(
						"Key missing in deserialized map",
					);
					match (v1, v2) {
						(
							Value::Uuid4(_),
							Value::Uuid4(_),
						) => (), // UUIDs will be
						// different
						(
							Value::Uuid7(_),
							Value::Uuid7(_),
						) => (), // UUIDs will be
						// different
						(
							Value::IdentityId(_),
							Value::IdentityId(_),
						) => (), // IdentityIds will be
						// different
						(
							Value::DateTime(_),
							Value::DateTime(_),
						) => (), // DateTimes may be
						// different
						_ => assert_eq!(v1, v2),
					}
				}
			}
			_ => panic!("Type mismatch after deserialization"),
		}
	}

	#[test]
	fn test_undefined() {
		let params = WsParams::Positional(vec![Value::Undefined]);
		roundtrip(params);
	}

	#[test]
	fn test_bool() {
		let params = WsParams::Positional(vec![
			Value::Bool(true),
			Value::Bool(false),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_float4() {
		let params = WsParams::Positional(vec![
			Value::Float4(OrderedF32::try_from(3.14_f32).unwrap()),
			Value::Float4(OrderedF32::try_from(-0.5_f32).unwrap()),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_float8() {
		let params = WsParams::Positional(vec![
			Value::Float8(
				OrderedF64::try_from(3.14159265_f64).unwrap(),
			),
			Value::Float8(
				OrderedF64::try_from(-0.00001_f64).unwrap(),
			),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_int1() {
		let params = WsParams::Positional(vec![
			Value::Int1(i8::MIN),
			Value::Int1(i8::MAX),
			Value::Int1(0),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_int2() {
		let params = WsParams::Positional(vec![
			Value::Int2(i16::MIN),
			Value::Int2(i16::MAX),
			Value::Int2(0),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_int4() {
		let params = WsParams::Positional(vec![
			Value::Int4(i32::MIN),
			Value::Int4(i32::MAX),
			Value::Int4(0),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_int8() {
		let params = WsParams::Positional(vec![
			Value::Int8(i64::MIN),
			Value::Int8(i64::MAX),
			Value::Int8(0),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_int16() {
		let params = WsParams::Positional(vec![
			Value::Int16(i128::MIN),
			Value::Int16(i128::MAX),
			Value::Int16(0),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_utf8() {
		let params = WsParams::Positional(vec![
			Value::Utf8("Hello".to_string()),
			Value::Utf8("世界".to_string()),
			Value::Utf8("".to_string()),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_uint1() {
		let params = WsParams::Positional(vec![
			Value::Uint1(u8::MIN),
			Value::Uint1(u8::MAX),
			Value::Uint1(128),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_uint2() {
		let params = WsParams::Positional(vec![
			Value::Uint2(u16::MIN),
			Value::Uint2(u16::MAX),
			Value::Uint2(32768),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_uint4() {
		let params = WsParams::Positional(vec![
			Value::Uint4(u32::MIN),
			Value::Uint4(u32::MAX),
			Value::Uint4(2147483648),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_uint8() {
		let params = WsParams::Positional(vec![
			Value::Uint8(u64::MIN),
			Value::Uint8(u64::MAX),
			Value::Uint8(9223372036854775808),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_uint16() {
		let params = WsParams::Positional(vec![
			Value::Uint16(u128::MIN),
			Value::Uint16(u128::MAX),
			Value::Uint16(170141183460469231731687303715884105728),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_date() {
		let params = WsParams::Positional(vec![
			Value::Date(Date::new(2025, 1, 15).unwrap()),
			Value::Date(Date::new(1970, 1, 1).unwrap()),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_datetime() {
		let params = WsParams::Positional(vec![Value::DateTime(
			DateTime::now(),
		)]);
		roundtrip(params);
	}

	#[test]
	fn test_time() {
		let params = WsParams::Positional(vec![
			Value::Time(Time::new(12, 30, 45, 0).unwrap()),
			Value::Time(Time::new(0, 0, 0, 0).unwrap()),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_interval() {
		let params = WsParams::Positional(vec![
			Value::Interval(Interval::from_seconds(3600)),
			Value::Interval(Interval::from_seconds(0)),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_row_id() {
		let params = WsParams::Positional(vec![
			Value::RowId(RowId::from(12345u64)),
			Value::RowId(RowId::from(0u64)),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_uuid4() {
		let params = WsParams::Positional(vec![Value::Uuid4(
			Uuid4::generate(),
		)]);
		roundtrip(params);
	}

	#[test]
	fn test_uuid7() {
		let params = WsParams::Positional(vec![Value::Uuid7(
			Uuid7::generate(),
		)]);
		roundtrip(params);
	}

	#[test]
	fn test_identity_id() {
		let params = WsParams::Positional(vec![Value::IdentityId(
			IdentityId::generate(),
		)]);
		roundtrip(params);
	}

	#[test]
	fn test_blob() {
		let params = WsParams::Positional(vec![
			Value::Blob(Blob::from(vec![1, 2, 3, 4, 5])),
			Value::Blob(Blob::from(vec![])),
		]);
		roundtrip(params);
	}

	#[test]
	fn test_named_params() {
		let mut map = HashMap::new();
		map.insert("id".to_string(), Value::Int4(42));
		map.insert(
			"name".to_string(),
			Value::Utf8("Alice".to_string()),
		);
		map.insert("active".to_string(), Value::Bool(true));

		let params = WsParams::Named(map);
		roundtrip(params);
	}

	#[test]
	fn test_mixed_types_positional() {
		let params = WsParams::Positional(vec![
			Value::Bool(true),
			Value::Int4(42),
			Value::Utf8("test".to_string()),
			Value::Float8(OrderedF64::try_from(3.14).unwrap()),
			Value::Undefined,
		]);
		roundtrip(params);
	}

	#[test]
	fn test_mixed_types_named() {
		let mut map = HashMap::new();
		map.insert("bool_val".to_string(), Value::Bool(false));
		map.insert("int_val".to_string(), Value::Int8(9999));
		map.insert(
			"float_val".to_string(),
			Value::Float4(OrderedF32::try_from(2.5).unwrap()),
		);
		map.insert(
			"text_val".to_string(),
			Value::Utf8("hello world".to_string()),
		);
		map.insert(
			"date_val".to_string(),
			Value::Date(Date::new(2025, 1, 15).unwrap()),
		);

		let params = WsParams::Named(map);
		roundtrip(params);
	}

	#[test]
	fn test_empty_positional() {
		let params = WsParams::Positional(vec![]);
		roundtrip(params);
	}

	#[test]
	fn test_empty_named() {
		let params = WsParams::Named(HashMap::new());
		roundtrip(params);
	}

	#[test]
	fn test_json_format_array() {
		let json = r#"[{"Int4":42},{"Utf8":"hello"},{"Bool":true}]"#;
		let params: WsParams = serde_json::from_str(json).unwrap();

		match &params {
			WsParams::Positional(values) => {
				assert_eq!(values.len(), 3);
				assert_eq!(values[0], Value::Int4(42));
				assert_eq!(
					values[1],
					Value::Utf8("hello".to_string())
				);
				assert_eq!(values[2], Value::Bool(true));
			}
			_ => panic!("Expected positional parameters"),
		}

		// Verify it serializes back to the same format
		let serialized = serde_json::to_string(&params).unwrap();
		assert_eq!(serialized, json);
	}

	#[test]
	fn test_json_format_object() {
		let json = r#"{"id":{"Int4":42},"name":{"Utf8":"Alice"},"active":{"Bool":true}}"#;
		let params: WsParams = serde_json::from_str(json).unwrap();

		match &params {
			WsParams::Named(map) => {
				assert_eq!(map.len(), 3);
				assert_eq!(
					map.get("id"),
					Some(&Value::Int4(42))
				);
				assert_eq!(
					map.get("name"),
					Some(&Value::Utf8("Alice".to_string()))
				);
				assert_eq!(
					map.get("active"),
					Some(&Value::Bool(true))
				);
			}
			_ => panic!("Expected named parameters"),
		}
	}

	#[test]
	fn test_command_request_with_params() {
		let json = r#"{"statements":["MAP $1 as result"],"params":[{"Bool":true}]}"#;
		let req: CommandRequest = serde_json::from_str(json).unwrap();

		assert_eq!(req.statements, vec!["MAP $1 as result"]);
		match req.params {
			Some(WsParams::Positional(values)) => {
				assert_eq!(values.len(), 1);
				assert_eq!(values[0], Value::Bool(true));
			}
			_ => panic!("Expected positional parameters"),
		}
	}

	#[test]
	fn test_command_request_without_params() {
		let json = r#"{"statements":["SELECT * FROM users"]}"#;
		let req: CommandRequest = serde_json::from_str(json).unwrap();

		assert_eq!(req.statements, vec!["SELECT * FROM users"]);
		assert!(req.params.is_none());
	}

	#[test]
	fn test_full_request_deserialization() {
		// This is the actual format the client is sending
		let json = r#"{"id":"req-1","type":"Command","payload":{"statements":["MAP $1 as result"],"params":[{"type":"BOOL","value":"true"}]}}"#;
		let request: Request = serde_json::from_str(json).unwrap();

		assert_eq!(request.id, "req-1");
		match request.payload {
			RequestPayload::Command(cmd) => {
				assert_eq!(
					cmd.statements,
					vec!["MAP $1 as result"]
				);
				match cmd.params {
					Some(WsParams::Positional(values)) => {
						assert_eq!(values.len(), 1);
						assert_eq!(
							values[0],
							Value::Bool(true)
						);
					}
					_ => panic!(
						"Expected positional parameters"
					),
				}
			}
			_ => panic!("Expected Command payload"),
		}
	}

	#[test]
	fn test_client_format_params() {
		// Test the client format with various types
		let json = r#"[{"type":"BOOL","value":"true"},{"type":"INT4","value":"42"},{"type":"UTF8","value":"hello"}]"#;
		let params: WsParams = serde_json::from_str(json).unwrap();

		match params {
			WsParams::Positional(values) => {
				assert_eq!(values.len(), 3);
				assert_eq!(values[0], Value::Bool(true));
				assert_eq!(values[1], Value::Int4(42));
				assert_eq!(
					values[2],
					Value::Utf8("hello".to_string())
				);
			}
			_ => panic!("Expected positional parameters"),
		}
	}

	#[test]
	fn test_client_format_named_params() {
		let json = r#"{
            "id": {"type":"Int4","value":"42"},
            "name": {"type":"Utf8","value":"Alice"},
            "active": {"type":"Bool","value":"true"},
            "score": {"type":"Float8","value":"98.5"}
        }"#;

		let params: WsParams = serde_json::from_str(json).unwrap();
		match params {
			WsParams::Named(map) => {
				assert_eq!(map.len(), 4);
				assert_eq!(
					map.get("id"),
					Some(&Value::Int4(42))
				);
				assert_eq!(
					map.get("name"),
					Some(&Value::Utf8("Alice".to_string()))
				);
				assert_eq!(
					map.get("active"),
					Some(&Value::Bool(true))
				);
				match map.get("score") {
					Some(Value::Float8(f)) => {
						assert!((**f - 98.5).abs()
							< 0.01)
					}
					_ => panic!(
						"Expected Float8 for score"
					),
				}
			}
			_ => panic!("Expected named parameters"),
		}
	}

	#[test]
	fn test_mixed_format_params() {
		// Test that we can handle both old and new formats in the same
		// array
		let json = r#"[
            {"type":"Bool","value":"true"},
            {"Int4":42},
            {"type":"Utf8","value":"hello"},
            {"Bool":false}
        ]"#;

		let params: WsParams = serde_json::from_str(json).unwrap();
		match params {
			WsParams::Positional(values) => {
				assert_eq!(values.len(), 4);
				assert_eq!(values[0], Value::Bool(true)); // New format
				assert_eq!(values[1], Value::Int4(42)); // Old format
				assert_eq!(
					values[2],
					Value::Utf8("hello".to_string())
				); // New format
				assert_eq!(values[3], Value::Bool(false)); // Old format
			}
			_ => panic!("Expected positional parameters"),
		}
	}
}
