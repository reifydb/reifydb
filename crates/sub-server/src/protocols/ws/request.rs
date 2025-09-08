// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, fmt, str::FromStr};

use num_bigint;
use reifydb_type::{
	Blob, BorrowedFragment, OrderedF32, OrderedF64, RowNumber, Type, Value,
	parse_bool, parse_date, parse_datetime, parse_float, parse_interval,
	parse_primitive_int, parse_primitive_uint, parse_time, parse_uuid4,
	parse_uuid7,
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
	let fragment = BorrowedFragment::new_internal(str_val);

	let parsed_value = match value_type {
		Type::Boolean => parse_bool(fragment.clone())
			.map(Value::Boolean)
			.unwrap_or(Value::Undefined),
		Type::Float4 => parse_float::<f32>(fragment.clone())
			.ok()
			.and_then(|f| OrderedF32::try_from(f).ok())
			.map(Value::Float4)
			.unwrap_or(Value::Undefined),
		Type::Float8 => parse_float::<f64>(fragment.clone())
			.ok()
			.and_then(|f| OrderedF64::try_from(f).ok())
			.map(Value::Float8)
			.unwrap_or(Value::Undefined),
		Type::Int1 => parse_primitive_int::<i8>(fragment.clone())
			.map(Value::Int1)
			.unwrap_or(Value::Undefined),
		Type::Int2 => parse_primitive_int::<i16>(fragment.clone())
			.map(Value::Int2)
			.unwrap_or(Value::Undefined),
		Type::Int4 => parse_primitive_int::<i32>(fragment.clone())
			.map(Value::Int4)
			.unwrap_or(Value::Undefined),
		Type::Int8 => parse_primitive_int::<i64>(fragment.clone())
			.map(Value::Int8)
			.unwrap_or(Value::Undefined),
		Type::Int16 => parse_primitive_int::<i128>(fragment.clone())
			.map(Value::Int16)
			.unwrap_or(Value::Undefined),
		Type::Utf8 => Value::Utf8(str_val.to_string()),
		Type::Uint1 => parse_primitive_uint::<u8>(fragment.clone())
			.map(Value::Uint1)
			.unwrap_or(Value::Undefined),
		Type::Uint2 => parse_primitive_uint::<u16>(fragment.clone())
			.map(Value::Uint2)
			.unwrap_or(Value::Undefined),
		Type::Uint4 => parse_primitive_uint::<u32>(fragment.clone())
			.map(Value::Uint4)
			.unwrap_or(Value::Undefined),
		Type::Uint8 => parse_primitive_uint::<u64>(fragment.clone())
			.map(Value::Uint8)
			.unwrap_or(Value::Undefined),
		Type::Uint16 => parse_primitive_uint::<u128>(fragment.clone())
			.map(Value::Uint16)
			.unwrap_or(Value::Undefined),
		Type::Date => parse_date(fragment.clone())
			.map(Value::Date)
			.unwrap_or(Value::Undefined),
		Type::DateTime => parse_datetime(fragment.clone())
			.map(Value::DateTime)
			.unwrap_or(Value::Undefined),
		Type::Time => parse_time(fragment.clone())
			.map(Value::Time)
			.unwrap_or(Value::Undefined),
		Type::Interval => parse_interval(fragment.clone())
			.map(Value::Interval)
			.unwrap_or(Value::Undefined),
		Type::RowNumber => {
			parse_primitive_uint::<u64>(fragment.clone())
				.map(|id| Value::RowNumber(RowNumber::from(id)))
				.unwrap_or(Value::Undefined)
		}
		Type::Uuid4 => parse_uuid4(fragment.clone())
			.map(Value::Uuid4)
			.unwrap_or(Value::Undefined),
		Type::Uuid7 => parse_uuid7(fragment.clone())
			.map(Value::Uuid7)
			.unwrap_or(Value::Undefined),
		Type::IdentityId => {
			parse_uuid7(fragment.clone())
				.map(|uuid7| {
					Value::IdentityId(reifydb_type::value::IdentityId::from(uuid7))
				})
				.unwrap_or(Value::Undefined)
		}
		Type::Blob => Blob::from_hex(fragment.clone())
			.map(Value::Blob)
			.unwrap_or(Value::Undefined),
		Type::Int => str_val
			.parse::<num_bigint::BigInt>()
			.map(|bi| Value::Int(reifydb_type::Int::from(bi)))
			.unwrap_or(Value::Undefined),
		Type::Uint => str_val
			.parse::<num_bigint::BigInt>()
			.map(|bi| Value::Uint(reifydb_type::Uint::from(bi)))
			.unwrap_or(Value::Undefined),
		Type::Decimal {
			..
		} => str_val
			.parse::<reifydb_type::Decimal>()
			.map(Value::Decimal)
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
