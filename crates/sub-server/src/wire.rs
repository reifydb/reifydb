// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Wire format types for the WebSocket and HTTP protocol layers.
//!
//! Parameters are encoded in the following wire format:
//! - Positional: `[{"type":"Int2","value":"1234"}, ...]`
//! - Named: `{"key": {"type":"Int2","value":"1234"}, ...}`
//!
//! These types deserialize that format and convert it to the internal [`Params`] type.

use std::collections::HashMap;

use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{
		Value,
		blob::Blob,
		decimal::parse::parse_decimal,
		identity::IdentityId,
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		uuid::parse::{parse_uuid4, parse_uuid7},
	},
};
use serde::{Deserialize, Serialize};

/// Wire format for a single typed value: `{"type": "Int2", "value": "1234"}`
#[derive(Debug, Serialize, Deserialize)]
pub struct WireValue {
	#[serde(rename = "type")]
	pub type_name: String,
	pub value: String,
}

/// Wire format for query parameters.
///
/// Either positional or named:
/// - Positional: `[{"type":"Int2","value":"1234"}, ...]`
/// - Named: `{"key": {"type":"Int2","value":"1234"}, ...}`
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireParams {
	Positional(Vec<WireValue>),
	Named(HashMap<String, WireValue>),
}

fn wire_value_to_value(wire: WireValue) -> Result<Value, String> {
	let v = wire.value.as_str();
	match wire.type_name.as_str() {
		"None" => Ok(Value::none()),
		"Boolean" => v
			.parse::<bool>()
			.map(Value::Boolean)
			.map_err(|e| format!("invalid Boolean value '{}': {}", v, e)),
		"Float4" => {
			v.parse::<f32>().map(Value::float4).map_err(|e| format!("invalid Float4 value '{}': {}", v, e))
		}
		"Float8" => {
			v.parse::<f64>().map(Value::float8).map_err(|e| format!("invalid Float8 value '{}': {}", v, e))
		}
		"Int1" => v.parse::<i8>().map(Value::Int1).map_err(|e| format!("invalid Int1 value '{}': {}", v, e)),
		"Int2" => v.parse::<i16>().map(Value::Int2).map_err(|e| format!("invalid Int2 value '{}': {}", v, e)),
		"Int4" => v.parse::<i32>().map(Value::Int4).map_err(|e| format!("invalid Int4 value '{}': {}", v, e)),
		"Int8" => v.parse::<i64>().map(Value::Int8).map_err(|e| format!("invalid Int8 value '{}': {}", v, e)),
		"Int16" => {
			v.parse::<i128>().map(Value::Int16).map_err(|e| format!("invalid Int16 value '{}': {}", v, e))
		}
		"Utf8" => Ok(Value::Utf8(v.to_string())),
		"Uint1" => v.parse::<u8>().map(Value::Uint1).map_err(|e| format!("invalid Uint1 value '{}': {}", v, e)),
		"Uint2" => {
			v.parse::<u16>().map(Value::Uint2).map_err(|e| format!("invalid Uint2 value '{}': {}", v, e))
		}
		"Uint4" => {
			v.parse::<u32>().map(Value::Uint4).map_err(|e| format!("invalid Uint4 value '{}': {}", v, e))
		}
		"Uint8" => {
			v.parse::<u64>().map(Value::Uint8).map_err(|e| format!("invalid Uint8 value '{}': {}", v, e))
		}
		"Uint16" => {
			v.parse::<u128>().map(Value::Uint16).map_err(|e| format!("invalid Uint16 value '{}': {}", v, e))
		}
		"Uuid4" => parse_uuid4(Fragment::internal(v))
			.map(Value::Uuid4)
			.map_err(|e| format!("invalid Uuid4 value '{}': {:?}", v, e)),
		"Uuid7" => parse_uuid7(Fragment::internal(v))
			.map(Value::Uuid7)
			.map_err(|e| format!("invalid Uuid7 value '{}': {:?}", v, e)),
		"Date" => parse_date(Fragment::internal(v))
			.map(Value::Date)
			.map_err(|e| format!("invalid Date value '{}': {:?}", v, e)),
		"DateTime" => parse_datetime(Fragment::internal(v))
			.map(Value::DateTime)
			.map_err(|e| format!("invalid DateTime value '{}': {:?}", v, e)),
		"Time" => parse_time(Fragment::internal(v))
			.map(Value::Time)
			.map_err(|e| format!("invalid Time value '{}': {:?}", v, e)),
		"Duration" => parse_duration(Fragment::internal(v))
			.map(Value::Duration)
			.map_err(|e| format!("invalid Duration value '{}': {:?}", v, e)),
		"Blob" => Blob::from_hex(Fragment::internal(v))
			.map(Value::Blob)
			.map_err(|e| format!("invalid Blob value '{}': {:?}", v, e)),
		"Decimal" => parse_decimal(Fragment::internal(v))
			.map(Value::Decimal)
			.map_err(|e| format!("invalid Decimal value '{}': {:?}", v, e)),
		"IdentityId" => parse_uuid7(Fragment::internal(v))
			.map(|u| Value::IdentityId(IdentityId::new(u)))
			.map_err(|e| format!("invalid IdentityId value '{}': {:?}", v, e)),
		_ => Err(format!("unknown type '{}'", wire.type_name)),
	}
}

impl WireParams {
	pub fn into_params(self) -> Result<Params, String> {
		match self {
			WireParams::Positional(items) => {
				let mut values = Vec::with_capacity(items.len());
				for item in items {
					values.push(wire_value_to_value(item)?);
				}
				Ok(Params::Positional(values))
			}
			WireParams::Named(map) => {
				let mut result = HashMap::with_capacity(map.len());
				for (key, wire) in map {
					result.insert(key, wire_value_to_value(wire)?);
				}
				Ok(Params::Named(result))
			}
		}
	}
}
