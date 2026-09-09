// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::value_type::ValueType;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError, ser::Error as SerError};
use serde_json::{Map, Value as JsonValue};

#[derive(Debug, Clone, PartialEq)]
pub struct WireValueType(pub ValueType);

impl From<ValueType> for WireValueType {
	fn from(ty: ValueType) -> Self {
		Self(ty)
	}
}

impl From<WireValueType> for ValueType {
	fn from(wire: WireValueType) -> Self {
		wire.0
	}
}

const ID: &str = "id";
const UNDERLYING: &str = "underlying";
const NAME: &str = "name";
const TYPE: &str = "type";

fn scalar_id(ty: &ValueType) -> Option<&'static str> {
	Some(match ty {
		ValueType::Boolean => "Boolean",
		ValueType::Float4 => "Float4",
		ValueType::Float8 => "Float8",
		ValueType::Int1 => "Int1",
		ValueType::Int2 => "Int2",
		ValueType::Int4 => "Int4",
		ValueType::Int8 => "Int8",
		ValueType::Int16 => "Int16",
		ValueType::Utf8 => "Utf8",
		ValueType::Uint1 => "Uint1",
		ValueType::Uint2 => "Uint2",
		ValueType::Uint4 => "Uint4",
		ValueType::Uint8 => "Uint8",
		ValueType::Uint16 => "Uint16",
		ValueType::Date => "Date",
		ValueType::DateTime => "DateTime",
		ValueType::Time => "Time",
		ValueType::Duration => "Duration",
		ValueType::IdentityId => "IdentityId",
		ValueType::Uuid4 => "Uuid4",
		ValueType::Uuid7 => "Uuid7",
		ValueType::Blob => "Blob",
		ValueType::Int => "Int",
		ValueType::Uint => "Uint",
		ValueType::Decimal => "Decimal",
		ValueType::Any => "Any",
		ValueType::DictionaryId => "DictionaryId",
		ValueType::Option(_) | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_) => {
			return None;
		}
	})
}

fn scalar_from_id(id: &str) -> Option<ValueType> {
	Some(match id {
		"Boolean" => ValueType::Boolean,
		"Float4" => ValueType::Float4,
		"Float8" => ValueType::Float8,
		"Int1" => ValueType::Int1,
		"Int2" => ValueType::Int2,
		"Int4" => ValueType::Int4,
		"Int8" => ValueType::Int8,
		"Int16" => ValueType::Int16,
		"Utf8" => ValueType::Utf8,
		"Uint1" => ValueType::Uint1,
		"Uint2" => ValueType::Uint2,
		"Uint4" => ValueType::Uint4,
		"Uint8" => ValueType::Uint8,
		"Uint16" => ValueType::Uint16,
		"Date" => ValueType::Date,
		"DateTime" => ValueType::DateTime,
		"Time" => ValueType::Time,
		"Duration" => ValueType::Duration,
		"IdentityId" => ValueType::IdentityId,
		"Uuid4" => ValueType::Uuid4,
		"Uuid7" => ValueType::Uuid7,
		"Blob" => ValueType::Blob,
		"Int" => ValueType::Int,
		"Uint" => ValueType::Uint,
		"Decimal" => ValueType::Decimal,
		"Any" => ValueType::Any,
		"DictionaryId" => ValueType::DictionaryId,
		_ => return None,
	})
}

fn descriptor(id: &str, underlying: Option<JsonValue>) -> JsonValue {
	let mut object = Map::new();
	object.insert(ID.to_string(), JsonValue::String(id.to_string()));
	if let Some(underlying) = underlying {
		object.insert(UNDERLYING.to_string(), underlying);
	}
	JsonValue::Object(object)
}

pub fn to_json(ty: &ValueType) -> JsonValue {
	if let Some(id) = scalar_id(ty) {
		return descriptor(id, None);
	}
	match ty {
		ValueType::Option(inner) => descriptor("Option", Some(to_json(inner))),
		ValueType::List(inner) => descriptor("List", Some(to_json(inner))),
		ValueType::Tuple(members) => {
			descriptor("Tuple", Some(JsonValue::Array(members.iter().map(to_json).collect())))
		}
		ValueType::Record(fields) => {
			let entries = fields
				.iter()
				.map(|(name, ty)| {
					let mut field = Map::new();
					field.insert(NAME.to_string(), JsonValue::String(name.clone()));
					field.insert(TYPE.to_string(), to_json(ty));
					JsonValue::Object(field)
				})
				.collect();
			descriptor("Record", Some(JsonValue::Array(entries)))
		}

		other => descriptor(&other.to_string(), None),
	}
}

pub fn from_json(value: &JsonValue) -> Result<ValueType, String> {
	let object = value.as_object().ok_or_else(|| format!("expected a type descriptor object, got {value}"))?;
	let id = object
		.get(ID)
		.and_then(JsonValue::as_str)
		.ok_or_else(|| format!("type descriptor is missing a string `{ID}`: {value}"))?;
	let underlying = object.get(UNDERLYING);

	if let Some(scalar) = scalar_from_id(id) {
		return Ok(scalar);
	}

	let child = || -> Result<ValueType, String> {
		let underlying = underlying.ok_or_else(|| format!("`{id}` needs an `{UNDERLYING}` type: {value}"))?;
		from_json(underlying)
	};

	match id {
		"Option" => Ok(ValueType::Option(Box::new(child()?))),
		"List" => Ok(ValueType::List(Box::new(child()?))),
		"Tuple" => {
			let members = underlying
				.and_then(JsonValue::as_array)
				.ok_or_else(|| format!("`Tuple` needs an `{UNDERLYING}` array: {value}"))?;
			members.iter().map(from_json).collect::<Result<Vec<_>, _>>().map(ValueType::Tuple)
		}
		"Record" => {
			let fields = underlying
				.and_then(JsonValue::as_array)
				.ok_or_else(|| format!("`Record` needs an `{UNDERLYING}` array: {value}"))?;
			fields.iter()
				.map(|field| {
					let name = field
						.get(NAME)
						.and_then(JsonValue::as_str)
						.ok_or_else(|| format!("record field is missing `{NAME}`: {field}"))?;
					let ty = field
						.get(TYPE)
						.ok_or_else(|| format!("record field is missing `{TYPE}`: {field}"))?;
					Ok((name.to_string(), from_json(ty)?))
				})
				.collect::<Result<Vec<_>, String>>()
				.map(ValueType::Record)
		}
		unknown => Err(format!("unknown type id `{unknown}`")),
	}
}

impl Serialize for WireValueType {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		to_json(&self.0).serialize(serializer).map_err(S::Error::custom)
	}
}

impl<'de> Deserialize<'de> for WireValueType {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let value = JsonValue::deserialize(deserializer)?;
		from_json(&value).map(WireValueType).map_err(D::Error::custom)
	}
}
