// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{precision::Precision, scale::Scale},
	digest::Digest,
	value_type::ValueType,
};
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
const ACCURACY: &str = "accuracy";
const PRECISION: &str = "precision";
const SCALE: &str = "scale";

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
		ValueType::Any => "Any",
		ValueType::DictionaryId => "DictionaryId",
		ValueType::Int {
			..
		}
		| ValueType::Uint {
			..
		}
		| ValueType::Decimal {
			..
		}
		| ValueType::Option(_)
		| ValueType::List(_)
		| ValueType::Record(_)
		| ValueType::Tuple(_)
		| ValueType::Digest {
			..
		} => {
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
		"Any" => ValueType::Any,
		"DictionaryId" => ValueType::DictionaryId,
		_ => return None,
	})
}

fn family_descriptor(id: &str, precision: Precision, scale: Option<Scale>) -> JsonValue {
	let mut object = Map::new();
	object.insert(ID.to_string(), JsonValue::String(id.to_string()));
	object.insert(PRECISION.to_string(), JsonValue::from(precision.value()));
	if let Some(scale) = scale {
		object.insert(SCALE.to_string(), JsonValue::from(scale.value()));
	}
	JsonValue::Object(object)
}

fn family_param(object: &Map<String, JsonValue>, key: &str, default: u8, value: &JsonValue) -> Result<u8, String> {
	match object.get(key) {
		None => Ok(default),
		Some(param) => param
			.as_u64()
			.and_then(|param| u8::try_from(param).ok())
			.ok_or_else(|| format!("`{key}` must be an unsigned 8-bit integer: {value}")),
	}
}

fn family_precision(
	object: &Map<String, JsonValue>,
	default: Precision,
	value: &JsonValue,
) -> Result<Precision, String> {
	let precision = family_param(object, PRECISION, default.value(), value)?;
	Precision::try_new(precision).map_err(|error| format!("invalid `{PRECISION}` {precision}: {error}"))
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
		ValueType::Int {
			precision,
		} => family_descriptor("Int", *precision, None),
		ValueType::Uint {
			precision,
		} => family_descriptor("Uint", *precision, None),
		ValueType::Decimal {
			precision,
			scale,
		} => family_descriptor("Decimal", *precision, Some(*scale)),
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
		ValueType::Digest {
			inner,
			accuracy,
		} => {
			let mut object = descriptor("Digest", Some(to_json(inner)));
			if let JsonValue::Object(fields) = &mut object {
				fields.insert(ACCURACY.to_string(), JsonValue::from(*accuracy));
			}
			object
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
		"Int" | "Uint" => {
			let default = ValueType::INT.precision().expect("int carries a precision");
			let precision = family_precision(object, default, value)?;
			Ok(if id == "Int" {
				ValueType::int(precision)
			} else {
				ValueType::uint(precision)
			})
		}
		"Decimal" => {
			let precision = family_precision(
				object,
				ValueType::DECIMAL.precision().expect("decimal carries a precision"),
				value,
			)?;
			let default_scale = ValueType::DECIMAL.scale().expect("decimal carries a scale");
			let scale = family_param(object, SCALE, default_scale.value(), value)?;
			let scale = Scale::try_new_with_precision(scale, precision)
				.map_err(|error| format!("invalid `{SCALE}` {scale}: {error}"))?;
			Ok(ValueType::decimal(precision, scale))
		}
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
		"Digest" => {
			let inner = child()?;
			let accuracy = object
				.get(ACCURACY)
				.and_then(JsonValue::as_u64)
				.and_then(|accuracy| u32::try_from(accuracy).ok())
				.ok_or_else(|| format!("`Digest` needs an unsigned 32-bit `{ACCURACY}`: {value}"))?;
			Digest::new(inner.clone(), accuracy)
				.map_err(|error| format!("invalid `Digest` type: {error}"))?;
			Ok(ValueType::Digest {
				inner: Box::new(inner),
				accuracy,
			})
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
