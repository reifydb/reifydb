// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use serde_json::{Map, Value as JsonValue, json};

use super::Value;

impl Value {
	pub fn to_json_value(&self) -> JsonValue {
		match self {
			Value::None {
				..
			} => JsonValue::Null,
			Value::Boolean(b) => JsonValue::Bool(*b),
			Value::Int1(i) => json!(*i),
			Value::Int2(i) => json!(*i),
			Value::Int4(i) => json!(*i),
			Value::Int8(i) => json!(*i),
			Value::Int16(i) => JsonValue::String(i.to_string()),
			Value::Uint1(u) => json!(*u),
			Value::Uint2(u) => json!(*u),
			Value::Uint4(u) => json!(*u),
			Value::Uint8(u) => json!(*u),
			Value::Uint16(u) => JsonValue::String(u.to_string()),
			Value::Float4(f) => {
				let v: f32 = **f;
				if v.is_finite() {
					json!(v)
				} else {
					JsonValue::Null
				}
			}
			Value::Float8(f) => {
				let v: f64 = **f;
				if v.is_finite() {
					json!(v)
				} else {
					JsonValue::Null
				}
			}
			Value::Utf8(s) => JsonValue::String(s.clone()),
			Value::Int(i) => JsonValue::String(i.to_string()),
			Value::Uint(u) => JsonValue::String(u.to_string()),
			Value::Decimal(d) => JsonValue::String(d.to_string()),
			Value::Uuid4(u) => JsonValue::String(u.to_string()),
			Value::Uuid7(u) => JsonValue::String(u.to_string()),
			Value::IdentityId(id) => JsonValue::String(id.to_string()),
			Value::Date(d) => JsonValue::String(d.to_string()),
			Value::DateTime(dt) => JsonValue::String(dt.to_string()),
			Value::Time(t) => JsonValue::String(t.to_string()),
			Value::Duration(d) => JsonValue::String(d.to_string()),
			Value::Blob(b) => JsonValue::String(b.to_string()),
			Value::DictionaryId(id) => JsonValue::String(id.to_string()),
			Value::Type(t) => JsonValue::String(t.to_string()),
			Value::Any(v) => v.to_json_value(),
			Value::List(items) => JsonValue::Array(items.iter().map(|v| v.to_json_value()).collect()),
			Value::Record(fields) => {
				let map: Map<String, JsonValue> =
					fields.iter().map(|(k, v)| (k.clone(), v.to_json_value())).collect();
				JsonValue::Object(map)
			}
			Value::Tuple(items) => JsonValue::Array(items.iter().map(|v| v.to_json_value()).collect()),
		}
	}
}
