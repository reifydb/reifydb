// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::value::Value;

pub trait AsString {
	fn as_string(&self) -> String;
}

impl AsString for Value {
	fn as_string(&self) -> String {
		match self {
			Value::Undefined => "Undefined".to_string(),
			Value::Boolean(b) => b.to_string(),
			Value::Float4(f) => f.to_string(),
			Value::Float8(f) => f.to_string(),
			Value::Int1(i) => i.to_string(),
			Value::Int2(i) => i.to_string(),
			Value::Int4(i) => i.to_string(),
			Value::Int8(i) => i.to_string(),
			Value::Int16(i) => i.to_string(),
			Value::Utf8(s) => s.clone(),
			Value::Uint1(u) => u.to_string(),
			Value::Uint2(u) => u.to_string(),
			Value::Uint4(u) => u.to_string(),
			Value::Uint8(u) => u.to_string(),
			Value::Uint16(u) => u.to_string(),
			Value::Date(d) => d.to_string(),
			Value::DateTime(dt) => dt.to_string(),
			Value::Time(t) => t.to_string(),
			Value::Duration(i) => i.to_string(),
			Value::IdentityId(id) => id.to_string(),
			Value::Uuid4(u) => u.to_string(),
			Value::Uuid7(u) => u.to_string(),
			Value::Blob(b) => b.to_string(),
			Value::Int(bi) => bi.to_string(),
			Value::Uint(bu) => bu.to_string(),
			Value::Decimal(bd) => bd.to_string(),
			Value::DictionaryId(id) => id.to_string(),
			Value::Type(t) => t.to_string(),
			Value::Any(v) => v.as_string(),
		}
	}
}

impl Value {
	pub fn as_string(&self) -> String {
		AsString::as_string(self)
	}
}
