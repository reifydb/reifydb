// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{Value, r#type::Type};

use crate::function::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionResult, propagate_options};

fn to_json(value: &Value) -> String {
	match value {
		Value::None {
			..
		} => "null".to_string(),
		Value::Boolean(b) => b.to_string(),
		Value::Float4(f) => f.to_string(),
		Value::Float8(f) => f.to_string(),
		Value::Int1(i) => i.to_string(),
		Value::Int2(i) => i.to_string(),
		Value::Int4(i) => i.to_string(),
		Value::Int8(i) => i.to_string(),
		Value::Int16(i) => i.to_string(),
		Value::Uint1(u) => u.to_string(),
		Value::Uint2(u) => u.to_string(),
		Value::Uint4(u) => u.to_string(),
		Value::Uint8(u) => u.to_string(),
		Value::Uint16(u) => u.to_string(),
		Value::Int(i) => i.to_string(),
		Value::Uint(u) => u.to_string(),
		Value::Decimal(d) => d.to_string(),
		Value::Utf8(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
		Value::Uuid4(u) => format!("\"{}\"", u),
		Value::Uuid7(u) => format!("\"{}\"", u),
		Value::IdentityId(id) => format!("\"{}\"", id),
		Value::Date(d) => format!("\"{}\"", d),
		Value::DateTime(dt) => format!("\"{}\"", dt),
		Value::Time(t) => format!("\"{}\"", t),
		Value::Duration(d) => format!("\"{}\"", d.to_iso_string()),
		Value::Blob(b) => format!("\"{}\"", b),
		Value::DictionaryId(id) => format!("\"{}\"", id),
		Value::Type(t) => format!("\"{}\"", t),
		Value::Any(v) => to_json(v),
		Value::List(items) => {
			let inner: Vec<String> = items.iter().map(|v| to_json(v)).collect();
			format!("[{}]", inner.join(", "))
		}
		Value::Tuple(items) => {
			let inner: Vec<String> = items.iter().map(|v| to_json(v)).collect();
			format!("[{}]", inner.join(", "))
		}
		Value::Record(fields) => {
			let inner: Vec<String> = fields
				.iter()
				.map(|(k, v)| {
					format!("\"{}\": {}", k.replace('\\', "\\\\").replace('"', "\\\""), to_json(v))
				})
				.collect();
			format!("{{{}}}", inner.join(", "))
		}
	}
}

pub struct JsonSerialize;

impl JsonSerialize {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for JsonSerialize {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let col = columns.get(0).unwrap();
		let results: Vec<String> = (0..row_count).map(|row| to_json(&col.data().get_value(row))).collect();

		Ok(ColumnData::utf8(results))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
