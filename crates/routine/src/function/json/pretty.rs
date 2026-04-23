// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{Value, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

fn to_json_pretty(value: &Value, indent: usize) -> String {
	let pad = "  ".repeat(indent);
	let inner_pad = "  ".repeat(indent + 1);
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
		Value::Any(v) => to_json_pretty(v, indent),
		Value::List(items) | Value::Tuple(items) => {
			if items.is_empty() {
				return "[]".to_string();
			}
			let inner: Vec<String> = items
				.iter()
				.map(|v| format!("{}{}", inner_pad, to_json_pretty(v, indent + 1)))
				.collect();
			format!("[\n{}\n{}]", inner.join(",\n"), pad)
		}
		Value::Record(fields) => {
			if fields.is_empty() {
				return "{}".to_string();
			}
			let inner: Vec<String> = fields
				.iter()
				.map(|(k, v)| {
					format!(
						"{}\"{}\": {}",
						inner_pad,
						k.replace('\\', "\\\\").replace('"', "\\\""),
						to_json_pretty(v, indent + 1)
					)
				})
				.collect();
			format!("{{\n{}\n{}}}", inner.join(",\n"), pad)
		}
	}
}

pub struct JsonPretty {
	info: FunctionInfo,
}

impl Default for JsonPretty {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonPretty {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("json::pretty"),
		}
	}
}

impl Function for JsonPretty {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.data().unwrap_option();
		let row_count = data.len();

		let results: Vec<String> = (0..row_count).map(|row| to_json_pretty(&data.get_value(row), 0)).collect();

		let result_data = ColumnBuffer::utf8(results);
		let final_data = match bitvec {
			Some(bv) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			None => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
