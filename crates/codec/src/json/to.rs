// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	util::hex::encode,
	value::{Value, diff_type::DiffType, frame::frame::Frame, value_type::ValueType},
};
use serde_json::{Error, Value as JsonValue, to_string};

use crate::{
	json::{
		none_marker,
		types::{ResponseColumn, ResponseFrame},
		wire_type::WireValueType,
	},
	tag::peel_options,
};

pub fn value_to_json(value: &Value, declared: &ValueType) -> JsonValue {
	let depth = peel_options(declared).1;
	match value {
		Value::None {
			inner,
		} => JsonValue::String(none_marker(depth.saturating_sub(peel_options(inner).1 + 1))),
		Value::Blob(b) => JsonValue::String(b.to_hex()),
		Value::Digest(digest) => JsonValue::String(format!("0x{}", encode(&digest.encode()))),
		Value::List(items) => {
			let elem_ty = match peel_options(declared).0 {
				ValueType::List(inner) => inner.as_ref(),
				_ => &ValueType::Any,
			};
			JsonValue::Array(items.iter().map(|item| value_to_json(item, elem_ty)).collect())
		}
		Value::Record(fields) => {
			let field_types: &[(String, ValueType)] = match peel_options(declared).0 {
				ValueType::Record(f) => f.as_slice(),
				_ => &[],
			};
			JsonValue::Object(
				fields.iter()
					.map(|(name, item)| {
						let field_ty = field_types
							.iter()
							.find(|(n, _)| n == name)
							.map(|(_, t)| t)
							.unwrap_or(&ValueType::Any);
						(name.clone(), value_to_json(item, field_ty))
					})
					.collect(),
			)
		}
		_ => JsonValue::String(value.to_string()),
	}
}

pub fn convert_frames(frames: &[Frame]) -> Vec<ResponseFrame> {
	let mut result = Vec::new();

	for frame in frames {
		let row_numbers: Vec<u64> = frame.row_numbers().iter().map(|rn| rn.value()).collect();
		let created_at: Vec<String> = frame.created_at().iter().map(|dt| dt.to_string()).collect();
		let updated_at: Vec<String> = frame.updated_at().iter().map(|dt| dt.to_string()).collect();
		let time: Vec<String> = frame.time().iter().map(|dt| dt.to_string()).collect();

		let mut columns = Vec::new();

		for column in frame.iter() {
			let column_type = column.data.get_type();
			let column_data: Vec<JsonValue> =
				column.data.iter().map(|value| value_to_json(&value, &column_type)).collect();

			columns.push(ResponseColumn {
				name: column.name.clone(),
				r#type: WireValueType(column_type),
				payload: column_data,
			});
		}

		result.push(ResponseFrame {
			op: frame.op.map(DiffType::as_u8),
			row_numbers,
			created_at,
			updated_at,
			time,
			columns,
		});
	}

	result
}

pub fn frames_to_json(frames: &[Frame]) -> Result<String, Error> {
	let response_frames = convert_frames(frames);
	to_string(&response_frames)
}
