// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::serializer::KeySerializer,
	tag::{TypeTag, ValueKind},
};
use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::varlen_array,
		value_type::ValueType,
	},
};

pub fn extend_key(view: &ColumnView, index: usize, serializer: &mut KeySerializer) -> Result<()> {
	if view.none_at(index) {
		serializer.try_extend_value(&Value::None {
			inner: view.base_type(),
		})?;
		return Ok(());
	}
	match &view.data {
		ViewData::Utf8 {
			container,
			..
		} => match varlen_array::get(*container, index) {
			Some(text) => {
				serializer.extend_kind(ValueKind::Utf8).extend_str(text);
			}
			None => {
				serializer.extend_value(&Value::none_of(ValueType::Utf8));
			}
		},
		ViewData::Blob {
			container,
			..
		} => match varlen_array::get(*container, index) {
			Some(bytes) => {
				serializer.extend_kind(ValueKind::Blob).extend_bytes(bytes);
			}
			None => {
				serializer.extend_value(&Value::none_of(ValueType::Blob));
			}
		},
		_ => {
			serializer.try_extend_value(&view.get_value(index))?;
		}
	}
	Ok(())
}

pub fn extend_keys(view: &ColumnView, rows: &mut [KeySerializer]) -> Result<()> {
	match &view.data {
		ViewData::Float4(array) => {
			let none = none_bytes(view);
			for (index, row) in rows.iter_mut().enumerate() {
				let value = array.value(index);
				if view.none_at(index) || value.is_nan() {
					row.extend_raw(&none);
				} else {
					row.extend_kind(ValueKind::Float4).extend_f32(if value == 0.0 {
						0.0
					} else {
						value
					});
				}
			}
		}
		ViewData::Float8(array) => {
			let none = none_bytes(view);
			for (index, row) in rows.iter_mut().enumerate() {
				let value = array.value(index);
				if view.none_at(index) || value.is_nan() {
					row.extend_raw(&none);
				} else {
					row.extend_kind(ValueKind::Float8).extend_f64(if value == 0.0 {
						0.0
					} else {
						value
					});
				}
			}
		}
		_ => {
			for (index, row) in rows.iter_mut().enumerate() {
				extend_key(view, index, row)?;
			}
		}
	}
	Ok(())
}

fn none_bytes(view: &ColumnView) -> [u8; 2] {
	[
		ValueKind::None.byte(),
		TypeTag::of_type(&view.base_type())
			.expect("option nesting in a key none inner exceeds the supported depth")
			.byte(),
	]
}
