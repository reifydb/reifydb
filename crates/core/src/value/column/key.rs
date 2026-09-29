// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::serializer::KeySerializer, tag::ValueKind};
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
