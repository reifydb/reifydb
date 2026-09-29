// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_value::value::{
	constraint::bytes::MaxBytes,
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};

pub(crate) fn array_column(name: &str, value_type: ValueType, array: ArrayRef) -> (FieldRef, ArrayRef) {
	named(name, field_type(value_type, &array), array)
}

pub(crate) fn utf8_column(name: &str, max_bytes: MaxBytes, container: LargeStringArray) -> (FieldRef, ArrayRef) {
	let array: ArrayRef = Arc::new(container);
	let field_type = FieldType {
		max_bytes: (max_bytes != MaxBytes::MAX).then_some(max_bytes),
		..field_type(ValueType::Utf8, &array)
	};
	named(name, field_type, array)
}

fn field_type(value_type: ValueType, array: &ArrayRef) -> FieldType {
	match array.null_count() > 0 {
		true => FieldType::from(ValueType::Option(Box::new(value_type))),
		false => FieldType::from(value_type),
	}
}
