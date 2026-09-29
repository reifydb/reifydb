// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, sync::Arc};

use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	value::{
		Value,
		column_view::ColumnView,
		value_type::{
			ValueType,
			field::{FieldType, from_field, named, to_field},
		},
	},
};

use crate::value::column::{data::ColumnData, encoding::EncodingId};

#[derive(Clone, Debug)]
pub struct Canonical {
	field_type: FieldType,
	field: FieldRef,
	buffer: ArrayRef,
}

impl Canonical {
	pub fn new(field_type: FieldType, buffer: ArrayRef) -> Result<Self> {
		let field: FieldRef = Arc::new(to_field("", &field_type));
		ColumnView::try_from((&buffer, field.as_ref()))?;
		Ok(Self {
			field_type,
			field,
			buffer,
		})
	}

	pub fn from_column(column: &(FieldRef, ArrayRef)) -> Result<Self> {
		Self::new(from_field(&column.0)?, column.1.clone())
	}

	pub fn to_column(&self, name: &str) -> (FieldRef, ArrayRef) {
		named(name, self.field_type.clone(), self.buffer.clone())
	}

	pub fn field_type(&self) -> &FieldType {
		&self.field_type
	}

	pub fn buffer(&self) -> &ArrayRef {
		&self.buffer
	}

	pub fn view(&self) -> ColumnView<'_> {
		ColumnView::try_from((&self.buffer, self.field.as_ref()))
			.unwrap_or_else(|error| panic!("canonical buffer does not match its field type: {error}"))
	}

	pub fn len(&self) -> usize {
		self.buffer.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

pub fn encoding_for_type(ty: &ValueType) -> EncodingId {
	match ty {
		ValueType::Boolean => EncodingId::CANONICAL_BOOL,
		ValueType::Utf8
		| ValueType::Blob
		| ValueType::Digest {
			..
		} => EncodingId::CANONICAL_VARLEN,
		_ => EncodingId::CANONICAL_FIXED,
	}
}

impl ColumnData for Canonical {
	fn len(&self) -> usize {
		self.buffer.len()
	}

	fn encoding(&self) -> EncodingId {
		encoding_for_type(&self.view().base_type())
	}

	fn nones(&self) -> Option<NullBuffer> {
		self.buffer.logical_nulls()
	}

	fn get_value(&self, idx: usize) -> Value {
		self.view().get_value(idx)
	}

	fn as_string(&self, idx: usize) -> String {
		self.view().as_string(idx)
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn to_canonical(&self) -> Result<Arc<Canonical>> {
		Ok(Arc::new(self.clone()))
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::Int32Array;
	use reifydb_value::value::{
		Value,
		constraint::bytes::MaxBytes,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::{
			ValueType,
			field::{FieldType, from_field},
		},
	};

	use super::Canonical;
	use crate::value::column::{
		data::{Column, ColumnData},
		factory,
	};

	#[test]
	fn a_slice_keeps_the_max_bytes_and_dictionary_id() {
		// A canonical rebuilt from the bare value type drops the limit and id, so reads cannot decode.
		let (field, array) = factory::utf8("c", ["abc", "de", "f"]);
		let mut field_type = from_field(&field).unwrap();
		field_type.max_bytes = Some(MaxBytes::new(8));
		let limited = Canonical::new(field_type.clone(), array).unwrap();
		let sliced = Column::from_canonical(limited).slice(1, 3).unwrap().to_canonical().unwrap();
		assert_eq!(sliced.field_type(), &field_type);
		assert_eq!(from_field(&sliced.to_column("c").0).unwrap().max_bytes, Some(MaxBytes::new(8)));

		let (field, array) = factory::dictionary_id("d", [DictionaryEntryId::U2(1), DictionaryEntryId::U2(2)]);
		let mut field_type = from_field(&field).unwrap();
		field_type.dictionary_id = Some(DictionaryId(9));
		let tagged = Canonical::new(field_type, array).unwrap();
		let sliced = Column::from_canonical(tagged).slice(1, 2).unwrap().to_canonical().unwrap();
		assert_eq!(sliced.field_type().dictionary_id, Some(DictionaryId(9)));
		assert_eq!(sliced.get_value(0), Value::DictionaryId(DictionaryEntryId::U2(2)));
	}

	#[test]
	fn new_rejects_a_buffer_of_another_type() {
		// A buffer that does not match its type must fail here, never panic on the first read.
		let result = Canonical::new(FieldType::from(ValueType::Utf8), Arc::new(Int32Array::from(vec![1])));
		assert!(result.is_err());
	}
}
