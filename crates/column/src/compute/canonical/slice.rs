// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::data::canonical::Canonical;
use reifydb_value::Result;

pub fn slice(array: &Canonical, start: usize, end: usize) -> Result<Canonical> {
	assert!(start <= end);
	assert!(end <= array.len());

	let new_buffer = array.buffer.slice(start, end);

	Ok(Canonical::new(array.ty.clone(), array.nullable, new_buffer))
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{buffer::ColumnBuffer, builder::ColumnBuilder};
	use reifydb_value::value::{
		Value,
		container::dictionary_array::dictionary_array,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::ValueType,
	};

	use super::*;

	fn dictionary_column() -> ColumnBuffer {
		ColumnBuffer::DictionaryId {
			container: dictionary_array([
				DictionaryEntryId::U4(1),
				DictionaryEntryId::U4(2),
				DictionaryEntryId::U4(3),
			]),
			dictionary_id: Some(DictionaryId(42)),
		}
	}

	fn kept_dictionary_id(buffer: &ColumnBuffer) -> Option<DictionaryId> {
		let ColumnBuffer::DictionaryId {
			dictionary_id,
			..
		} = buffer
		else {
			panic!("expected a DictionaryId column, got {:?}", buffer.get_type());
		};
		*dictionary_id
	}

	#[test]
	fn slice_fixed_returns_subrange() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		assert_eq!(out.buffer.as_slice::<i32>(), &[20, 30, 40]);
	}

	#[test]
	fn slice_preserves_nullability_and_bitmap() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 4);
		cd.push_none();
		cd.push::<i32>(20);
		cd.push_none();
		cd.push::<i32>(40);
		let cd = cd.finish();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let out = slice(&ca, 1, 4).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.buffer.nulls().unwrap();
		assert!(!nones.is_null(0));
		assert!(nones.is_null(1));
		assert!(!nones.is_null(2));
	}

	#[test]
	fn slice_dictionary_keeps_dictionary_id() {
		// Without copying the id, a sliced dictionary column has none and can no longer be decoded.
		let ca = Canonical::from_column_buffer(&dictionary_column()).unwrap();
		let out = slice(&ca, 1, 3).unwrap();
		assert_eq!(kept_dictionary_id(&out.buffer), Some(DictionaryId(42)));
		assert_eq!(out.len(), 2);
		assert_eq!(out.buffer.get_value(0), Value::DictionaryId(DictionaryEntryId::U4(2)));
		assert_eq!(out.buffer.get_value(1), Value::DictionaryId(DictionaryEntryId::U4(3)));
	}
}
