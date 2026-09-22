// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_core::value::column::data::canonical::Canonical;
use reifydb_value::Result;

pub fn filter(array: &Canonical, mask: &BooleanBuffer) -> Result<Canonical> {
	assert_eq!(array.len(), mask.len(), "filter: array len {} vs mask len {}", array.len(), mask.len());

	let mut new_buffer = array.buffer.clone();
	new_buffer.filter(mask)?;

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
	fn filter_keeps_selected_int4_rows() {
		let cd = ColumnBuffer::int4([10i32, 20, 30, 40, 50]);
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let mask = BooleanBuffer::from(vec![false, true, false, true, false]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 2);
		assert_eq!(out.buffer.as_slice::<i32>(), &[20, 40]);
	}

	#[test]
	fn filter_preserves_none_bitmap_alignment() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 5);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(50);
		let cd = cd.finish();
		let ca = Canonical::from_column_buffer(&cd).unwrap();
		let mask = BooleanBuffer::from(vec![true, true, false, true, false]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.nullable);
		let nones = out.buffer.nulls().unwrap();
		assert!(!nones.is_null(0));
		assert!(nones.is_null(1));
		assert!(nones.is_null(2));
	}

	#[test]
	fn filter_dictionary_keeps_dictionary_id() {
		// Without copying the id, a filtered dictionary column has none and can no longer be decoded.
		let ca = Canonical::from_column_buffer(&dictionary_column()).unwrap();
		let mask = BooleanBuffer::from(vec![true, false, true]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(kept_dictionary_id(&out.buffer), Some(DictionaryId(42)));
		assert_eq!(out.len(), 2);
		assert_eq!(out.buffer.get_value(0), Value::DictionaryId(DictionaryEntryId::U4(1)));
		assert_eq!(out.buffer.get_value(1), Value::DictionaryId(DictionaryEntryId::U4(3)));
	}
}
