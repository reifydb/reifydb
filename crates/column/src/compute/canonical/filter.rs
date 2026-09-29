// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::BooleanArray;
use arrow_buffer::BooleanBuffer;
use arrow_select::filter::filter as filter_array;
use reifydb_core::{error::CoreError, value::column::data::canonical::Canonical};
use reifydb_value::Result;

pub fn filter(array: &Canonical, mask: &BooleanBuffer) -> Result<Canonical> {
	assert_eq!(array.len(), mask.len(), "filter: array len {} vs mask len {}", array.len(), mask.len());

	let filtered =
		filter_array(array.buffer().as_ref(), &BooleanArray::new(mask.clone(), None)).map_err(|error| {
			CoreError::FrameError {
				message: error.to_string(),
			}
		})?;

	Canonical::new(array.field_type().clone(), filtered)
}

#[cfg(test)]
mod tests {
	use arrow_array::Array;
	use reifydb_core::value::column::{builder::ColumnBuilder, factory};
	use reifydb_value::value::{
		Value,
		column_view::ViewData,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::{ValueType, field::from_field},
	};

	use super::*;

	fn dictionary_column() -> Canonical {
		let column = factory::dictionary_id(
			"c",
			[DictionaryEntryId::U4(1), DictionaryEntryId::U4(2), DictionaryEntryId::U4(3)],
		);
		let mut field_type = from_field(&column.0).unwrap();
		field_type.dictionary_id = Some(DictionaryId(42));
		Canonical::new(field_type, column.1).unwrap()
	}

	fn kept_dictionary_id(canonical: &Canonical) -> Option<DictionaryId> {
		let ViewData::DictionaryId {
			dictionary_id,
			..
		} = canonical.view().data
		else {
			panic!("expected a DictionaryId column, got {:?}", canonical.view().get_type());
		};
		dictionary_id
	}

	#[test]
	fn filter_keeps_selected_int4_rows() {
		let ca = Canonical::from_column(&factory::int4("c", [10i32, 20, 30, 40, 50])).unwrap();
		let mask = BooleanBuffer::from(vec![false, true, false, true, false]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 2);
		let ViewData::Int4(values) = out.view().data else {
			panic!("expected an Int4 column, got {:?}", out.view().get_type());
		};
		assert_eq!(&values.values()[..], &[20, 40]);
	}

	#[test]
	fn filter_preserves_none_bitmap_alignment() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 5);
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(50);
		let ca = Canonical::from_column(&cd.finish("c")).unwrap();
		let mask = BooleanBuffer::from(vec![true, true, false, true, false]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(out.len(), 3);
		assert!(out.view().is_nullable());
		let nones = out.buffer().logical_nulls().unwrap();
		assert!(!nones.is_null(0));
		assert!(nones.is_null(1));
		assert!(nones.is_null(2));
	}

	#[test]
	fn filter_dictionary_keeps_dictionary_id() {
		// Without copying the id, a filtered dictionary column has none and can no longer be decoded.
		let ca = dictionary_column();
		let mask = BooleanBuffer::from(vec![true, false, true]);
		let out = filter(&ca, &mask).unwrap();
		assert_eq!(kept_dictionary_id(&out), Some(DictionaryId(42)));
		assert_eq!(out.len(), 2);
		assert_eq!(out.view().get_value(0), Value::DictionaryId(DictionaryEntryId::U4(1)));
		assert_eq!(out.view().get_value(1), Value::DictionaryId(DictionaryEntryId::U4(3)));
	}
}
