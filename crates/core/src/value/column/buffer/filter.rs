// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use arrow_select::filter::FilterPredicate;
use reifydb_value::{
	Result,
	util::kernel,
	value::container::{bool_array, fixed_array, primitive, varlen_array},
};

use crate::value::column::{
	ColumnBuffer,
	buffer::{map_decimal, with_container},
};

impl ColumnBuffer {
	pub fn filter(&mut self, mask: &BooleanBuffer) -> Result<()> {
		self.filter_with(&kernel::predicate(mask, self.len()))
	}

	pub fn filter_with(&mut self, predicate: &FilterPredicate) -> Result<()> {
		match self {
			ColumnBuffer::Bool(a) => *a = bool_array::filter_with(a, predicate),
			ColumnBuffer::Uint16(a) => *a = primitive::filter_with(a, predicate),
			ColumnBuffer::Decimal(d) => *d = map_decimal!(&*d, |a| primitive::filter_with(a, predicate)),
			_ => with_container!(
				self,
				|a| *a = primitive::filter_with(a, predicate),
				|t| *t = primitive::filter_with(t, predicate),
				|u| *u = fixed_array::filter_with(u, predicate),
				|v| *v = varlen_array::filter_with(v, predicate)
			),
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use arrow_buffer::{BooleanBuffer, NullBuffer};
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_value::value::{Value, dictionary::DictionaryEntryId, identity::IdentityId, value_type::ValueType};

	use crate::value::column::ColumnBuffer;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_filter_bool() {
		let mut col = ColumnBuffer::bool([true, false, true, false]);
		let mask = BooleanBuffer::from(vec![true, false, true, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::Boolean(true));
		assert_eq!(col.get_value(1), Value::Boolean(true));
	}

	#[test]
	fn test_filter_int4() {
		let mut col = ColumnBuffer::int4([1, 2, 3, 4, 5]);
		let mask = BooleanBuffer::from(vec![true, false, true, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Int4(1));
		assert_eq!(col.get_value(1), Value::Int4(3));
		assert_eq!(col.get_value(2), Value::Int4(5));
	}

	#[test]
	fn test_filter_float4() {
		let mut col = ColumnBuffer::float4([1.0, 2.0, 3.0, 4.0]);
		let mask = BooleanBuffer::from(vec![false, true, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		match col.get_value(0) {
			Value::Float4(v) => assert_eq!(v.value(), 2.0),
			_ => panic!("Expected Float4"),
		}
		match col.get_value(1) {
			Value::Float4(v) => assert_eq!(v.value(), 4.0),
			_ => panic!("Expected Float4"),
		}
	}

	#[test]
	fn test_filter_string() {
		let mut col = ColumnBuffer::utf8(["a", "b", "c", "d"]);
		let mask = BooleanBuffer::from(vec![true, false, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::Utf8("a".to_string()));
		assert_eq!(col.get_value(1), Value::Utf8("d".to_string()));
	}

	#[test]
	fn test_filter_none() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 5);
		let mask = BooleanBuffer::from(vec![true, false, true, false, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::none_of(ValueType::Boolean));
		assert_eq!(col.get_value(1), Value::none_of(ValueType::Boolean));
	}

	#[test]
	fn test_filter_empty_mask() {
		let mut col = ColumnBuffer::int4([1, 2, 3]);
		let mask = BooleanBuffer::from(vec![false, false, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 0);
	}

	#[test]
	fn test_filter_all_true_mask() {
		let mut col = ColumnBuffer::int4([1, 2, 3]);
		let mask = BooleanBuffer::from(vec![true, true, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Int4(1));
		assert_eq!(col.get_value(1), Value::Int4(2));
		assert_eq!(col.get_value(2), Value::Int4(3));
	}

	#[test]
	fn test_filter_identity_id() {
		let (mock, clock, rng) = test_clock_and_rng();
		let id1 = IdentityId::generate(&clock, &rng);
		mock.advance_millis(1);
		let id2 = IdentityId::generate(&clock, &rng);
		mock.advance_millis(1);
		let id3 = IdentityId::generate(&clock, &rng);
		mock.advance_millis(1);
		let id4 = IdentityId::generate(&clock, &rng);

		let mut col = ColumnBuffer::identity_id([id1, id2, id3, id4]);
		let mask = BooleanBuffer::from(vec![true, false, true, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::IdentityId(id1));
		assert_eq!(col.get_value(1), Value::IdentityId(id3));
	}

	#[test]
	fn test_filter_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);
		let e3 = DictionaryEntryId::U4(30);
		let e4 = DictionaryEntryId::U4(40);

		let mut col = ColumnBuffer::dictionary_id([e1, e2, e3, e4]);
		let mask = BooleanBuffer::from(vec![true, false, true, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::DictionaryId(e1));
		assert_eq!(col.get_value(1), Value::DictionaryId(e3));
	}

	#[test]
	fn test_filter_dictionary_id_with_undefined() {
		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);

		let mut col = ColumnBuffer::dictionary_id_with_bitvec(
			[e1, DictionaryEntryId::default(), e2, DictionaryEntryId::default()],
			BooleanBuffer::from(vec![true, false, true, false]),
		);
		let mask = BooleanBuffer::from(vec![true, true, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 3);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert!(!col.is_defined(2));
		assert_eq!(col.get_value(0), Value::DictionaryId(e1));
	}
	#[test]
	fn filter_with_a_mask_longer_than_the_column_ignores_the_extra_bits() {
		// A mask is sized for the batch, not for the column, so the bits past the end must select nothing
		// instead of failing the kernel.
		let mut col = ColumnBuffer::int4([1, 2, 3]);
		let mask = BooleanBuffer::from(vec![true, false, true, true, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::Int4(1));
		assert_eq!(col.get_value(1), Value::Int4(3));
	}

	#[test]
	fn filter_with_a_mask_shorter_than_the_column_drops_the_tail() {
		// The rows past the end of the mask are unselected, never kept by default.
		let mut col = ColumnBuffer::int4([1, 2, 3, 4]);
		let mask = BooleanBuffer::from(vec![true, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::Int4(1));
		assert_eq!(col.get_value(1), Value::Int4(2));
	}

	#[test]
	fn filter_keeps_an_all_valid_column_nullable() {
		// Arrow drops a null buffer whose bits are all valid; without putting it back the column type flips
		// from Option to bare.
		let mut col = ColumnBuffer::int4([1, 2, 3]).replace_nulls(Some(NullBuffer::new_valid(3)));
		assert_eq!(col.get_type(), ValueType::Option(Box::new(ValueType::Int4)));

		col.filter(&BooleanBuffer::from(vec![true, false, true])).unwrap();

		assert!(col.nulls().is_some());
		assert_eq!(col.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	}

	#[test]
	fn filter_with_an_all_false_mask_keeps_the_type_and_nullability() {
		// An all-false mask makes arrow return a fresh empty array that carries neither the null buffer nor
		// anything else of the source.
		let mut col = ColumnBuffer::utf8_with_bitvec(["a", "b"], BooleanBuffer::from(vec![true, false]));

		col.filter(&BooleanBuffer::from(vec![false, false])).unwrap();

		assert_eq!(col.len(), 0);
		assert!(col.nulls().is_some());
		assert_eq!(col.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
	}

	#[test]
	fn filter_keeps_the_placeholder_bytes_under_a_none_row() {
		// Serde bytes and equality see what is stored under a none row, so the filter must move those bytes
		// across unchanged.
		let mut col =
			ColumnBuffer::utf8_with_bitvec(["keep", "hidden"], BooleanBuffer::from(vec![true, false]));

		col.filter(&BooleanBuffer::from(vec![true, true])).unwrap();

		let ColumnBuffer::Utf8 {
			container,
			..
		} = &col
		else {
			panic!("expected a utf8 column");
		};
		assert_eq!(container.value(1), "hidden");
		assert_eq!(col.get_value(1), Value::none_of(ValueType::Utf8));
	}
}
