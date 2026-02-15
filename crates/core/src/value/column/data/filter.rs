// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::util::bitvec::BitVec;

use crate::value::column::{Column, ColumnData, data::with_container};

impl Column {
	pub fn filter(&mut self, mask: &BitVec) -> reifydb_type::Result<()> {
		self.data_mut().filter(mask)
	}
}

impl ColumnData {
	pub fn filter(&mut self, mask: &BitVec) -> reifydb_type::Result<()> {
		with_container!(self, |c| c.filter(mask));
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::{util::bitvec::BitVec, value::Value};

	use crate::value::column::ColumnData;

	#[test]
	fn test_filter_bool() {
		let mut col = ColumnData::bool([true, false, true, false]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::Boolean(true));
		assert_eq!(col.get_value(1), Value::Boolean(true));
	}

	#[test]
	fn test_filter_int4() {
		let mut col = ColumnData::int4([1, 2, 3, 4, 5]);
		let mask = BitVec::from_slice(&[true, false, true, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Int4(1));
		assert_eq!(col.get_value(1), Value::Int4(3));
		assert_eq!(col.get_value(2), Value::Int4(5));
	}

	#[test]
	fn test_filter_float4() {
		let mut col = ColumnData::float4([1.0, 2.0, 3.0, 4.0]);
		let mask = BitVec::from_slice(&[false, true, false, true]);

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
		let mut col = ColumnData::utf8(["a", "b", "c", "d"]);
		let mask = BitVec::from_slice(&[true, false, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::Utf8("a".to_string()));
		assert_eq!(col.get_value(1), Value::Utf8("d".to_string()));
	}

	#[test]
	fn test_filter_undefined() {
		let mut col = ColumnData::undefined(5);
		let mask = BitVec::from_slice(&[true, false, true, false, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::None);
		assert_eq!(col.get_value(1), Value::None);
	}

	#[test]
	fn test_filter_empty_mask() {
		let mut col = ColumnData::int4([1, 2, 3]);
		let mask = BitVec::from_slice(&[false, false, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 0);
	}

	#[test]
	fn test_filter_all_true_mask() {
		let mut col = ColumnData::int4([1, 2, 3]);
		let mask = BitVec::from_slice(&[true, true, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Int4(1));
		assert_eq!(col.get_value(1), Value::Int4(2));
		assert_eq!(col.get_value(2), Value::Int4(3));
	}

	#[test]
	fn test_filter_identity_id() {
		use reifydb_type::value::identity::IdentityId;

		let id1 = IdentityId::generate();
		let id2 = IdentityId::generate();
		let id3 = IdentityId::generate();
		let id4 = IdentityId::generate();

		let mut col = ColumnData::identity_id([id1, id2, id3, id4]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::IdentityId(id1));
		assert_eq!(col.get_value(1), Value::IdentityId(id3));
	}

	#[test]
	fn test_filter_dictionary_id() {
		use reifydb_type::value::dictionary::DictionaryEntryId;

		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);
		let e3 = DictionaryEntryId::U4(30);
		let e4 = DictionaryEntryId::U4(40);

		let mut col = ColumnData::dictionary_id([e1, e2, e3, e4]);
		let mask = BitVec::from_slice(&[true, false, true, false]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 2);
		assert_eq!(col.get_value(0), Value::DictionaryId(e1));
		assert_eq!(col.get_value(1), Value::DictionaryId(e3));
	}

	#[test]
	fn test_filter_dictionary_id_with_undefined() {
		use reifydb_type::value::dictionary::DictionaryEntryId;

		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);

		let mut col = ColumnData::dictionary_id_with_bitvec(
			[e1, DictionaryEntryId::default(), e2, DictionaryEntryId::default()],
			BitVec::from_slice(&[true, false, true, false]),
		);
		let mask = BitVec::from_slice(&[true, true, false, true]);

		col.filter(&mask).unwrap();

		assert_eq!(col.len(), 3);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert!(!col.is_defined(2));
		assert_eq!(col.get_value(0), Value::DictionaryId(e1));
	}
}
