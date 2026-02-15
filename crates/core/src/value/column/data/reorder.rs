// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::storage::DataBitVec;

use crate::value::column::{ColumnData, data::with_container};

impl ColumnData {
	pub fn reorder(&mut self, indices: &[usize]) {
		match self {
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				inner.reorder(indices);
				let mut new_bitvec = DataBitVec::spawn(bitvec, indices.len());
				for &idx in indices {
					if idx < DataBitVec::len(bitvec) {
						DataBitVec::push(&mut new_bitvec, DataBitVec::get(bitvec, idx));
					} else {
						DataBitVec::push(&mut new_bitvec, false);
					}
				}
				*bitvec = new_bitvec;
			}
			_ => with_container!(self, |c| c.reorder(indices)),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{Value, identity::IdentityId};

	use crate::value::column::ColumnData;

	#[test]
	fn test_reorder_bool() {
		let mut col = ColumnData::bool([true, false, true]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Boolean(true));
		assert_eq!(col.get_value(1), Value::Boolean(true));
		assert_eq!(col.get_value(2), Value::Boolean(false));
	}

	#[test]
	fn test_reorder_float4() {
		let mut col = ColumnData::float4([1.0, 2.0, 3.0]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		// Check data after reordering
		match col.get_value(0) {
			Value::Float4(v) => assert_eq!(v.value(), 3.0),
			_ => panic!("Expected Float4"),
		}
		match col.get_value(1) {
			Value::Float4(v) => assert_eq!(v.value(), 1.0),
			_ => panic!("Expected Float4"),
		}
		match col.get_value(2) {
			Value::Float4(v) => assert_eq!(v.value(), 2.0),
			_ => panic!("Expected Float4"),
		}
	}

	#[test]
	fn test_reorder_int4() {
		let mut col = ColumnData::int4([1, 2, 3]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Int4(3));
		assert_eq!(col.get_value(1), Value::Int4(1));
		assert_eq!(col.get_value(2), Value::Int4(2));
	}

	#[test]
	fn test_reorder_string() {
		let mut col = ColumnData::utf8(["a".to_string(), "b".to_string(), "c".to_string()]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Utf8("c".to_string()));
		assert_eq!(col.get_value(1), Value::Utf8("a".to_string()));
		assert_eq!(col.get_value(2), Value::Utf8("b".to_string()));
	}

	#[test]
	fn test_reorder_undefined() {
		let mut col = ColumnData::undefined(3);
		col.reorder(&[2, 0, 1]);
		assert_eq!(col.len(), 3);

		col.reorder(&[1, 0]);
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_reorder_identity_id() {
		let id1 = IdentityId::generate();
		let id2 = IdentityId::generate();
		let id3 = IdentityId::generate();

		let mut col = ColumnData::identity_id([id1, id2, id3]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::IdentityId(id3));
		assert_eq!(col.get_value(1), Value::IdentityId(id1));
		assert_eq!(col.get_value(2), Value::IdentityId(id2));
	}

	#[test]
	fn test_reorder_dictionary_id() {
		use reifydb_type::value::dictionary::DictionaryEntryId;

		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);
		let e3 = DictionaryEntryId::U4(30);

		let mut col = ColumnData::dictionary_id([e1, e2, e3]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::DictionaryId(e3));
		assert_eq!(col.get_value(1), Value::DictionaryId(e1));
		assert_eq!(col.get_value(2), Value::DictionaryId(e2));
	}
}
