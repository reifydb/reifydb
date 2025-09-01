// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::columnar::ColumnData;

impl ColumnData {
	pub fn reorder(&mut self, indices: &[usize]) {
		match self {
			ColumnData::Bool(container) => {
				container.reorder(indices)
			}
			ColumnData::Float4(container) => {
				container.reorder(indices)
			}
			ColumnData::Float8(container) => {
				container.reorder(indices)
			}
			ColumnData::Int1(container) => {
				container.reorder(indices)
			}
			ColumnData::Int2(container) => {
				container.reorder(indices)
			}
			ColumnData::Int4(container) => {
				container.reorder(indices)
			}
			ColumnData::Int8(container) => {
				container.reorder(indices)
			}
			ColumnData::Int16(container) => {
				container.reorder(indices)
			}
			ColumnData::Utf8(container) => {
				container.reorder(indices)
			}
			ColumnData::Uint1(container) => {
				container.reorder(indices)
			}
			ColumnData::Uint2(container) => {
				container.reorder(indices)
			}
			ColumnData::Uint4(container) => {
				container.reorder(indices)
			}
			ColumnData::Uint8(container) => {
				container.reorder(indices)
			}
			ColumnData::Uint16(container) => {
				container.reorder(indices)
			}
			ColumnData::Date(container) => {
				container.reorder(indices)
			}
			ColumnData::DateTime(container) => {
				container.reorder(indices)
			}
			ColumnData::Time(container) => {
				container.reorder(indices)
			}
			ColumnData::Interval(container) => {
				container.reorder(indices)
			}
			ColumnData::Undefined(container) => {
				container.reorder(indices)
			}
			ColumnData::RowNumber(container) => {
				container.reorder(indices)
			}
			ColumnData::IdentityId(container) => {
				container.reorder(indices)
			}
			ColumnData::Uuid4(container) => {
				container.reorder(indices)
			}
			ColumnData::Uuid7(container) => {
				container.reorder(indices)
			}
			ColumnData::Blob(container) => {
				container.reorder(indices)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{Value, value::columnar::ColumnData};

	#[test]
	fn test_reorder_bool() {
		let mut col = ColumnData::bool([true, false, true]);
		col.reorder(&[2, 0, 1]);

		assert_eq!(col.len(), 3);
		assert_eq!(col.get_value(0), Value::Bool(true));
		assert_eq!(col.get_value(1), Value::Bool(true));
		assert_eq!(col.get_value(2), Value::Bool(false));
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
		let mut col = ColumnData::utf8([
			"a".to_string(),
			"b".to_string(),
			"c".to_string(),
		]);
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
		use reifydb_type::IdentityId;

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
}
