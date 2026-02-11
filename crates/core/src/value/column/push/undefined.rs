// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::value::column::data::{ColumnData, with_container};

impl ColumnData {
	pub fn push_undefined(&mut self) {
		with_container!(self, |c| c.push_undefined())
	}
}

#[cfg(test)]
pub mod tests {
	use crate::value::column::ColumnData;

	#[test]
	fn test_bool() {
		let mut col = ColumnData::bool(vec![true]);
		col.push_undefined();
		let ColumnData::Bool(container) = col else {
			panic!("Expected Bool");
		};

		assert_eq!(container.data().to_vec(), vec![true, false]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnData::float4(vec![1.0]);
		col.push_undefined();
		let ColumnData::Float4(container) = col else {
			panic!("Expected Float4");
		};

		assert_eq!(container.data().as_slice(), &[1.0, 0.0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnData::float8(vec![1.0]);
		col.push_undefined();
		let ColumnData::Float8(container) = col else {
			panic!("Expected Float8");
		};

		assert_eq!(container.data().as_slice(), &[1.0, 0.0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnData::int1(vec![1]);
		col.push_undefined();
		let ColumnData::Int1(container) = col else {
			panic!("Expected Int1");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_undefined();
		let ColumnData::Int2(container) = col else {
			panic!("Expected Int2");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnData::int4(vec![1]);
		col.push_undefined();
		let ColumnData::Int4(container) = col else {
			panic!("Expected Int4");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnData::int8(vec![1]);
		col.push_undefined();
		let ColumnData::Int8(container) = col else {
			panic!("Expected Int8");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnData::int16(vec![1]);
		col.push_undefined();
		let ColumnData::Int16(container) = col else {
			panic!("Expected Int16");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_string() {
		let mut col = ColumnData::utf8(vec!["a"]);
		col.push_undefined();
		let ColumnData::Utf8 {
			container,
			..
		} = col
		else {
			panic!("Expected Utf8");
		};

		assert_eq!(container.data().as_slice(), &["a".to_string(), "".to_string()]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnData::uint1(vec![1]);
		col.push_undefined();
		let ColumnData::Uint1(container) = col else {
			panic!("Expected Uint1");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnData::uint2(vec![1]);
		col.push_undefined();
		let ColumnData::Uint2(container) = col else {
			panic!("Expected Uint2");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnData::uint4(vec![1]);
		col.push_undefined();
		let ColumnData::Uint4(container) = col else {
			panic!("Expected Uint4");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnData::uint8(vec![1]);
		col.push_undefined();
		let ColumnData::Uint8(container) = col else {
			panic!("Expected Uint8");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnData::uint16(vec![1]);
		col.push_undefined();
		let ColumnData::Uint16(container) = col else {
			panic!("Expected Uint16");
		};

		assert_eq!(container.data().as_slice(), &[1, 0]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_identity_id() {
		use reifydb_type::value::identity::IdentityId;

		let id1 = IdentityId::generate();
		let mut col = ColumnData::identity_id(vec![id1]);
		col.push_undefined();
		let ColumnData::IdentityId(container) = col else {
			panic!("Expected IdentityId");
		};

		assert_eq!(container.data().as_slice(), &[id1, IdentityId::default()]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_dictionary_id() {
		use reifydb_type::value::dictionary::DictionaryEntryId;

		let e1 = DictionaryEntryId::U4(10);
		let mut col = ColumnData::dictionary_id(vec![e1]);
		col.push_undefined();
		let ColumnData::DictionaryId(container) = col else {
			panic!("Expected DictionaryId");
		};

		assert_eq!(container.data().as_slice(), &[e1, DictionaryEntryId::default()]);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
	}

	#[test]
	fn test_undefined() {
		let mut col = ColumnData::undefined(5);
		col.push_undefined();
		let ColumnData::Undefined(container) = col else {
			panic!("Expected Undefined");
		};

		assert_eq!(container.len(), 6);
	}
}
