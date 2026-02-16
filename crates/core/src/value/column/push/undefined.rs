// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, util::bitvec::BitVec};

use crate::value::column::data::{ColumnData, with_container};

impl ColumnData {
	pub fn push_undefined(&mut self) {
		match self {
			ColumnData::Option {
				inner,
				bitvec,
			} => {
				// Push a default value to the inner container (not recursive promotion)
				with_container!(inner.as_mut(), |c| c.push_undefined());
				DataBitVec::push(bitvec, false);
			}
			ColumnData::Undefined(c) => c.push_undefined(),
			_ => {
				// Promote bare container to Option-wrapped, then push undefined
				let len = self.len();
				let mut bitvec = BitVec::repeat(len, true);
				let mut inner = std::mem::replace(self, ColumnData::undefined(0));
				// Push a default value to the inner container directly (avoid recursion)
				with_container!(&mut inner, |c| c.push_undefined());
				DataBitVec::push(&mut bitvec, false);
				*self = ColumnData::Option {
					inner: Box::new(inner),
					bitvec,
				};
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use crate::value::column::ColumnData;

	#[test]
	fn test_bool() {
		let mut col = ColumnData::bool(vec![true]);
		col.push_undefined();
		// push_undefined promotes a bare column to Option-wrapped
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnData::float4(vec![1.0]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnData::float8(vec![1.0]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnData::int1(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnData::int2(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnData::int4(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnData::int8(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnData::int16(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_string() {
		let mut col = ColumnData::utf8(vec!["a"]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnData::uint1(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnData::uint2(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnData::uint4(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnData::uint8(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnData::uint16(vec![1]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_identity_id() {
		let mut col = ColumnData::identity_id(vec![reifydb_type::value::identity::IdentityId::generate()]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_dictionary_id() {
		use reifydb_type::value::dictionary::DictionaryEntryId;

		let mut col = ColumnData::dictionary_id(vec![DictionaryEntryId::U4(10)]);
		col.push_undefined();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
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
