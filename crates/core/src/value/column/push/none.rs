// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use reifydb_type::{storage::DataBitVec, util::bitvec::BitVec};

use crate::value::column::buffer::{ColumnBuffer, with_container};

impl ColumnBuffer {
	pub fn push_none(&mut self) {
		match self {
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => {
				// Push a default value to the inner container (not recursive promotion)
				with_container!(inner.as_mut(), |c| c.push_default());
				DataBitVec::push(bitvec, false);
			}
			_ => {
				// Promote bare container to Option-wrapped, then push none
				let len = self.len();
				let mut bitvec = BitVec::repeat(len, true);
				let mut inner = mem::replace(self, ColumnBuffer::bool(vec![]));
				// Push a default value to the inner container directly (avoid recursion)
				with_container!(&mut inner, |c| c.push_default());
				DataBitVec::push(&mut bitvec, false);
				*self = ColumnBuffer::Option {
					inner: Box::new(inner),
					bitvec,
				};
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_type::value::{dictionary::DictionaryEntryId, identity::IdentityId, r#type::Type};

	use crate::value::column::ColumnBuffer;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_bool() {
		let mut col = ColumnBuffer::bool(vec![true]);
		col.push_none();
		// push_none promotes a bare column to Option-wrapped
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnBuffer::float4(vec![1.0]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnBuffer::float8(vec![1.0]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnBuffer::int1(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnBuffer::int2(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnBuffer::int4(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnBuffer::int8(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnBuffer::int16(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_string() {
		let mut col = ColumnBuffer::utf8(vec!["a"]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnBuffer::uint1(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnBuffer::uint2(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnBuffer::uint4(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnBuffer::uint8(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnBuffer::uint16(vec![1]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let mut col = ColumnBuffer::identity_id(vec![IdentityId::generate(&clock, &rng)]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_dictionary_id() {
		let mut col = ColumnBuffer::dictionary_id(vec![DictionaryEntryId::U4(10)]);
		col.push_none();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_none_on_option() {
		let mut col = ColumnBuffer::none_typed(Type::Boolean, 5);
		col.push_none();
		assert_eq!(col.len(), 6);
		assert!(!col.is_defined(0));
		assert!(!col.is_defined(5));
	}
}
