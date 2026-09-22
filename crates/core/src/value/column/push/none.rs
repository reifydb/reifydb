// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use arrow_buffer::BooleanBufferBuilder;

use crate::value::column::builder::ColumnBuilder;

impl ColumnBuilder {
	pub fn push_none(&mut self) {
		match self {
			ColumnBuilder::Option {
				inner,
				bitvec,
			} => {
				inner.push_default();
				bitvec.append(false);
			}
			_ => {
				let len = self.len();
				let mut bitvec = BooleanBufferBuilder::new(len + 1);
				bitvec.append_n(len, true);
				let mut inner = mem::replace(self, ColumnBuilder::Bool(BooleanBufferBuilder::new(0)));

				inner.push_default();
				bitvec.append(false);
				*self = ColumnBuilder::Option {
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
	use reifydb_value::value::{dictionary::DictionaryEntryId, identity::IdentityId, value_type::ValueType};

	use crate::value::column::ColumnBuffer;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_bool() {
		let mut col = ColumnBuffer::bool(vec![true]).into_builder();
		col.push_none();
		// push_none promotes a bare column to Option-wrapped
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float4() {
		let mut col = ColumnBuffer::float4(vec![1.0]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float8() {
		let mut col = ColumnBuffer::float8(vec![1.0]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int1() {
		let mut col = ColumnBuffer::int1(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int2() {
		let mut col = ColumnBuffer::int2(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int4() {
		let mut col = ColumnBuffer::int4(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int8() {
		let mut col = ColumnBuffer::int8(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int16() {
		let mut col = ColumnBuffer::int16(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_string() {
		let mut col = ColumnBuffer::utf8(vec!["a"]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint1() {
		let mut col = ColumnBuffer::uint1(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint2() {
		let mut col = ColumnBuffer::uint2(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint4() {
		let mut col = ColumnBuffer::uint4(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint8() {
		let mut col = ColumnBuffer::uint8(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint16() {
		let mut col = ColumnBuffer::uint16(vec![1]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let mut col = ColumnBuffer::identity_id(vec![IdentityId::generate(&clock, &rng)]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_dictionary_id() {
		let mut col = ColumnBuffer::dictionary_id(vec![DictionaryEntryId::U4(10)]).into_builder();
		col.push_none();
		let col = col.finish();
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_none_on_option() {
		let mut col = ColumnBuffer::none_typed(ValueType::Boolean, 5).into_builder();
		col.push_none();
		let col = col.finish();
		assert_eq!(col.len(), 6);
		assert!(!col.is_defined(0));
		assert!(!col.is_defined(5));
	}
}
