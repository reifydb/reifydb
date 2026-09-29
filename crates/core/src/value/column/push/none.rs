// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::value::column::builder::ColumnBuilder;

impl ColumnBuilder {
	pub fn push_none(&mut self) {
		self.inner.append_null();
		self.optional = true;
	}
}

#[cfg(test)]
pub mod tests {
	use arrow_array::ArrayRef;
	use arrow_schema::FieldRef;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_value::value::{
		column_view::ColumnView, dictionary::DictionaryEntryId, identity::IdentityId, value_type::ValueType,
	};

	use crate::value::column::{builder::ColumnBuilder, factory};

	fn builder_of(column: (FieldRef, ArrayRef)) -> ColumnBuilder {
		ColumnBuilder::from_view(&ColumnView::try_from(&column).unwrap())
	}

	fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
		ColumnView::try_from(column).unwrap()
	}

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_bool() {
		let mut col = builder_of(factory::bool("c", vec![true]));
		col.push_none();
		// push_none must add a null row and make the bare column optional, never a default value.
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float4() {
		let mut col = builder_of(factory::float4("c", vec![1.0]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_float8() {
		let mut col = builder_of(factory::float8("c", vec![1.0]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int1() {
		let mut col = builder_of(factory::int1("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int2() {
		let mut col = builder_of(factory::int2("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int4() {
		let mut col = builder_of(factory::int4("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int8() {
		let mut col = builder_of(factory::int8("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_int16() {
		let mut col = builder_of(factory::int16("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_string() {
		let mut col = builder_of(factory::utf8("c", vec!["a"]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint1() {
		let mut col = builder_of(factory::uint1("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint2() {
		let mut col = builder_of(factory::uint2("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint4() {
		let mut col = builder_of(factory::uint4("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint8() {
		let mut col = builder_of(factory::uint8("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_uint16() {
		let mut col = builder_of(factory::uint16("c", vec![1]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let mut col = builder_of(factory::identity_id("c", vec![IdentityId::generate(&clock, &rng)]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_dictionary_id() {
		let mut col = builder_of(factory::dictionary_id("c", vec![DictionaryEntryId::U4(10)]));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
		assert_eq!(col.len(), 2);
	}

	#[test]
	fn test_none_on_option() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Boolean, 5));
		col.push_none();
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 6);
		assert!(!col.is_defined(0));
		assert!(!col.is_defined(5));
	}
}
