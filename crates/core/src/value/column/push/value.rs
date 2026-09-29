// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	Value,
	container::{
		any_array::push_any,
		dictionary_array,
		digest_array::push_digest,
		temporal_array::{date_to_native, datetime_to_native, duration_to_native, time_to_native},
	},
};

use crate::value::column::builder::{ColumnBuilder, TypedBuilder, append_fixed};

macro_rules! push_or_promote {
	(native $self:expr, $val:expr, $col_variant:ident) => {
		match &mut $self.inner {
			TypedBuilder::$col_variant(builder) => builder.append_value($val),
			_ => unimplemented!(),
		}
	};

	(temporal $self:expr, $val:expr, $col_variant:ident, $to_native:ident) => {
		match &mut $self.inner {
			TypedBuilder::$col_variant(builder) => builder.append_value($to_native($val)),
			_ => unimplemented!(),
		}
	};

	(fixed $self:expr, $val:expr, $col_variant:ident) => {
		match &mut $self.inner {
			TypedBuilder::$col_variant(builder) => append_fixed(builder, $val.as_bytes()),
			_ => unimplemented!(),
		}
	};

	(varlen $self:expr, $val:expr, $col_variant:ident) => {
		match &mut $self.inner {
			TypedBuilder::$col_variant {
				builder,
				..
			} => builder.append_value($val),
			_ => unimplemented!(),
		}
	};

	(decimal $self:expr, $val:expr, $col_variant:ident) => {
		match &mut $self.inner {
			TypedBuilder::$col_variant(builder) => builder.push(&$val),
			_ => unimplemented!(),
		}
	};
}

impl ColumnBuilder {
	pub fn push_value(&mut self, value: Value) {
		if matches!(self.inner, TypedBuilder::None(_)) && !matches!(value, Value::None { .. }) {
			panic!("the untyped none column can not take a value of type {:?}", value.get_type());
		}
		match value {
			Value::Boolean(v) => match &mut self.inner {
				TypedBuilder::Bool(builder) => builder.append_value(v),
				_ => unimplemented!(),
			},
			Value::Float4(v) => push_or_promote!(native self, v.value(), Float4),
			Value::Float8(v) => push_or_promote!(native self, v.value(), Float8),
			Value::Int1(v) => push_or_promote!(native self, v, Int1),
			Value::Int2(v) => push_or_promote!(native self, v, Int2),
			Value::Int4(v) => push_or_promote!(native self, v, Int4),
			Value::Int8(v) => push_or_promote!(native self, v, Int8),
			Value::Int16(v) => push_or_promote!(native self, v, Int16),
			Value::Uint1(v) => push_or_promote!(native self, v, Uint1),
			Value::Uint2(v) => push_or_promote!(native self, v, Uint2),
			Value::Uint4(v) => push_or_promote!(native self, v, Uint4),
			Value::Uint8(v) => push_or_promote!(native self, v, Uint8),
			Value::Uint16(v) => push_or_promote!(native self, v, Uint16),
			Value::Utf8(v) => push_or_promote!(varlen self, v, Utf8),
			Value::Date(v) => push_or_promote!(temporal self, v, Date, date_to_native),
			Value::DateTime(v) => push_or_promote!(temporal self, v, DateTime, datetime_to_native),
			Value::Time(v) => push_or_promote!(temporal self, v, Time, time_to_native),
			Value::Duration(v) => push_or_promote!(temporal self, v, Duration, duration_to_native),
			Value::Uuid4(v) => push_or_promote!(fixed self, v, Uuid4),
			Value::Uuid7(v) => push_or_promote!(fixed self, v, Uuid7),
			Value::IdentityId(v) => push_or_promote!(fixed self, v, IdentityId),
			Value::DictionaryId(v) => match &mut self.inner {
				TypedBuilder::DictionaryId {
					builder,
					..
				} => append_fixed(builder, &dictionary_array::encode(v)),
				_ => unimplemented!(),
			},
			Value::Blob(v) => push_or_promote!(varlen self, v.as_bytes(), Blob),
			Value::Decimal(v) => push_or_promote!(decimal self, v, Decimal),
			Value::None {
				..
			} => self.push_none(),
			Value::Type(t) => self.push_value(Value::Any(Box::new(Value::Type(t)))),
			Value::List(v) => self.push_value(Value::Any(Box::new(Value::List(v)))),
			Value::Record(v) => self.push_value(Value::Any(Box::new(Value::Record(v)))),
			Value::Tuple(v) => self.push_value(Value::Any(Box::new(Value::Tuple(v)))),
			Value::Any(v) => match &mut self.inner {
				TypedBuilder::Any {
					builder,
					..
				} => push_any(builder, &v),
				_ => unreachable!("Cannot push Any value to non-Any column"),
			},
			Value::Digest(digest) => match &mut self.inner {
				TypedBuilder::Digest {
					builder,
					inner,
					accuracy,
				} => {
					if digest.inner() != inner || digest.accuracy() != *accuracy {
						panic!(
							"cannot push a Digest({}, {}) into a Digest({inner}, {accuracy}) column",
							digest.inner(),
							digest.accuracy()
						);
					}
					push_digest(builder, &digest);
				}
				_ => unimplemented!(),
			},
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use arrow_array::{Array, ArrayRef};
	use arrow_schema::FieldRef;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_value::value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::{
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			wide_int_array::wides,
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
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
		col.push_value(Value::Boolean(false));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Bool(container) = col.data else {
			panic!("Expected Bool");
		};
		assert_eq!(container.values().iter().collect::<Vec<_>>(), vec![true, false]);
	}

	#[test]
	fn test_undefined_bool() {
		let mut col = builder_of(factory::bool("c", vec![true]));
		col.push_value(Value::none());
		// Pushing none must add a null row and make the bare column optional, never a false.
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_bool() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Boolean, 2));
		col.push_value(Value::Boolean(true));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 3);
		assert!(!col.is_defined(0));
		assert!(!col.is_defined(1));
		assert!(col.is_defined(2));
		assert_eq!(col.get_value(2), Value::Boolean(true));
	}

	#[test]
	fn test_float4() {
		let mut col = builder_of(factory::float4("c", vec![1.0]));
		col.push_value(Value::Float4(OrderedF32::try_from(2.0).unwrap()));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Float4(container) = col.data else {
			panic!("Expected Float4");
		};
		assert_eq!(&container.values()[..], &[1.0, 2.0]);
	}

	#[test]
	fn test_undefined_float4() {
		let mut col = builder_of(factory::float4("c", vec![1.0]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_float4() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Float4, 1));
		col.push_value(Value::Float4(OrderedF32::try_from(3.14).unwrap()));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
	}

	#[test]
	fn test_float8() {
		let mut col = builder_of(factory::float8("c", vec![1.0]));
		col.push_value(Value::Float8(OrderedF64::try_from(2.0).unwrap()));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Float8(container) = col.data else {
			panic!("Expected Float8");
		};
		assert_eq!(&container.values()[..], &[1.0, 2.0]);
	}

	#[test]
	fn test_undefined_float8() {
		let mut col = builder_of(factory::float8("c", vec![1.0]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_float8() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Float8, 1));
		col.push_value(Value::Float8(OrderedF64::try_from(2.718).unwrap()));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
	}

	#[test]
	fn test_int1() {
		let mut col = builder_of(factory::int1("c", vec![1]));
		col.push_value(Value::Int1(2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Int1(container) = col.data else {
			panic!("Expected Int1");
		};
		assert_eq!(&container.values()[..], &[1, 2]);
	}

	#[test]
	fn test_undefined_int1() {
		let mut col = builder_of(factory::int1("c", vec![1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int1() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Int1, 1));
		col.push_value(Value::Int1(5));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int1(5));
	}

	#[test]
	fn test_int2() {
		let mut col = builder_of(factory::int2("c", vec![1]));
		col.push_value(Value::Int2(3));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Int2(container) = col.data else {
			panic!("Expected Int2");
		};
		assert_eq!(&container.values()[..], &[1, 3]);
	}

	#[test]
	fn test_undefined_int2() {
		let mut col = builder_of(factory::int2("c", vec![1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int2() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Int2, 1));
		col.push_value(Value::Int2(10));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int2(10));
	}

	#[test]
	fn test_int4() {
		let mut col = builder_of(factory::int4("c", vec![10]));
		col.push_value(Value::Int4(20));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Int4(container) = col.data else {
			panic!("Expected Int4");
		};
		assert_eq!(&container.values()[..], &[10, 20]);
	}

	#[test]
	fn test_undefined_int4() {
		let mut col = builder_of(factory::int4("c", vec![10]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int4() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Int4, 1));
		col.push_value(Value::Int4(20));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int4(20));
	}

	#[test]
	fn test_int8() {
		let mut col = builder_of(factory::int8("c", vec![100]));
		col.push_value(Value::Int8(200));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Int8(container) = col.data else {
			panic!("Expected Int8");
		};
		assert_eq!(&container.values()[..], &[100, 200]);
	}

	#[test]
	fn test_undefined_int8() {
		let mut col = builder_of(factory::int8("c", vec![100]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int8() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Int8, 1));
		col.push_value(Value::Int8(30));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int8(30));
	}

	#[test]
	fn test_int16() {
		let mut col = builder_of(factory::int16("c", vec![1000]));
		col.push_value(Value::Int16(2000));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Int16(container) = col.data else {
			panic!("Expected Int16");
		};
		assert_eq!(wides::<i128>(&container), [1000, 2000]);
	}

	#[test]
	fn test_undefined_int16() {
		let mut col = builder_of(factory::int16("c", vec![1000]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_int16() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Int16, 1));
		col.push_value(Value::Int16(40));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Int16(40));
	}

	#[test]
	fn test_uint1() {
		let mut col = builder_of(factory::uint1("c", vec![1]));
		col.push_value(Value::Uint1(2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uint1(container) = col.data else {
			panic!("Expected Uint1");
		};
		assert_eq!(&container.values()[..], &[1, 2]);
	}

	#[test]
	fn test_undefined_uint1() {
		let mut col = builder_of(factory::uint1("c", vec![1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint1() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Uint1, 1));
		col.push_value(Value::Uint1(1));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint1(1));
	}

	#[test]
	fn test_uint2() {
		let mut col = builder_of(factory::uint2("c", vec![10]));
		col.push_value(Value::Uint2(20));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uint2(container) = col.data else {
			panic!("Expected Uint2");
		};
		assert_eq!(&container.values()[..], &[10, 20]);
	}

	#[test]
	fn test_undefined_uint2() {
		let mut col = builder_of(factory::uint2("c", vec![10]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint2() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Uint2, 1));
		col.push_value(Value::Uint2(2));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint2(2));
	}

	#[test]
	fn test_uint4() {
		let mut col = builder_of(factory::uint4("c", vec![100]));
		col.push_value(Value::Uint4(200));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uint4(container) = col.data else {
			panic!("Expected Uint4");
		};
		assert_eq!(&container.values()[..], &[100, 200]);
	}

	#[test]
	fn test_undefined_uint4() {
		let mut col = builder_of(factory::uint4("c", vec![100]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint4() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Uint4, 1));
		col.push_value(Value::Uint4(3));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint4(3));
	}

	#[test]
	fn test_uint8() {
		let mut col = builder_of(factory::uint8("c", vec![1000]));
		col.push_value(Value::Uint8(2000));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uint8(container) = col.data else {
			panic!("Expected Uint8");
		};
		assert_eq!(&container.values()[..], &[1000, 2000]);
	}

	#[test]
	fn test_undefined_uint8() {
		let mut col = builder_of(factory::uint8("c", vec![1000]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint8() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Uint8, 1));
		col.push_value(Value::Uint8(4));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint8(4));
	}

	#[test]
	fn test_uint16() {
		let mut col = builder_of(factory::uint16("c", vec![10000]));
		col.push_value(Value::Uint16(20000));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uint16(container) = col.data else {
			panic!("Expected Uint16");
		};
		assert_eq!(wides::<u128>(&container), [10000, 20000]);
	}

	#[test]
	fn test_undefined_uint16() {
		let mut col = builder_of(factory::uint16("c", vec![10000]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uint16() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Uint16, 1));
		col.push_value(Value::Uint16(5));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uint16(5));
	}

	#[test]
	fn test_utf8() {
		let mut col = builder_of(factory::utf8("c", vec!["hello".to_string()]));
		col.push_value(Value::Utf8("world".to_string()));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Utf8 {
			container,
			..
		} = col.data
		else {
			panic!("Expected Utf8");
		};
		let collected: Vec<&str> = (0..container.len()).map(|i| container.value(i)).collect();
		assert_eq!(collected, vec!["hello", "world"]);
	}

	#[test]
	fn test_undefined_utf8() {
		let mut col = builder_of(factory::utf8("c", vec!["hello".to_string()]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_utf8() {
		let mut col = builder_of(factory::none_typed("c", ValueType::Utf8, 1));
		col.push_value(Value::Utf8("ok".to_string()));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Utf8("ok".to_string()));
	}

	#[test]
	fn test_undefined() {
		let mut col = builder_of(factory::int2("c", vec![1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let date2 = Date::from_ymd(2023, 12, 31).unwrap();
		let mut col = builder_of(factory::date("c", vec![date1]));
		col.push_value(Value::Date(date2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Date(container) = col.data else {
			panic!("Expected Date");
		};
		assert_eq!(dates(&container), &[date1, date2]);
	}

	#[test]
	fn test_undefined_date() {
		let date1 = Date::from_ymd(2023, 1, 1).unwrap();
		let mut col = builder_of(factory::date("c", vec![date1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_date() {
		let date = Date::from_ymd(2023, 6, 15).unwrap();
		let mut col = builder_of(factory::none_typed("c", ValueType::Date, 1));
		col.push_value(Value::Date(date));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Date(date));
	}

	#[test]
	fn test_datetime() {
		let dt1 = DateTime::from_epoch_secs(1672531200).unwrap(); // 2023-01-01 00:00:00 SVTC
		let dt2 = DateTime::from_epoch_secs(1704067200).unwrap(); // 2024-01-01 00:00:00 SVTC
		let mut col = builder_of(factory::datetime("c", vec![dt1]));
		col.push_value(Value::DateTime(dt2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::DateTime(container) = col.data else {
			panic!("Expected DateTime");
		};
		assert_eq!(datetimes(&container), &[dt1, dt2]);
	}

	#[test]
	fn test_undefined_datetime() {
		let dt1 = DateTime::from_epoch_secs(1672531200).unwrap();
		let mut col = builder_of(factory::datetime("c", vec![dt1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_datetime() {
		let dt = DateTime::from_epoch_secs(1672531200).unwrap();
		let mut col = builder_of(factory::none_typed("c", ValueType::DateTime, 1));
		col.push_value(Value::DateTime(dt));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::DateTime(dt));
	}

	#[test]
	fn test_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let time2 = Time::from_hms(18, 45, 30).unwrap();
		let mut col = builder_of(factory::time("c", vec![time1]));
		col.push_value(Value::Time(time2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Time(container) = col.data else {
			panic!("Expected Time");
		};
		assert_eq!(times(&container), &[time1, time2]);
	}

	#[test]
	fn test_undefined_time() {
		let time1 = Time::from_hms(12, 30, 0).unwrap();
		let mut col = builder_of(factory::time("c", vec![time1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_time() {
		let time = Time::from_hms(15, 20, 10).unwrap();
		let mut col = builder_of(factory::none_typed("c", ValueType::Time, 1));
		col.push_value(Value::Time(time));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Time(time));
	}

	#[test]
	fn test_duration() {
		let duration1 = Duration::from_days(30).unwrap();
		let duration2 = Duration::from_hours(24).unwrap();
		let mut col = builder_of(factory::duration("c", vec![duration1]));
		col.push_value(Value::Duration(duration2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Duration(container) = col.data else {
			panic!("Expected Duration");
		};
		assert_eq!(durations(&container), &[duration1, duration2]);
	}

	#[test]
	fn test_undefined_duration() {
		let duration1 = Duration::from_days(30).unwrap();
		let mut col = builder_of(factory::duration("c", vec![duration1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_duration() {
		let duration = Duration::from_minutes(90).unwrap();
		let mut col = builder_of(factory::none_typed("c", ValueType::Duration, 1));
		col.push_value(Value::Duration(duration));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Duration(duration));
	}

	#[test]
	fn test_identity_id() {
		let (mock, clock, rng) = test_clock_and_rng();
		let id1 = IdentityId::generate(&clock, &rng);
		mock.advance_millis(1);
		let id2 = IdentityId::generate(&clock, &rng);
		let mut col = builder_of(factory::identity_id("c", vec![id1]));
		col.push_value(Value::IdentityId(id2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::IdentityId(container) = col.data else {
			panic!("Expected IdentityId");
		};
		assert_eq!(identity_ids(&container), &[id1, id2]);
	}

	#[test]
	fn test_undefined_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let id1 = IdentityId::generate(&clock, &rng);
		let mut col = builder_of(factory::identity_id("c", vec![id1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_identity_id() {
		let (_, clock, rng) = test_clock_and_rng();
		let id = IdentityId::generate(&clock, &rng);
		let mut col = builder_of(factory::none_typed("c", ValueType::IdentityId, 1));
		col.push_value(Value::IdentityId(id));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::IdentityId(id));
	}

	#[test]
	fn test_uuid4() {
		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let mut col = builder_of(factory::uuid4("c", vec![uuid1]));
		col.push_value(Value::Uuid4(uuid2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uuid4(container) = col.data else {
			panic!("Expected Uuid4");
		};
		assert_eq!(uuid4s(&container), &[uuid1, uuid2]);
	}

	#[test]
	fn test_undefined_uuid4() {
		let uuid1 = Uuid4::generate();
		let mut col = builder_of(factory::uuid4("c", vec![uuid1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uuid4() {
		let uuid = Uuid4::generate();
		let mut col = builder_of(factory::none_typed("c", ValueType::Uuid4, 1));
		col.push_value(Value::Uuid4(uuid));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uuid4(uuid));
	}

	#[test]
	fn test_uuid7() {
		let (mock, clock, rng) = test_clock_and_rng();
		let uuid1 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid2 = Uuid7::generate(&clock, &rng);
		let mut col = builder_of(factory::uuid7("c", vec![uuid1]));
		col.push_value(Value::Uuid7(uuid2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::Uuid7(container) = col.data else {
			panic!("Expected Uuid7");
		};
		assert_eq!(uuid7s(&container), &[uuid1, uuid2]);
	}

	#[test]
	fn test_undefined_uuid7() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid1 = Uuid7::generate(&clock, &rng);
		let mut col = builder_of(factory::uuid7("c", vec![uuid1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_uuid7() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid = Uuid7::generate(&clock, &rng);
		let mut col = builder_of(factory::none_typed("c", ValueType::Uuid7, 1));
		col.push_value(Value::Uuid7(uuid));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Uuid7(uuid));
	}

	#[test]
	fn test_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let e2 = DictionaryEntryId::U4(20);
		let mut col = builder_of(factory::dictionary_id("c", vec![e1]));
		col.push_value(Value::DictionaryId(e2));
		let col = col.finish("c");
		let col = view(&col);
		let ViewData::DictionaryId {
			container,
			..
		} = col.data
		else {
			panic!("Expected DictionaryId");
		};
		assert_eq!(dictionary_array::iter(&container).collect::<Vec<_>>(), &[e1, e2]);
	}

	#[test]
	fn test_undefined_dictionary_id() {
		let e1 = DictionaryEntryId::U4(10);
		let mut col = builder_of(factory::dictionary_id("c", vec![e1]));
		col.push_value(Value::none());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(col.is_defined(0));
		assert!(!col.is_defined(1));
	}

	#[test]
	fn test_push_value_to_none_dictionary_id() {
		let e = DictionaryEntryId::U4(42);
		let mut col = builder_of(factory::none_typed("c", ValueType::DictionaryId, 1));
		col.push_value(Value::DictionaryId(e));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::DictionaryId(e));
	}

	#[test]
	fn test_push_value_to_none_list() {
		// A list arriving after only none rows must widen the column like a record does, never hit an
		// unreachable arm.
		let list = Value::List(vec![Value::Int4(1), Value::Int4(2)]);
		let mut col = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ValueType::Any)), 2);
		col.push_none();
		col.push_value(list.clone());
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Any(Box::new(list)));
	}

	#[test]
	fn test_push_value_to_none_type() {
		// A type value arriving after only none rows must widen the column like a record does, never hit an
		// unreachable arm.
		let mut col = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ValueType::Any)), 2);
		col.push_none();
		col.push_value(Value::Type(ValueType::Int4));
		let col = col.finish("c");
		let col = view(&col);
		assert_eq!(col.len(), 2);
		assert!(!col.is_defined(0));
		assert!(col.is_defined(1));
		assert_eq!(col.get_value(1), Value::Any(Box::new(Value::Type(ValueType::Int4))));
	}
}
