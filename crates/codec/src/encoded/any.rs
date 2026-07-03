// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use reifydb_value::value::value_type::ValueType;
use reifydb_value::{reifydb_assertions, value::Value};

use crate::{
	encoded::{row::EncodedRow, shape::RowShape},
	value::{decode_value, encode_value},
};

impl RowShape {
	pub fn set_any(&self, row: &mut EncodedRow, index: usize, value: &Value) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), ValueType::Any);
		}
		let encoded = encode_value(value).expect("unsupported value in any row field");
		self.replace_dynamic_data(row, index, &encoded);
	}

	pub fn get_any(&self, row: &EncodedRow, index: usize) -> Value {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::Any);
		}

		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let data_start = dynamic_start + offset;
		let data_slice = &row.as_slice()[data_start..data_start + length];

		decode_value(data_slice).expect("corrupt any row field bytes")
	}
}

#[cfg(test)]
pub mod tests {
	use crate::value::{decode_value as decode_value_codec, encode_value as encode_value_codec};

	pub fn encode_value(value: &Value) -> Vec<u8> {
		encode_value_codec(value).unwrap()
	}

	pub fn decode_value(bytes: &[u8]) -> Value {
		decode_value_codec(bytes).unwrap()
	}

	use std::f64::consts::E;

	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_value::value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	};

	use crate::encoded::shape::RowShape;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_any_boolean() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let mut row = shape.allocate();
		shape.set_any(&mut row, 0, &Value::Boolean(true));
		assert_eq!(shape.get_any(&row, 0), Value::Boolean(true));
	}

	#[test]
	fn test_any_integers() {
		let shape = RowShape::testing(&[ValueType::Any]);

		let cases: &[Value] = &[
			Value::Int1(-42),
			Value::Int2(-1000),
			Value::Int4(-100000),
			Value::Int8(i64::MIN),
			Value::Int16(i128::MAX),
			Value::Uint1(255),
			Value::Uint2(65535),
			Value::Uint4(u32::MAX),
			Value::Uint8(u64::MAX),
			Value::Uint16(u128::MAX),
		];

		for case in cases {
			let mut row = shape.allocate();
			shape.set_any(&mut row, 0, case);
			assert_eq!(&shape.get_any(&row, 0), case);
		}
	}

	#[test]
	fn test_any_floats() {
		let shape = RowShape::testing(&[ValueType::Any]);

		let f4 = Value::Float4(OrderedF32::try_from(3.14f32).unwrap());
		let mut row = shape.allocate();
		shape.set_any(&mut row, 0, &f4);
		assert_eq!(shape.get_any(&row, 0), f4);

		let f8 = Value::Float8(OrderedF64::try_from(E).unwrap());
		let mut row2 = shape.allocate();
		shape.set_any(&mut row2, 0, &f8);
		assert_eq!(shape.get_any(&row2, 0), f8);
	}

	#[test]
	fn test_any_temporal() {
		let shape = RowShape::testing(&[ValueType::Any]);

		let date = Value::Date(Date::new(2025, 7, 4).unwrap());
		let mut row = shape.allocate();
		shape.set_any(&mut row, 0, &date);
		assert_eq!(shape.get_any(&row, 0), date);

		let dt = Value::DateTime(DateTime::new(2025, 1, 1, 12, 0, 0, 0).unwrap());
		let mut row2 = shape.allocate();
		shape.set_any(&mut row2, 0, &dt);
		assert_eq!(shape.get_any(&row2, 0), dt);

		let t = Value::Time(Time::new(14, 30, 45, 123456789).unwrap());
		let mut row3 = shape.allocate();
		shape.set_any(&mut row3, 0, &t);
		assert_eq!(shape.get_any(&row3, 0), t);

		let dur = Value::duration_seconds(3600);
		let mut row4 = shape.allocate();
		shape.set_any(&mut row4, 0, &dur);
		assert_eq!(shape.get_any(&row4, 0), dur);
	}

	#[test]
	fn test_any_uuid() {
		let (_, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[ValueType::Any]);

		let u4 = Value::Uuid4(Uuid4::generate());
		let mut row = shape.allocate();
		shape.set_any(&mut row, 0, &u4);
		assert_eq!(shape.get_any(&row, 0), u4);

		let u7 = Value::Uuid7(Uuid7::generate(&clock, &rng));
		let mut row2 = shape.allocate();
		shape.set_any(&mut row2, 0, &u7);
		assert_eq!(shape.get_any(&row2, 0), u7);
	}

	#[test]
	fn test_any_utf8() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let v = Value::Utf8("hello, world!".to_string());
		let mut row = shape.allocate();
		shape.set_any(&mut row, 0, &v);
		assert_eq!(shape.get_any(&row, 0), v);
	}

	#[test]
	fn test_any_blob() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let v = Value::Blob(Blob::from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]));
		let mut row = shape.allocate();
		shape.set_any(&mut row, 0, &v);
		assert_eq!(shape.get_any(&row, 0), v);
	}

	#[test]
	fn test_any_none_via_set_value() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let mut row = shape.allocate();
		shape.set_value(&mut row, 0, &Value::none());
		assert!(!row.is_defined(0));
		assert_eq!(shape.get_value(&row, 0), Value::none());
	}

	#[test]
	fn test_any_roundtrip_via_set_get_value() {
		let shape = RowShape::testing(&[ValueType::Any]);

		let cases: &[Value] = &[
			Value::Boolean(false),
			Value::Int4(42),
			Value::Utf8("test".to_string()),
			Value::Uint8(1234567890),
		];

		for inner in cases {
			let wrapped = Value::any(inner.clone());
			let mut row = shape.allocate();
			shape.set_value(&mut row, 0, &wrapped);
			let retrieved = shape.get_value(&row, 0);
			assert_eq!(retrieved, wrapped, "roundtrip failed for {:?}", inner);
		}
	}

	#[test]
	fn test_any_multiple_fields() {
		let shape = RowShape::testing(&[ValueType::Any, ValueType::Int4, ValueType::Any]);
		let mut row = shape.allocate();

		shape.set_any(&mut row, 0, &Value::Utf8("first".to_string()));
		shape.set_i32(&mut row, 1, 99);
		shape.set_any(&mut row, 2, &Value::Boolean(true));

		assert_eq!(shape.get_any(&row, 0), Value::Utf8("first".to_string()));
		assert_eq!(shape.get_i32(&row, 1), 99);
		assert_eq!(shape.get_any(&row, 2), Value::Boolean(true));
	}

	#[test]
	fn test_update_any() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let mut row = shape.allocate();

		shape.set_any(&mut row, 0, &Value::Int4(42));
		assert_eq!(shape.get_any(&row, 0), Value::Int4(42));

		// Overwrite with a different type
		shape.set_any(&mut row, 0, &Value::Utf8("hello".to_string()));
		assert_eq!(shape.get_any(&row, 0), Value::Utf8("hello".to_string()));

		// Overwrite again with boolean
		shape.set_any(&mut row, 0, &Value::Boolean(true));
		assert_eq!(shape.get_any(&row, 0), Value::Boolean(true));
	}

	#[test]
	fn test_update_any_with_other_dynamic_fields() {
		let shape = RowShape::testing(&[ValueType::Any, ValueType::Utf8, ValueType::Any]);
		let mut row = shape.allocate();

		shape.set_any(&mut row, 0, &Value::Int4(1));
		shape.set_utf8(&mut row, 1, "middle");
		shape.set_any(&mut row, 2, &Value::Boolean(false));

		// Update first any with a larger value
		shape.set_any(&mut row, 0, &Value::Utf8("a long string value".to_string()));

		assert_eq!(shape.get_any(&row, 0), Value::Utf8("a long string value".to_string()));
		assert_eq!(shape.get_utf8(&row, 1), "middle");
		assert_eq!(shape.get_any(&row, 2), Value::Boolean(false));
	}

	// `Value::PartialEq` treats every `Value::None { .. }` as equal regardless of `inner`
	// (see crates/value/src/value/mod.rs), so these tests destructure and compare `inner`
	// directly instead of using `assert_eq!(decoded, Value::none_of(ty))`, which would pass
	// vacuously even if the inner type were lost or wrong.

	#[test]
	fn test_encode_decode_none_various_inner_types() {
		// Config defaults such as METRICS_PROFILER_SNAPSHOT_INTERVAL use
		// Value::None { inner: Duration } to mean "disabled". The Any encoding must round-trip
		// that sentinel for any inner type, not just concrete values.
		let cases: &[ValueType] = &[
			ValueType::Boolean,
			ValueType::Int4,
			ValueType::Uint8,
			ValueType::Utf8,
			ValueType::Blob,
			ValueType::Date,
			ValueType::DateTime,
			ValueType::Time,
			ValueType::Duration,
			ValueType::Uuid4,
		];

		for ty in cases {
			let encoded = encode_value(&Value::none_of(ty.clone()));
			match decode_value(&encoded) {
				Value::None {
					inner,
				} => assert_eq!(&inner, ty, "inner type lost for {ty}"),
				other => panic!("expected Value::None for {ty}, got {other:?}"),
			}
		}
	}

	#[test]
	fn test_encode_decode_none_nested_option_duration() {
		// Option<Option<Duration>>::None must round-trip distinctly from Option<Duration>::None:
		// the inner type carries the full nesting, not just the base scalar.
		let inner_ty = ValueType::Option(Box::new(ValueType::Duration));

		let encoded = encode_value(&Value::none_of(inner_ty.clone()));
		match decode_value(&encoded) {
			Value::None {
				inner,
			} => assert_eq!(inner, inner_ty),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_encode_decode_none_triple_nested_option() {
		// Nesting depth must generalize past 2 levels, not just the one depth that happens to
		// survive the current "high bit means Option" type-tag scheme.
		let inner_ty = ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Option(Box::new(
			ValueType::Duration,
		))))));

		let encoded = encode_value(&Value::none_of(inner_ty.clone()));
		match decode_value(&encoded) {
			Value::None {
				inner,
			} => assert_eq!(inner, inner_ty),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_any_set_get_none_duration() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let mut row = shape.allocate();

		shape.set_any(&mut row, 0, &Value::none_of(ValueType::Duration));

		assert!(
			row.is_defined(0),
			"an Any field holding a None-of-Duration is a stored value, not an unset field"
		);
		match shape.get_any(&row, 0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Duration),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_any_set_get_none_nested_option_duration() {
		let shape = RowShape::testing(&[ValueType::Any]);
		let mut row = shape.allocate();

		let inner_ty = ValueType::Option(Box::new(ValueType::Duration));
		shape.set_any(&mut row, 0, &Value::none_of(inner_ty.clone()));

		match shape.get_any(&row, 0) {
			Value::None {
				inner,
			} => assert_eq!(inner, inner_ty),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}
}
