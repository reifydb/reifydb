// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{IdentityId, OrderedF32, OrderedF64, Type, Uuid4, Uuid7, Value};

use crate::value::encoded::{EncodedValues, EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_values(&self, row: &mut EncodedValues, values: &[Value]) {
		debug_assert!(values.len() == self.fields.len());
		for (idx, value) in values.iter().enumerate() {
			self.set_value(row, idx, value)
		}
	}

	pub fn set_value(&self, row: &mut EncodedValues, index: usize, val: &Value) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());

		match (field.r#type, val) {
			(Type::Boolean, Value::Boolean(v)) => self.set_bool(row, index, *v),
			(Type::Boolean, Value::Undefined) => self.set_undefined(row, index),

			(Type::Float4, Value::Float4(v)) => self.set_f32(row, index, v.value()),
			(Type::Float4, Value::Undefined) => self.set_undefined(row, index),

			(Type::Float8, Value::Float8(v)) => self.set_f64(row, index, v.value()),
			(Type::Float8, Value::Undefined) => self.set_undefined(row, index),

			(Type::Int1, Value::Int1(v)) => self.set_i8(row, index, *v),
			(Type::Int1, Value::Undefined) => self.set_undefined(row, index),

			(Type::Int2, Value::Int2(v)) => self.set_i16(row, index, *v),
			(Type::Int2, Value::Undefined) => self.set_undefined(row, index),

			(Type::Int4, Value::Int4(v)) => self.set_i32(row, index, *v),
			(Type::Int4, Value::Undefined) => self.set_undefined(row, index),

			(Type::Int8, Value::Int8(v)) => self.set_i64(row, index, *v),
			(Type::Int8, Value::Undefined) => self.set_undefined(row, index),

			(Type::Int16, Value::Int16(v)) => self.set_i128(row, index, *v),
			(Type::Int16, Value::Undefined) => self.set_undefined(row, index),

			(Type::Utf8, Value::Utf8(v)) => self.set_utf8(row, index, v),
			(Type::Utf8, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uint1, Value::Uint1(v)) => self.set_u8(row, index, *v),
			(Type::Uint1, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uint2, Value::Uint2(v)) => self.set_u16(row, index, *v),
			(Type::Uint2, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uint4, Value::Uint4(v)) => self.set_u32(row, index, *v),
			(Type::Uint4, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uint8, Value::Uint8(v)) => self.set_u64(row, index, *v),
			(Type::Uint8, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uint16, Value::Uint16(v)) => self.set_u128(row, index, *v),
			(Type::Uint16, Value::Undefined) => self.set_undefined(row, index),

			(Type::Date, Value::Date(v)) => self.set_date(row, index, v.clone()),
			(Type::Date, Value::Undefined) => self.set_undefined(row, index),

			(Type::DateTime, Value::DateTime(v)) => self.set_datetime(row, index, v.clone()),
			(Type::DateTime, Value::Undefined) => self.set_undefined(row, index),

			(Type::Time, Value::Time(v)) => self.set_time(row, index, v.clone()),
			(Type::Time, Value::Undefined) => self.set_undefined(row, index),

			(Type::Duration, Value::Duration(v)) => self.set_duration(row, index, v.clone()),
			(Type::Duration, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uuid4, Value::Uuid4(v)) => self.set_uuid4(row, index, v.clone()),
			(Type::Uuid4, Value::Undefined) => self.set_undefined(row, index),

			(Type::Uuid7, Value::Uuid7(v)) => self.set_uuid7(row, index, v.clone()),
			(Type::Uuid7, Value::Undefined) => self.set_undefined(row, index),

			(Type::Blob, Value::Blob(v)) => self.set_blob(row, index, v),
			(Type::Blob, Value::Undefined) => self.set_undefined(row, index),

			(Type::Int, Value::Int(v)) => self.set_int(row, index, v),
			(Type::Uint, Value::Uint(v)) => self.set_uint(row, index, v),
			(Type::Int, Value::Undefined) => self.set_undefined(row, index),
			(Type::Uint, Value::Undefined) => self.set_undefined(row, index),

			(
				Type::Decimal {
					..
				},
				Value::Decimal(v),
			) => self.set_decimal(row, index, v),
			(
				Type::Decimal {
					..
				},
				Value::Undefined,
			) => self.set_undefined(row, index),

			(Type::Undefined, Value::Undefined) => {}
			(Type::Any, Value::Any(_)) => unreachable!("Any type cannot be stored in database"),
			(Type::Any, Value::Undefined) => self.set_undefined(row, index),
			(ty, val) => unreachable!("{ty:?}, {val:?}"),
		}
	}

	pub fn get_value(&self, row: &EncodedValues, index: usize) -> Value {
		let field = &self.fields[index];
		if !row.is_defined(index) {
			return Value::Undefined;
		}
		match field.r#type {
			Type::Boolean => Value::Boolean(self.get_bool(row, index)),
			Type::Float4 => OrderedF32::try_from(self.get_f32(row, index))
				.map(Value::Float4)
				.unwrap_or(Value::Undefined),
			Type::Float8 => OrderedF64::try_from(self.get_f64(row, index))
				.map(Value::Float8)
				.unwrap_or(Value::Undefined),
			Type::Int1 => Value::Int1(self.get_i8(row, index)),
			Type::Int2 => Value::Int2(self.get_i16(row, index)),
			Type::Int4 => Value::Int4(self.get_i32(row, index)),
			Type::Int8 => Value::Int8(self.get_i64(row, index)),
			Type::Int16 => Value::Int16(self.get_i128(row, index)),
			Type::Utf8 => Value::Utf8(self.get_utf8(row, index).to_string()),
			Type::Uint1 => Value::Uint1(self.get_u8(row, index)),
			Type::Uint2 => Value::Uint2(self.get_u16(row, index)),
			Type::Uint4 => Value::Uint4(self.get_u32(row, index)),
			Type::Uint8 => Value::Uint8(self.get_u64(row, index)),
			Type::Uint16 => Value::Uint16(self.get_u128(row, index)),
			Type::Date => Value::Date(self.get_date(row, index)),
			Type::DateTime => Value::DateTime(self.get_datetime(row, index)),
			Type::Time => Value::Time(self.get_time(row, index)),
			Type::Duration => Value::Duration(self.get_duration(row, index)),
			Type::IdentityId => {
				Value::IdentityId(IdentityId::from(Uuid7::from(self.get_uuid7(row, index))))
			}
			Type::Uuid4 => Value::Uuid4(Uuid4::from(self.get_uuid4(row, index))),
			Type::Uuid7 => Value::Uuid7(Uuid7::from(self.get_uuid7(row, index))),
			Type::Blob => Value::Blob(self.get_blob(row, index)),
			Type::Int => Value::Int(self.get_int(row, index)),
			Type::Uint => Value::Uint(self.get_uint(row, index)),
			Type::Decimal {
				..
			} => Value::Decimal(self.get_decimal(row, index)),
			Type::Undefined => Value::Undefined,
			Type::Any => unreachable!("Any type cannot be stored in database"),
		}
	}
}

/// Encode a single value into an encoded row at the specified column index.
/// Unlike `set_value`, this does not validate the field type matches the value.
pub fn encode_value(layout: &EncodedValuesLayout, row: &mut EncodedValues, idx: usize, value: &Value) {
	match value {
		Value::Boolean(v) => layout.set_bool(row, idx, *v),
		Value::Float4(v) => layout.set_f32(row, idx, **v),
		Value::Float8(v) => layout.set_f64(row, idx, **v),
		Value::Int1(v) => layout.set_i8(row, idx, *v),
		Value::Int2(v) => layout.set_i16(row, idx, *v),
		Value::Int4(v) => layout.set_i32(row, idx, *v),
		Value::Int8(v) => layout.set_i64(row, idx, *v),
		Value::Int16(v) => layout.set_i128(row, idx, *v),
		Value::Utf8(v) => layout.set_utf8(row, idx, v),
		Value::Uint1(v) => layout.set_u8(row, idx, *v),
		Value::Uint2(v) => layout.set_u16(row, idx, *v),
		Value::Uint4(v) => layout.set_u32(row, idx, *v),
		Value::Uint8(v) => layout.set_u64(row, idx, *v),
		Value::Uint16(v) => layout.set_u128(row, idx, *v),
		Value::Date(v) => layout.set_date(row, idx, *v),
		Value::DateTime(v) => layout.set_datetime(row, idx, *v),
		Value::Time(v) => layout.set_time(row, idx, *v),
		Value::Duration(v) => layout.set_duration(row, idx, *v),
		Value::IdentityId(v) => layout.set_identity_id(row, idx, *v),
		Value::Uuid4(v) => layout.set_uuid4(row, idx, *v),
		Value::Uuid7(v) => layout.set_uuid7(row, idx, *v),
		Value::Blob(v) => layout.set_blob(row, idx, v),
		Value::Int(v) => layout.set_int(row, idx, v),
		Value::Uint(v) => layout.set_uint(row, idx, v),
		Value::Decimal(v) => layout.set_decimal(row, idx, v),
		Value::Undefined => layout.set_undefined(row, idx),
		Value::Any(_) => {
			unreachable!("Any type cannot be stored")
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
	use reifydb_type::{Blob, Date, DateTime, Duration, OrderedF32, OrderedF64, Time, Type, Uuid4, Uuid7, Value};

	use crate::value::encoded::EncodedValuesLayout;

	#[test]
	fn test_set_utf8_with_dynamic_content() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Int4, Type::Utf8]);
		let mut row = layout.allocate();

		let value1 = Value::Utf8("hello".to_string());
		let value2 = Value::Int4(42);
		let value3 = Value::Utf8("world".to_string());

		layout.set_value(&mut row, 0, &value1);
		layout.set_value(&mut row, 1, &value2);
		layout.set_value(&mut row, 2, &value3);

		assert_eq!(layout.get_utf8(&row, 0), "hello");
		assert_eq!(layout.get_i32(&row, 1), 42);
		assert_eq!(layout.get_utf8(&row, 2), "world");
	}

	#[test]
	fn test_set_values_with_mixed_dynamic_content() {
		let layout =
			EncodedValuesLayout::new(&[Type::Boolean, Type::Utf8, Type::Float4, Type::Utf8, Type::Int2]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Boolean(true),
			Value::Utf8("first_string".to_string()),
			Value::Float4(OrderedF32::try_from(3.14f32).unwrap()),
			Value::Utf8("second_string".to_string()),
			Value::Int2(-100),
		];

		layout.set_values(&mut row, &values);

		assert_eq!(layout.get_bool(&row, 0), true);
		assert_eq!(layout.get_utf8(&row, 1), "first_string");
		assert_eq!(layout.get_f32(&row, 2), 3.14f32);
		assert_eq!(layout.get_utf8(&row, 3), "second_string");
		assert_eq!(layout.get_i16(&row, 4), -100);
	}

	#[test]
	fn test_set_with_empty_and_large_utf8() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate();

		let large_string = "X".repeat(2000);
		let values = vec![
			Value::Utf8("".to_string()),
			Value::Utf8(large_string.clone()),
			Value::Utf8("small".to_string()),
		];

		layout.set_values(&mut row, &values);

		assert_eq!(layout.get_utf8(&row, 0), "");
		assert_eq!(layout.get_utf8(&row, 1), large_string);
		assert_eq!(layout.get_utf8(&row, 2), "small");
		assert_eq!(layout.dynamic_section_size(&row), 2005); // 0 + 2000 + 5
	}

	#[test]
	fn test_get_from_dynamic_content() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Int8, Type::Utf8]);
		let mut row = layout.allocate();

		layout.set_utf8(&mut row, 0, "test_string");
		layout.set_i64(&mut row, 1, 9876543210i64);
		layout.set_utf8(&mut row, 2, "another_string");

		let value0 = layout.get_value(&row, 0);
		let value1 = layout.get_value(&row, 1);
		let value2 = layout.get_value(&row, 2);

		match value0 {
			Value::Utf8(s) => assert_eq!(s, "test_string"),
			_ => panic!("Expected UTF8 value"),
		}

		match value1 {
			Value::Int8(i) => assert_eq!(i, 9876543210),
			_ => panic!("Expected Int8 value"),
		}

		match value2 {
			Value::Utf8(s) => assert_eq!(s, "another_string"),
			_ => panic!("Expected UTF8 value"),
		}
	}

	#[test]
	fn test_set_undefined_with_utf8_fields() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Boolean, Type::Utf8]);
		let mut row = layout.allocate();

		// Set some values
		layout.set_value(&mut row, 0, &Value::Utf8("hello".to_string()));
		layout.set_value(&mut row, 1, &Value::Boolean(true));
		layout.set_value(&mut row, 2, &Value::Utf8("world".to_string()));

		assert!(row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(row.is_defined(2));

		// Set some as undefined
		layout.set_value(&mut row, 0, &Value::Undefined);
		layout.set_value(&mut row, 2, &Value::Undefined);

		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));
		assert!(!row.is_defined(2));

		assert_eq!(layout.get_bool(&row, 1), true);
	}

	#[test]
	fn test_get_all_types_including_utf8() {
		let layout = EncodedValuesLayout::new(&[
			Type::Boolean,
			Type::Int1,
			Type::Int2,
			Type::Int4,
			Type::Int8,
			Type::Uint1,
			Type::Uint2,
			Type::Uint4,
			Type::Uint8,
			Type::Float4,
			Type::Float8,
			Type::Utf8,
		]);
		let mut row = layout.allocate();

		layout.set_bool(&mut row, 0, true);
		layout.set_i8(&mut row, 1, -42);
		layout.set_i16(&mut row, 2, -1000i16);
		layout.set_i32(&mut row, 3, -50000i32);
		layout.set_i64(&mut row, 4, -3000000000i64);
		layout.set_u8(&mut row, 5, 200u8);
		layout.set_u16(&mut row, 6, 50000u16);
		layout.set_u32(&mut row, 7, 3000000000u32);
		layout.set_u64(&mut row, 8, 15000000000000000000u64);
		layout.set_f32(&mut row, 9, 2.5);
		layout.set_f64(&mut row, 10, 123.456789);
		layout.set_utf8(&mut row, 11, "dynamic_string");

		let values: Vec<Value> = (0..12).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(values[0], Value::Boolean(true));
		assert_eq!(values[1], Value::Int1(-42));
		assert_eq!(values[2], Value::Int2(-1000));
		assert_eq!(values[3], Value::Int4(-50000));
		assert_eq!(values[4], Value::Int8(-3000000000));
		assert_eq!(values[5], Value::Uint1(200));
		assert_eq!(values[6], Value::Uint2(50000));
		assert_eq!(values[7], Value::Uint4(3000000000));
		assert_eq!(values[8], Value::Uint8(15000000000000000000));
		assert_eq!(values[9], Value::Float4(OrderedF32::try_from(2.5f32).unwrap()));
		assert_eq!(values[10], Value::Float8(OrderedF64::try_from(123.456789f64).unwrap()));
		assert_eq!(values[11], Value::Utf8("dynamic_string".to_string()));
	}

	#[test]
	fn test_set_values_sparse_with_utf8() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate();

		// Only set some values
		let values = vec![
			Value::Utf8("first".to_string()),
			Value::Undefined,
			Value::Utf8("third".to_string()),
			Value::Undefined,
		];

		layout.set_values(&mut row, &values);

		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));
		assert!(!row.is_defined(3));

		assert_eq!(layout.get_utf8(&row, 0), "first");
		assert_eq!(layout.get_utf8(&row, 2), "third");
	}

	#[test]
	fn test_set_values_unicode_strings() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Int4, Type::Utf8]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Utf8("🎉🚀✨".to_string()),
			Value::Int4(123),
			Value::Utf8("Hello 世界".to_string()),
		];

		layout.set_values(&mut row, &values);

		assert_eq!(layout.get_utf8(&row, 0), "🎉🚀✨");
		assert_eq!(layout.get_i32(&row, 1), 123);
		assert_eq!(layout.get_utf8(&row, 2), "Hello 世界");
	}

	#[test]
	fn test_static_fields_only_no_dynamic_with_values() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Int4, Type::Float8]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Boolean(false),
			Value::Int4(999),
			Value::Float8(OrderedF64::try_from(std::f64::consts::E).unwrap()),
		];

		layout.set_values(&mut row, &values);

		// Verify no dynamic section
		assert_eq!(layout.dynamic_section_size(&row), 0);
		assert_eq!(row.len(), layout.total_static_size());

		assert_eq!(layout.get_bool(&row, 0), false);
		assert_eq!(layout.get_i32(&row, 1), 999);
		assert_eq!(layout.get_f64(&row, 2), std::f64::consts::E);
	}

	#[test]
	fn test_temporal_types_roundtrip() {
		let layout = EncodedValuesLayout::new(&[Type::Date, Type::DateTime, Type::Time, Type::Duration]);
		let mut row = layout.allocate();

		let original_values = vec![
			Value::Date(Date::new(2025, 7, 15).unwrap()),
			Value::DateTime(DateTime::now()),
			Value::Time(Time::new(14, 30, 45, 123456789).unwrap()),
			Value::Duration(Duration::from_seconds(3600)),
		];

		layout.set_values(&mut row, &original_values);

		let retrieved_values: Vec<Value> = (0..4).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, original_values);
	}

	#[test]
	fn test_temporal_types_with_undefined() {
		let layout = EncodedValuesLayout::new(&[Type::Date, Type::DateTime, Type::Time, Type::Duration]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Date(Date::new(2000, 1, 1).unwrap()),
			Value::Undefined,
			Value::Time(Time::default()),
			Value::Undefined,
		];

		layout.set_values(&mut row, &values);

		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));
		assert!(!row.is_defined(3));

		let retrieved_values: Vec<Value> = (0..4).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values[0], values[0]);
		assert_eq!(retrieved_values[1], Value::Undefined);
		assert_eq!(retrieved_values[2], values[2]);
		assert_eq!(retrieved_values[3], Value::Undefined);
	}

	#[test]
	fn test_mixed_temporal_and_regular_types() {
		let layout = EncodedValuesLayout::new(&[
			Type::Boolean,
			Type::Date,
			Type::Utf8,
			Type::DateTime,
			Type::Int4,
			Type::Time,
			Type::Duration,
		]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Boolean(true),
			Value::Date(Date::new(1985, 10, 26).unwrap()),
			Value::Utf8("time travel".to_string()),
			Value::DateTime(DateTime::new(2015, 10, 21, 16, 29, 0, 0).unwrap()),
			Value::Int4(88),
			Value::Time(Time::new(12, 0, 0, 0).unwrap()),
			Value::Duration(Duration::from_minutes(30)),
		];

		layout.set_values(&mut row, &values);

		let retrieved_values: Vec<Value> = (0..7).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, values);
	}

	#[test]
	fn test_roundtrip_with_dynamic_content() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Int2, Type::Utf8, Type::Float4]);
		let mut row = layout.allocate();

		let original_values = vec![
			Value::Utf8("roundtrip_test".to_string()),
			Value::Int2(32000),
			Value::Utf8("".to_string()),
			Value::Float4(OrderedF32::try_from(1.5f32).unwrap()),
		];

		// Set values
		layout.set_values(&mut row, &original_values);

		// Get values back
		let retrieved_values: Vec<Value> = (0..4).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, original_values);
	}

	#[test]
	fn test_blob_roundtrip() {
		let layout = EncodedValuesLayout::new(&[Type::Blob, Type::Int4, Type::Blob]);
		let mut row = layout.allocate();

		let blob1 = Blob::new(vec![0xDE, 0xAD, 0xBE, 0xEF]);
		let blob2 = Blob::new(vec![]);
		let values = vec![Value::Blob(blob1.clone()), Value::Int4(42), Value::Blob(blob2.clone())];

		layout.set_values(&mut row, &values);

		let retrieved_values: Vec<Value> = (0..3).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, values);

		// Verify blob content directly
		match &retrieved_values[0] {
			Value::Blob(b) => assert_eq!(b.as_bytes(), &[0xDE, 0xAD, 0xBE, 0xEF]),
			_ => panic!("Expected Blob value"),
		}

		match &retrieved_values[2] {
			Value::Blob(b) => assert!(b.is_empty()),
			_ => panic!("Expected Blob value"),
		}
	}

	#[test]
	fn test_blob_with_undefined() {
		let layout = EncodedValuesLayout::new(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Blob(Blob::new(vec![0x00, 0x01, 0x02])),
			Value::Undefined,
			Value::Blob(Blob::new(vec![0xFF, 0xFE])),
		];

		layout.set_values(&mut row, &values);

		assert!(row.is_defined(0));
		assert!(!row.is_defined(1));
		assert!(row.is_defined(2));

		let retrieved_values: Vec<Value> = (0..3).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values[0], values[0]);
		assert_eq!(retrieved_values[1], Value::Undefined);
		assert_eq!(retrieved_values[2], values[2]);
	}

	#[test]
	fn test_uuid_roundtrip() {
		let layout = EncodedValuesLayout::new(&[Type::Uuid4, Type::Uuid7, Type::Int4]);
		let mut row = layout.allocate();

		let uuid4 = Uuid4::generate();
		let uuid7 = Uuid7::generate();
		let values = vec![Value::Uuid4(uuid4), Value::Uuid7(uuid7), Value::Int4(123)];

		layout.set_values(&mut row, &values);

		let retrieved_values: Vec<Value> = (0..3).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, values);
	}

	#[test]
	fn test_uuid_with_undefined() {
		let layout = EncodedValuesLayout::new(&[Type::Uuid4, Type::Uuid7]);
		let mut row = layout.allocate();

		let values = vec![Value::Undefined, Value::Uuid7(Uuid7::generate())];

		layout.set_values(&mut row, &values);

		assert!(!row.is_defined(0));
		assert!(row.is_defined(1));

		let retrieved_values: Vec<Value> = (0..2).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values[0], Value::Undefined);
		assert_eq!(retrieved_values[1], values[1]);
	}

	#[test]
	fn test_mixed_blob_row_number_uuid_types() {
		let layout = EncodedValuesLayout::new(&[
			Type::Blob,
			Type::Int16,
			Type::Uuid4,
			Type::Utf8,
			Type::Uuid7,
			Type::Int4,
		]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Blob(Blob::new(vec![0xCA, 0xFE, 0xBA, 0xBE])),
			Value::Int16(42424242i128),
			Value::Uuid4(Uuid4::generate()),
			Value::Utf8("mixed types test".to_string()),
			Value::Uuid7(Uuid7::generate()),
			Value::Int4(-999),
		];

		layout.set_values(&mut row, &values);

		let retrieved_values: Vec<Value> = (0..6).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, values);

		// Verify dynamic content exists (for blob and utf8)
		assert!(layout.dynamic_section_size(&row) > 0);
	}

	#[test]
	fn test_all_types_comprehensive() {
		// except encoded id

		let layout = EncodedValuesLayout::new(&[
			Type::Boolean,
			Type::Int1,
			Type::Int2,
			Type::Int4,
			Type::Int8,
			Type::Int16,
			Type::Uint1,
			Type::Uint2,
			Type::Uint4,
			Type::Uint8,
			Type::Uint16,
			Type::Float4,
			Type::Float8,
			Type::Utf8,
			Type::Date,
			Type::DateTime,
			Type::Time,
			Type::Duration,
			Type::Uuid4,
			Type::Uuid7,
			Type::Blob,
		]);
		let mut row = layout.allocate();

		let values = vec![
			Value::Boolean(true),
			Value::Int1(-128),
			Value::Int2(-32768),
			Value::Int4(-2147483648),
			Value::Int8(-9223372036854775808),
			Value::Int16(-170141183460469231731687303715884105728),
			Value::Uint1(255),
			Value::Uint2(65535),
			Value::Uint4(4294967295),
			Value::Uint8(18446744073709551615),
			Value::Uint16(340282366920938463463374607431768211455),
			Value::Float4(OrderedF32::try_from(3.14159f32).unwrap()),
			Value::Float8(OrderedF64::try_from(2.718281828459045).unwrap()),
			Value::Utf8("comprehensive test".to_string()),
			Value::Date(Date::new(2025, 12, 31).unwrap()),
			Value::DateTime(DateTime::new(2025, 1, 1, 0, 0, 0, 0).unwrap()),
			Value::Time(Time::new(23, 59, 59, 999999999).unwrap()),
			Value::Duration(Duration::from_hours(24)),
			Value::Uuid4(Uuid4::generate()),
			Value::Uuid7(Uuid7::generate()),
			Value::Blob(Blob::new(vec![
				0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
				0xEE, 0xFF,
			])),
		];

		layout.set_values(&mut row, &values);

		let retrieved_values: Vec<Value> = (0..21).map(|i| layout.get_value(&row, i)).collect();

		assert_eq!(retrieved_values, values);

		// Verify all fields are defined
		for i in 0..21 {
			assert!(row.is_defined(i), "Field {} should be defined", i);
		}
	}
}
