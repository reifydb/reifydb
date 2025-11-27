// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Integration tests for the `#[derive(FromFrame)]` macro.

mod common;

use common::*;
use reifydb_derive::FromFrame;
use reifydb_type::{Date, DateTime, FromFrame as FromFrameTrait, FromFrameError, Time, Uuid4, Uuid7};

// ============================================================================
// 1. Basic Struct Derivation
// ============================================================================

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct BasicUser {
	id: i64,
	name: String,
}

#[test]
fn test_basic_derivation() {
	let frame = frame(vec![int8_column("id", vec![1, 2, 3]), utf8_column("name", vec!["Alice", "Bob", "Charlie"])]);

	let users: Vec<BasicUser> = BasicUser::from_frame(&frame).unwrap();

	assert_eq!(users.len(), 3);
	assert_eq!(
		users[0],
		BasicUser {
			id: 1,
			name: "Alice".to_string()
		}
	);
	assert_eq!(
		users[1],
		BasicUser {
			id: 2,
			name: "Bob".to_string()
		}
	);
	assert_eq!(
		users[2],
		BasicUser {
			id: 3,
			name: "Charlie".to_string()
		}
	);
}

#[test]
fn test_single_row() {
	let frame = frame(vec![int8_column("id", vec![42]), utf8_column("name", vec!["Solo"])]);

	let users: Vec<BasicUser> = BasicUser::from_frame(&frame).unwrap();

	assert_eq!(users.len(), 1);
	assert_eq!(
		users[0],
		BasicUser {
			id: 42,
			name: "Solo".to_string()
		}
	);
}

#[test]
fn test_empty_frame() {
	let frame = frame(vec![int8_column("id", vec![]), utf8_column("name", vec![])]);

	let users: Vec<BasicUser> = BasicUser::from_frame(&frame).unwrap();

	assert!(users.is_empty());
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct MultiTypeStruct {
	int_val: i64,
	float_val: f64,
	bool_val: bool,
	text_val: String,
}

#[test]
fn test_multiple_field_types() {
	let frame = frame(vec![
		int8_column("int_val", vec![100, 200]),
		float8_column("float_val", vec![1.5, 2.5]),
		bool_column("bool_val", vec![true, false]),
		utf8_column("text_val", vec!["hello", "world"]),
	]);

	let items: Vec<MultiTypeStruct> = MultiTypeStruct::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(
		items[0],
		MultiTypeStruct {
			int_val: 100,
			float_val: 1.5,
			bool_val: true,
			text_val: "hello".to_string()
		}
	);
	assert_eq!(
		items[1],
		MultiTypeStruct {
			int_val: 200,
			float_val: 2.5,
			bool_val: false,
			text_val: "world".to_string()
		}
	);
}

// ============================================================================
// 2. Column Name Mapping (#[frame(column = "name")])
// ============================================================================

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct RenamedColumns {
	#[frame(column = "user_id")]
	id: i64,
	#[frame(column = "user_name")]
	name: String,
}

#[test]
fn test_column_rename() {
	let frame = frame(vec![int8_column("user_id", vec![1, 2]), utf8_column("user_name", vec!["Alice", "Bob"])]);

	let users: Vec<RenamedColumns> = RenamedColumns::from_frame(&frame).unwrap();

	assert_eq!(users.len(), 2);
	assert_eq!(
		users[0],
		RenamedColumns {
			id: 1,
			name: "Alice".to_string()
		}
	);
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct MixedRename {
	id: i64,
	#[frame(column = "display_name")]
	name: String,
	active: bool,
}

#[test]
fn test_partial_column_rename() {
	let frame = frame(vec![
		int8_column("id", vec![1]),
		utf8_column("display_name", vec!["Test"]),
		bool_column("active", vec![true]),
	]);

	let items: Vec<MixedRename> = MixedRename::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(
		items[0],
		MixedRename {
			id: 1,
			name: "Test".to_string(),
			active: true
		}
	);
}

// Note: Duplicate column aliases are detected at compile time.
// The following would fail to compile:
//
// #[derive(FromFrame)]
// struct Invalid {
//     #[frame(column = "shared")]
//     first: i64,
//     #[frame(column = "shared")]  // Error: duplicate column alias 'shared'
//     second: i64,
// }

// Test raw identifier: field name `r#type` maps to column "type"
#[derive(FromFrame, Debug, PartialEq, Clone)]
struct RawIdentifier {
	id: i64,
	r#type: String,
}

#[test]
fn test_raw_identifier_column_name() {
	let frame = frame(vec![int8_column("id", vec![1]), utf8_column("type", vec!["admin"])]);

	let items: Vec<RawIdentifier> = RawIdentifier::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(
		items[0],
		RawIdentifier {
			id: 1,
			r#type: "admin".to_string()
		}
	);
}

// ============================================================================
// 3. Optional Fields (#[frame(optional)])
// ============================================================================

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct OptionalFields {
	id: i64,
	#[frame(optional)]
	email: Option<String>,
}

#[test]
fn test_optional_with_existing_column() {
	let frame = frame(vec![int8_column("id", vec![1, 2]), utf8_column("email", vec!["a@test.com", "b@test.com"])]);

	let items: Vec<OptionalFields> = OptionalFields::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(
		items[0],
		OptionalFields {
			id: 1,
			email: Some("a@test.com".to_string())
		}
	);
}

#[test]
fn test_optional_with_missing_column() {
	let frame = frame(vec![int8_column("id", vec![1, 2])]);

	let items: Vec<OptionalFields> = OptionalFields::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(
		items[0],
		OptionalFields {
			id: 1,
			email: None
		}
	);
	assert_eq!(
		items[1],
		OptionalFields {
			id: 2,
			email: None
		}
	);
}

#[test]
fn test_optional_with_undefined_values() {
	let frame = frame(vec![
		int8_column("id", vec![1, 2, 3]),
		optional_utf8_column("email", vec![Some("a@test.com"), None, Some("c@test.com")]),
	]);

	let items: Vec<OptionalFields> = OptionalFields::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 3);
	assert_eq!(items[0].email, Some("a@test.com".to_string()));
	assert_eq!(items[1].email, None);
	assert_eq!(items[2].email, Some("c@test.com".to_string()));
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct MixedOptional {
	id: i64,
	required_name: String,
	#[frame(optional)]
	optional_score: Option<i64>,
	#[frame(optional)]
	optional_flag: Option<bool>,
}

#[test]
fn test_mixed_required_and_optional() {
	let frame = frame(vec![
		int8_column("id", vec![1, 2]),
		utf8_column("required_name", vec!["First", "Second"]),
		optional_int8_column("optional_score", vec![Some(100), None]),
	]);

	let items: Vec<MixedOptional> = MixedOptional::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(
		items[0],
		MixedOptional {
			id: 1,
			required_name: "First".to_string(),
			optional_score: Some(100),
			optional_flag: None, // missing column
		}
	);
	assert_eq!(
		items[1],
		MixedOptional {
			id: 2,
			required_name: "Second".to_string(),
			optional_score: None, // undefined value
			optional_flag: None,  // missing column
		}
	);
}

// ============================================================================
// 4. Type Coercion (#[frame(coerce)])
// ============================================================================

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct CoercedFields {
	#[frame(coerce)]
	int_val: i64, // Can accept Int1, Int2, Int4, Int8
	#[frame(coerce)]
	float_val: f64, // Can accept Float4, Float8, or integers
}

#[test]
fn test_coerce_int4_to_int8() {
	let frame = frame(vec![int4_column("int_val", vec![100, 200]), float8_column("float_val", vec![1.5, 2.5])]);

	let items: Vec<CoercedFields> = CoercedFields::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(items[0].int_val, 100);
	assert_eq!(items[1].int_val, 200);
}

#[test]
fn test_coerce_int_to_float() {
	let frame = frame(vec![int8_column("int_val", vec![42]), int4_column("float_val", vec![100])]);

	let items: Vec<CoercedFields> = CoercedFields::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].int_val, 42);
	assert_eq!(items[0].float_val, 100.0);
}

#[test]
fn test_coerce_float4_to_float8() {
	let frame = frame(vec![int8_column("int_val", vec![1]), float4_column("float_val", vec![3.14f32])]);

	let items: Vec<CoercedFields> = CoercedFields::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert!((items[0].float_val - 3.14).abs() < 0.01);
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct MixedCoerce {
	strict_int: i64, // strict - requires Int8
	#[frame(coerce)]
	coerced_int: i64, // coerced - accepts any int
}

#[test]
fn test_mixed_strict_and_coerced() {
	let frame = frame(vec![int8_column("strict_int", vec![100]), int4_column("coerced_int", vec![200i32])]);

	let items: Vec<MixedCoerce> = MixedCoerce::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].strict_int, 100);
	assert_eq!(items[0].coerced_int, 200);
}

// ============================================================================
// 5. Skipped Fields (#[frame(skip)])
// ============================================================================

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct WithSkipped {
	id: i64,
	#[frame(skip)]
	computed: String,
	name: String,
}

#[test]
fn test_skip_field() {
	let frame = frame(vec![int8_column("id", vec![1, 2]), utf8_column("name", vec!["Alice", "Bob"])]);

	let items: Vec<WithSkipped> = WithSkipped::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].computed, ""); // Default::default()
	assert_eq!(items[0].name, "Alice".to_string());
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct WithSkippedNumeric {
	id: i64,
	#[frame(skip)]
	counter: i32,
}

#[test]
fn test_skip_numeric_field() {
	let frame = frame(vec![int8_column("id", vec![1])]);

	let items: Vec<WithSkippedNumeric> = WithSkippedNumeric::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].counter, 0); // Default::default() for i32
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct WithSkippedOptional {
	id: i64,
	#[frame(skip)]
	metadata: Option<String>,
}

#[test]
fn test_skip_optional_field() {
	let frame = frame(vec![int8_column("id", vec![1, 2])]);

	let items: Vec<WithSkippedOptional> = WithSkippedOptional::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].metadata, None); // Default::default() for Option<T> is None
	assert_eq!(items[1].id, 2);
	assert_eq!(items[1].metadata, None);
}

// ============================================================================
// 6. Error Cases
// ============================================================================

#[test]
fn test_missing_required_column() {
	let frame = frame(vec![int8_column("id", vec![1, 2])]);
	// Missing "name" column

	let result = BasicUser::from_frame(&frame);

	assert!(result.is_err());
	let err = result.unwrap_err();
	match err {
		FromFrameError::MissingColumn {
			column,
			struct_name,
		} => {
			assert_eq!(column, "name");
			assert_eq!(struct_name, "BasicUser");
		}
		_ => panic!("Expected MissingColumn error"),
	}
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct StrictInt {
	value: i32, // strict - requires Int4
}

#[test]
fn test_type_mismatch_error() {
	let frame = frame(vec![int8_column("value", vec![42])]); // Int8, not Int4

	let result = StrictInt::from_frame(&frame);

	assert!(result.is_err());
	let err = result.unwrap_err();
	match err {
		FromFrameError::ValueError {
			column,
			row,
			..
		} => {
			assert_eq!(column, "value");
			assert_eq!(row, 0);
		}
		_ => panic!("Expected ValueError error"),
	}
}

// ============================================================================
// 7. All Value Types Coverage
// ============================================================================

#[derive(FromFrame, Debug, Clone)]
struct AllIntegerTypes {
	int1: i8,
	int2: i16,
	int4: i32,
	int8: i64,
	int16: i128,
	uint1: u8,
	uint2: u16,
	uint4: u32,
	uint8: u64,
	uint16: u128,
}

#[test]
fn test_all_integer_types() {
	let frame = frame(vec![
		int1_column("int1", vec![1i8]),
		int2_column("int2", vec![2i16]),
		int4_column("int4", vec![3i32]),
		int8_column("int8", vec![4i64]),
		int16_column("int16", vec![5i128]),
		uint1_column("uint1", vec![6u8]),
		uint2_column("uint2", vec![7u16]),
		uint4_column("uint4", vec![8u32]),
		uint8_column("uint8", vec![9u64]),
		uint16_column("uint16", vec![10u128]),
	]);

	let items: Vec<AllIntegerTypes> = AllIntegerTypes::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].int1, 1);
	assert_eq!(items[0].int2, 2);
	assert_eq!(items[0].int4, 3);
	assert_eq!(items[0].int8, 4);
	assert_eq!(items[0].int16, 5);
	assert_eq!(items[0].uint1, 6);
	assert_eq!(items[0].uint2, 7);
	assert_eq!(items[0].uint4, 8);
	assert_eq!(items[0].uint8, 9);
	assert_eq!(items[0].uint16, 10);
}

#[derive(FromFrame, Debug, Clone)]
struct FloatTypes {
	float4: f32,
	float8: f64,
}

#[test]
fn test_float_types() {
	let frame = frame(vec![float4_column("float4", vec![3.14f32]), float8_column("float8", vec![2.71828f64])]);

	let items: Vec<FloatTypes> = FloatTypes::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert!((items[0].float4 - 3.14).abs() < 0.001);
	assert!((items[0].float8 - 2.71828).abs() < 0.00001);
}

// ============================================================================
// 8. Temporal and UUID Types
// ============================================================================

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct WithDate {
	id: i64,
	created: Date,
}

#[test]
fn test_date_type() {
	let date = Date::from_ymd(2024, 3, 15).unwrap();
	let frame = frame(vec![int8_column("id", vec![1]), date_column("created", vec![date])]);

	let items: Vec<WithDate> = WithDate::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].created, date);
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct WithTime {
	id: i64,
	scheduled: Time,
}

#[test]
fn test_time_type() {
	let time = Time::from_hms(14, 30, 0).unwrap();
	let frame = frame(vec![int8_column("id", vec![1]), time_column("scheduled", vec![time])]);

	let items: Vec<WithTime> = WithTime::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].scheduled, time);
}

#[derive(FromFrame, Debug, PartialEq, Clone)]
struct WithDateTime {
	id: i64,
	timestamp: DateTime,
}

#[test]
fn test_datetime_type() {
	let dt = DateTime::from_timestamp(1700000000).unwrap();
	let frame = frame(vec![int8_column("id", vec![1]), datetime_column("timestamp", vec![dt])]);

	let items: Vec<WithDateTime> = WithDateTime::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].timestamp, dt);
}

#[derive(FromFrame, Debug, Clone)]
struct WithUuid4 {
	id: i64,
	uuid: Uuid4,
}

#[test]
fn test_uuid4_type() {
	let uuid = Uuid4::generate();
	let frame = frame(vec![int8_column("id", vec![1]), uuid4_column("uuid", vec![uuid])]);

	let items: Vec<WithUuid4> = WithUuid4::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].uuid, uuid);
}

#[derive(FromFrame, Debug, Clone)]
struct WithUuid7 {
	id: i64,
	uuid: Uuid7,
}

#[test]
fn test_uuid7_type() {
	let uuid = Uuid7::generate();
	let frame = frame(vec![int8_column("id", vec![1]), uuid7_column("uuid", vec![uuid])]);

	let items: Vec<WithUuid7> = WithUuid7::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 1);
	assert_eq!(items[0].id, 1);
	assert_eq!(items[0].uuid, uuid);
}

// ============================================================================
// 9. Combined attributes
// ============================================================================

#[derive(FromFrame, Debug, Clone, PartialEq)]
struct CombinedAttributes {
	id: i64,
	#[frame(column = "display_name", optional)]
	name: Option<String>,
	#[frame(coerce)]
	score: i64,
	#[frame(skip)]
	internal: i32,
}

#[test]
fn test_combined_attributes() {
	let frame = frame(vec![
		int8_column("id", vec![1, 2]),
		optional_utf8_column("display_name", vec![Some("Test"), None]),
		int4_column("score", vec![100, 200]),
	]);

	let items: Vec<CombinedAttributes> = CombinedAttributes::from_frame(&frame).unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(
		items[0],
		CombinedAttributes {
			id: 1,
			name: Some("Test".to_string()),
			score: 100,
			internal: 0, // Default
		}
	);
	assert_eq!(
		items[1],
		CombinedAttributes {
			id: 2,
			name: None,
			score: 200,
			internal: 0,
		}
	);
}

// ============================================================================
// 9. Large dataset
// ============================================================================

#[test]
fn test_large_dataset() {
	let count = 1000;
	let ids: Vec<i64> = (0..count).collect();
	let names: Vec<String> = (0..count).map(|i| format!("User_{}", i)).collect();

	let frame = frame(vec![int8_column("id", ids.clone()), utf8_column_owned("name", names.clone())]);

	let users: Vec<BasicUser> = BasicUser::from_frame(&frame).unwrap();

	assert_eq!(users.len(), count as usize);
	assert_eq!(users[0].id, 0);
	assert_eq!(users[0].name, "User_0");
	assert_eq!(users[999].id, 999);
	assert_eq!(users[999].name, "User_999");
}
