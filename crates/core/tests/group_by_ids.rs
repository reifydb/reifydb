// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Grouping by dense `GroupId` against a `GroupKeyDict`.
//!
//! The dict interns each distinct group once: the encoded byte key decides identity, and the `Vec<Value>` the caller
//! needs for output is materialized only on the first row of a group rather than on every row.

use std::str::FromStr;

use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	columns::Columns,
	view::group_by::{GroupId, GroupKeyDict},
};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::{precision::Precision, scale::Scale},
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	time::Time,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};
use uuid::Uuid;

fn frame(spec: Vec<(&str, ColumnBuffer)>) -> Columns {
	Columns::new(spec.into_iter().map(|(name, data)| ColumnWithName::new(name, data)).collect())
}

fn column_of(ty: ValueType, values: Vec<Value>) -> ColumnBuffer {
	let mut builder = ColumnBuffer::none_typed(ty, 0).into_builder();
	for value in values {
		builder.push_value(value);
	}
	builder.finish()
}

fn utf8_column(values: &[&str]) -> ColumnBuffer {
	column_of(ValueType::Utf8, values.iter().map(|v| Value::Utf8((*v).to_string())).collect())
}

fn int4_column(values: &[i32]) -> ColumnBuffer {
	column_of(ValueType::Int4, values.iter().map(|v| Value::Int4(*v)).collect())
}

fn group_rows(columns: &Columns, keys: &[&str], dict: &mut GroupKeyDict) -> Vec<(GroupId, Vec<usize>)> {
	columns.group_by_ids(keys, dict).expect("grouping must succeed")
}

#[test]
fn rows_sharing_a_key_land_in_one_group() {
	let columns = frame(vec![("name", utf8_column(&["a", "b", "a", "b", "a"]))]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["name"], &mut dict);

	assert_eq!(dict.len(), 2, "two distinct names must intern two groups");
	assert_eq!(groups.len(), 2);
	assert_eq!(groups[0].1, vec![0, 2, 4], "every row with name=a must be attributed to the same group");
	assert_eq!(groups[1].1, vec![1, 3]);
}

#[test]
fn group_ids_are_dense_and_follow_first_appearance() {
	let columns = frame(vec![("name", utf8_column(&["z", "y", "z", "x"]))]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["name"], &mut dict);

	let ids: Vec<u32> = groups.iter().map(|(id, _)| id.0).collect();
	assert_eq!(ids, vec![0, 1, 2], "ids must be dense and assigned in the order groups are first seen");
	assert_eq!(dict.values(GroupId(0)), Some(&vec![Value::Utf8("z".to_string())]));
	assert_eq!(dict.values(GroupId(2)), Some(&vec![Value::Utf8("x".to_string())]));
}

#[test]
fn a_second_batch_reuses_the_ids_of_groups_already_interned() {
	// Minting a fresh id for a key already interned would split one group into two for any
	// accumulator keyed by id.
	let mut dict = GroupKeyDict::new();

	let first = frame(vec![("name", utf8_column(&["a", "b"]))]);
	let first_groups = group_rows(&first, &["name"], &mut dict);
	assert_eq!(first_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(), vec![0, 1]);

	let second = frame(vec![("name", utf8_column(&["b", "c", "a"]))]);
	let second_groups = group_rows(&second, &["name"], &mut dict);

	assert_eq!(
		second_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
		vec![1, 2, 0],
		"b and a must keep their original ids; only c is new"
	);
	assert_eq!(dict.len(), 3, "the dict must hold exactly one entry per distinct key across all batches");
}

#[test]
fn a_composite_key_groups_on_every_column() {
	let columns =
		frame(vec![("region", utf8_column(&["eu", "eu", "us", "eu"])), ("tier", int4_column(&[1, 2, 1, 1]))]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["region", "tier"], &mut dict);

	assert_eq!(dict.len(), 3, "(eu,1) (eu,2) (us,1) are three distinct groups");
	assert_eq!(groups[0].1, vec![0, 3], "the two (eu,1) rows must share a group");
	assert_eq!(
		dict.values(GroupId(0)),
		Some(&vec![Value::Utf8("eu".to_string()), Value::Int4(1)]),
		"the dict must retain the key values in column order for output projection"
	);
}

#[test]
fn adjacent_key_columns_stay_framed_apart() {
	// A composite key concatenates its columns, so an encoding that did not frame each column would
	// make ("ab","c") and ("a","bc") collide onto one id.
	let columns = frame(vec![("left", utf8_column(&["ab", "a"])), ("right", utf8_column(&["c", "bc"]))]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["left", "right"], &mut dict);

	assert_eq!(dict.len(), 2, "(ab,c) and (a,bc) are different keys and must not share a group");
	assert_eq!(groups.len(), 2);
}

#[test]
fn distinct_types_with_equal_text_do_not_collide() {
	let columns = frame(vec![("mixed", utf8_column(&["1", "1"]))]);
	let mut text_dict = GroupKeyDict::new();
	group_rows(&columns, &["mixed"], &mut text_dict);

	let numeric = frame(vec![("mixed", int4_column(&[1, 1]))]);
	let mut numeric_dict = GroupKeyDict::new();
	group_rows(&numeric, &["mixed"], &mut numeric_dict);

	assert_ne!(
		text_dict.values(GroupId(0)),
		numeric_dict.values(GroupId(0)),
		"the string \"1\" and the integer 1 must remain distinct group keys"
	);
}

#[test]
fn an_empty_column_set_produces_no_groups() {
	let columns = frame(vec![("name", utf8_column(&[]))]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["name"], &mut dict);

	assert!(groups.is_empty());
	assert_eq!(dict.len(), 0);
}

#[test]
fn an_unknown_key_column_is_an_error_not_a_panic() {
	let columns = frame(vec![("name", utf8_column(&["a"]))]);
	let mut dict = GroupKeyDict::new();

	assert!(columns.group_by_ids(&["missing"], &mut dict).is_err());
}

#[test]
fn every_row_is_attributed_to_the_group_its_values_name() {
	// Groups come back in first-appearance order and the dict hands back the exact key values, which
	// is what output projection reads.
	let columns = frame(vec![
		("region", utf8_column(&["eu", "us", "eu", "ap", "us"])),
		("tier", int4_column(&[1, 1, 2, 1, 1])),
	]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["region", "tier"], &mut dict);

	let expected: Vec<(Vec<Value>, Vec<usize>)> = vec![
		(vec![Value::Utf8("eu".to_string()), Value::Int4(1)], vec![0]),
		(vec![Value::Utf8("us".to_string()), Value::Int4(1)], vec![1, 4]),
		(vec![Value::Utf8("eu".to_string()), Value::Int4(2)], vec![2]),
		(vec![Value::Utf8("ap".to_string()), Value::Int4(1)], vec![3]),
	];

	assert_eq!(groups.len(), expected.len(), "(eu,1) (us,1) (eu,2) (ap,1) are four distinct groups");
	for (index, ((group, rows), (expected_key, expected_rows))) in groups.iter().zip(expected.iter()).enumerate() {
		assert_eq!(group.0 as usize, index, "ids must be dense in first-appearance order");
		assert_eq!(rows, expected_rows, "row membership for {expected_key:?} must match");
		assert_eq!(dict.values(*group), Some(expected_key), "the dict must retain the key values verbatim");
	}
}

#[test]
fn group_by_dictionary_id_width_splits_groups() {
	// Entry ids of different widths are different keys, so a narrower id must never merge into a wider one.
	let column = ColumnBuffer::dictionary_id([
		DictionaryEntryId::U1(5),
		DictionaryEntryId::U2(5),
		DictionaryEntryId::U1(5),
		DictionaryEntryId::U2(256),
	]);
	let columns = frame(vec![("entry", column)]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["entry"], &mut dict);

	assert_eq!(dict.len(), 3, "u1(5), u2(5) and u2(256) are three distinct keys");
	assert_eq!(groups[0].1, vec![0, 2], "both u1(5) rows must share one group");
	assert_eq!(groups[1].1, vec![1]);
	assert_eq!(groups[2].1, vec![3]);
}

#[test]
fn group_by_decimal_ignores_trailing_zeros() {
	// Scale is presentation, not identity, so a key must never split rows that compare equal.
	let decimal = |text: &str| Decimal::from_str(text).expect("a decimal literal");
	let column = ColumnBuffer::decimal(
		Precision::new(10),
		Scale::new(2),
		[decimal("1.0"), decimal("1.00"), decimal("1.0"), decimal("0.5")],
	);
	let columns = frame(vec![("amount", column)]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["amount"], &mut dict);

	assert_eq!(dict.len(), 2, "1.0, 1.00 and 1.0 are one key, 0.5 is the other");
	assert_eq!(groups[0].1, vec![0, 1, 2], "1.0 and 1.00 must share one group");
	assert_eq!(groups[1].1, vec![3]);
}

#[test]
fn a_batch_of_another_decimal_scale_reuses_the_groups_already_interned() {
	// Keying each batch by its own column type would split 1.5 at scale 1 from 1.50 at scale 2.
	let mut dict = GroupKeyDict::new();
	let first = frame(vec![(
		"amount",
		ColumnBuffer::decimal(Precision::new(5), Scale::new(1), [decimal("1.5"), decimal("2.0")]),
	)]);
	let second = frame(vec![(
		"amount",
		ColumnBuffer::decimal(
			Precision::new(10),
			Scale::new(3),
			[decimal("2.000"), decimal("0.125"), decimal("1.500")],
		),
	)]);

	let first_groups = group_rows(&first, &["amount"], &mut dict);
	let second_groups = group_rows(&second, &["amount"], &mut dict);
	let third_groups = group_rows(&first, &["amount"], &mut dict);

	assert_eq!(first_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(), vec![0, 1]);
	assert_eq!(
		second_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
		vec![1, 2, 0],
		"2.000 and 1.500 must land in the groups 2.0 and 1.5 opened; only 0.125 is new"
	);
	assert_eq!(
		third_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
		vec![0, 1],
		"returning to the narrower scale must still find the groups after the dict was rekeyed"
	);
	assert_eq!(dict.len(), 3, "three distinct amounts across all batches");
}

fn decimal(text: &str) -> Decimal {
	Decimal::from_str(text).expect("a decimal literal")
}

fn uuid(bits: u128) -> Uuid {
	Uuid::from_u128(bits)
}

fn scalar_key_cases() -> Vec<(&'static str, ValueType, Value, Value)> {
	vec![
		("boolean", ValueType::Boolean, Value::Boolean(true), Value::Boolean(false)),
		("int1", ValueType::Int1, Value::Int1(-1), Value::Int1(127)),
		("int2", ValueType::Int2, Value::Int2(-300), Value::Int2(300)),
		("int4", ValueType::Int4, Value::Int4(-70_000), Value::Int4(70_000)),
		("int8", ValueType::Int8, Value::Int8(i64::MIN), Value::Int8(i64::MAX)),
		("int16", ValueType::Int16, Value::Int16(i128::MIN), Value::Int16(i128::MAX)),
		("uint1", ValueType::Uint1, Value::Uint1(0), Value::Uint1(255)),
		("uint2", ValueType::Uint2, Value::Uint2(1), Value::Uint2(u16::MAX)),
		("uint4", ValueType::Uint4, Value::Uint4(1), Value::Uint4(u32::MAX)),
		("uint8", ValueType::Uint8, Value::Uint8(1), Value::Uint8(u64::MAX)),
		("uint16", ValueType::Uint16, Value::Uint16(1), Value::Uint16(u128::MAX)),
		("utf8", ValueType::Utf8, Value::Utf8("a".to_string()), Value::Utf8(String::new())),
		(
			"date",
			ValueType::Date,
			Value::Date(Date::from_ymd(1970, 1, 1).unwrap()),
			Value::Date(Date::from_ymd(2026, 9, 23).unwrap()),
		),
		(
			"datetime",
			ValueType::DateTime,
			Value::DateTime(DateTime::from_nanos(0)),
			Value::DateTime(DateTime::from_nanos(1_758_500_000_123_456_789)),
		),
		(
			"time",
			ValueType::Time,
			Value::Time(Time::from_hms_nano(0, 0, 0, 0).unwrap()),
			Value::Time(Time::from_hms_nano(23, 59, 59, 999_999_999).unwrap()),
		),
		(
			"duration",
			ValueType::Duration,
			Value::Duration(Duration::new(1, 0, 0).unwrap()),
			Value::Duration(Duration::new(0, 30, 0).unwrap()),
		),
		(
			"identity_id",
			ValueType::IdentityId,
			Value::IdentityId(IdentityId(Uuid7(uuid(1)))),
			Value::IdentityId(IdentityId(Uuid7(uuid(2)))),
		),
		("uuid4", ValueType::Uuid4, Value::Uuid4(Uuid4(uuid(3))), Value::Uuid4(Uuid4(uuid(4)))),
		("uuid7", ValueType::Uuid7, Value::Uuid7(Uuid7(uuid(5))), Value::Uuid7(Uuid7(uuid(6)))),
		("blob", ValueType::Blob, Value::Blob(Blob::new(vec![])), Value::Blob(Blob::new(vec![0, 255, 7]))),
		("int", ValueType::INT, Value::Int(Int::from(i128::MIN)), Value::Int(Int::from(i128::MAX))),
		("uint", ValueType::UINT, Value::Uint(Uint::from(0u64)), Value::Uint(Uint::from(u128::MAX))),
		("decimal", ValueType::DECIMAL, Value::Decimal(decimal("-1.25")), Value::Decimal(decimal("1.25"))),
		(
			"dictionary_id",
			ValueType::DictionaryId,
			Value::DictionaryId(DictionaryEntryId::U1(7)),
			Value::DictionaryId(DictionaryEntryId::U8(7)),
		),
	]
}

#[test]
fn every_scalar_key_type_groups_by_the_values_it_holds() {
	// A key encoding that dropped a type's payload would merge two distinct keys into one group, and one
	// that dropped the none marker would split the none rows apart or fold them into a value group.
	for (name, ty, first, second) in scalar_key_cases() {
		let none = Value::None {
			inner: ty.clone(),
		};
		let column =
			column_of(ty.clone(), vec![first.clone(), second.clone(), none.clone(), first.clone(), none]);
		let columns = frame(vec![("key", column)]);
		let mut dict = GroupKeyDict::new();

		let groups = group_rows(&columns, &["key"], &mut dict);

		assert_eq!(dict.len(), 3, "{name}: two values and the none rows are three distinct keys");
		assert_eq!(groups.len(), 3, "{name}: three keys must produce three groups");
		assert_eq!(groups[0].1, vec![0, 3], "{name}: both rows holding the first value must share a group");
		assert_eq!(groups[1].1, vec![1], "{name}: the second value must keep a group of its own");
		assert_eq!(groups[2].1, vec![2, 4], "{name}: the none rows must group with each other");
		assert_eq!(
			dict.values(GroupId(0)),
			Some(&vec![first]),
			"{name}: the dict must retain the key value for output projection"
		);
		assert_eq!(dict.values(GroupId(1)), Some(&vec![second]), "{name}: the second key value must survive");
		assert!(
			matches!(dict.values(GroupId(2)).map(Vec::as_slice), Some([Value::None { .. }])),
			"{name}: the none group must materialize a none key value"
		);
	}
}

#[test]
fn positive_and_negative_zero_share_one_float_group() {
	// The value key normalizes -0.0 to 0.0, so a key path comparing raw float bits would split one group in two.
	let columns = frame(vec![("amount", ColumnBuffer::float8([0.0, -0.0, 0.0, 1.5]))]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["amount"], &mut dict);

	assert_eq!(dict.len(), 2, "0.0 and -0.0 are one key");
	assert_eq!(groups[0].1, vec![0, 1, 2]);
	assert_eq!(groups[1].1, vec![3]);
}

#[test]
fn duration_months_days_and_nanos_are_separate_parts_of_the_group_key() {
	// A month, thirty days and an hour carry different components, so folding a duration onto a single
	// nanosecond count would merge three keys into one.
	let column = ColumnBuffer::duration([
		Duration::new(1, 0, 0).unwrap(),
		Duration::new(0, 30, 0).unwrap(),
		Duration::new(0, 0, 3_600_000_000_000).unwrap(),
		Duration::new(1, 0, 0).unwrap(),
	]);
	let columns = frame(vec![("span", column)]);
	let mut dict = GroupKeyDict::new();

	let groups = group_rows(&columns, &["span"], &mut dict);

	assert_eq!(dict.len(), 3, "a month, thirty days and an hour are three distinct keys");
	assert_eq!(groups[0].1, vec![0, 3], "both one-month rows must share a group");
	assert_eq!(groups[1].1, vec![1]);
	assert_eq!(groups[2].1, vec![2]);
}

#[test]
fn a_composite_scalar_key_keeps_its_ids_across_batches() {
	// Group identity spans batches, so a key encoding rebuilt per batch must yield the same bytes or the
	// second batch mints fresh ids for keys already interned.
	let mut dict = GroupKeyDict::new();

	let first = frame(vec![("region", utf8_column(&["eu", "us"])), ("tier", int4_column(&[1, 2]))]);
	let first_groups = group_rows(&first, &["region", "tier"], &mut dict);
	assert_eq!(first_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(), vec![0, 1]);

	let second = frame(vec![("region", utf8_column(&["us", "eu", "ap"])), ("tier", int4_column(&[2, 1, 1]))]);
	let second_groups = group_rows(&second, &["region", "tier"], &mut dict);

	assert_eq!(
		second_groups.iter().map(|(id, _)| id.0).collect::<Vec<_>>(),
		vec![1, 0, 2],
		"(us,2) and (eu,1) must keep the ids the first batch gave them"
	);
	assert_eq!(dict.len(), 3, "only (ap,1) is new");
}

#[test]
fn a_key_column_with_no_rows_produces_no_groups_for_any_scalar_type() {
	// An empty batch must not intern anything, whatever the key type, or a later batch inherits a phantom group.
	for (name, ty, _, _) in scalar_key_cases() {
		let columns = frame(vec![("key", column_of(ty, vec![]))]);
		let mut dict = GroupKeyDict::new();

		let groups = group_rows(&columns, &["key"], &mut dict);

		assert!(groups.is_empty(), "{name}: an empty column must produce no groups");
		assert_eq!(dict.len(), 0, "{name}: an empty column must intern nothing");
	}
}

#[test]
fn keys_that_need_more_than_76_digits_together_are_an_out_of_range_error() {
	// A 70 digit key and a 7 fraction digit key share no 76 digit type; rounding would merge distinct groups.
	let mut dict = GroupKeyDict::new();
	let big = decimal(&format!("1{}", "0".repeat(69)));
	let first = frame(vec![("amount", ColumnBuffer::decimal(Precision::new(70), Scale::new(0), [big]))]);
	let second = frame(vec![(
		"amount",
		ColumnBuffer::decimal(Precision::new(7), Scale::new(7), [decimal("0.0000001")]),
	)]);

	group_rows(&first, &["amount"], &mut dict);
	let err = second.group_by_ids(&["amount"], &mut dict).unwrap_err();
	assert_eq!(err.code, "NUMBER_002", "{err}");
}
