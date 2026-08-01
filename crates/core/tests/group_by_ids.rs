// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Grouping by dense `GroupId` against a `GroupKeyDict`.
//!
//! The dict interns each distinct group once: the encoded byte key decides identity, and the `Vec<Value>` the caller
//! needs for output is materialized only on the first row of a group rather than on every row.

use reifydb_core::value::column::{
	ColumnWithName,
	buffer::ColumnBuffer,
	columns::Columns,
	view::group_by::{GroupId, GroupKeyDict},
};
use reifydb_value::value::{Value, value_type::ValueType};

fn frame(spec: Vec<(&str, ColumnBuffer)>) -> Columns {
	Columns::new(spec.into_iter().map(|(name, data)| ColumnWithName::new(name, data)).collect())
}

fn column_of(ty: ValueType, values: Vec<Value>) -> ColumnBuffer {
	let mut buffer = ColumnBuffer::none_typed(ty, 0);
	for value in values {
		buffer.push_value(value);
	}
	buffer
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
	assert!(dict.is_empty());
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
