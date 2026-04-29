// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, shape::RowShape},
	interface::change::{Change, Diff},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::value::{Value, row_number::RowNumber};

use super::helpers::get_values;
use crate::testing::state::TestStateStore;

pub struct ChangeAssertion<'a> {
	change: &'a Change,
}

impl<'a> ChangeAssertion<'a> {
	pub fn new(change: &'a Change) -> Self {
		Self {
			change,
		}
	}

	pub fn has_diffs(&self, count: usize) -> &Self {
		assert_eq!(
			self.change.diffs.len(),
			count,
			"Expected {} diffs, found {}",
			count,
			self.change.diffs.len()
		);
		self
	}

	pub fn is_empty(&self) -> &Self {
		assert!(self.change.diffs.is_empty(), "Expected empty change, found {} diffs", self.change.diffs.len());
		self
	}

	pub fn has_insert(&self) -> &Self {
		let has_insert = self.change.diffs.iter().any(|d| matches!(d, Diff::Insert { .. }));
		assert!(has_insert, "Expected at least one insert diff");
		self
	}

	pub fn has_update(&self) -> &Self {
		let has_update = self.change.diffs.iter().any(|d| matches!(d, Diff::Update { .. }));
		assert!(has_update, "Expected at least one update diff");
		self
	}

	pub fn has_remove(&self) -> &Self {
		let has_remove = self.change.diffs.iter().any(|d| matches!(d, Diff::Remove { .. }));
		assert!(has_remove, "Expected at least one remove diff");
		self
	}

	pub fn diff_at(&self, index: usize) -> DiffAssertion<'_> {
		assert!(
			index < self.change.diffs.len(),
			"Diff index {} out of range (total: {})",
			index,
			self.change.diffs.len()
		);
		DiffAssertion::new(&self.change.diffs[index])
	}

	pub fn inserts(&self) -> Vec<&Columns> {
		self.change
			.diffs
			.iter()
			.filter_map(|d| match d {
				Diff::Insert {
					post,
				} => Some(post.as_ref()),
				_ => None,
			})
			.collect()
	}

	pub fn updates(&self) -> Vec<(&Columns, &Columns)> {
		self.change
			.diffs
			.iter()
			.filter_map(|d| match d {
				Diff::Update {
					pre,
					post,
				} => Some((pre.as_ref(), post.as_ref())),
				_ => None,
			})
			.collect()
	}

	pub fn removes(&self) -> Vec<&Columns> {
		self.change
			.diffs
			.iter()
			.filter_map(|d| match d {
				Diff::Remove {
					pre,
				} => Some(pre.as_ref()),
				_ => None,
			})
			.collect()
	}

	pub fn has_inserts(&self, count: usize) -> &Self {
		let actual = self.inserts().len();
		assert_eq!(actual, count, "Expected {} inserts, found {}", count, actual);
		self
	}

	pub fn has_updates(&self, count: usize) -> &Self {
		let actual = self.updates().len();
		assert_eq!(actual, count, "Expected {} updates, found {}", count, actual);
		self
	}

	pub fn has_removes(&self, count: usize) -> &Self {
		let actual = self.removes().len();
		assert_eq!(actual, count, "Expected {} removes, found {}", count, actual);
		self
	}
}

pub struct DiffAssertion<'a> {
	diff: &'a Diff,
}

impl<'a> DiffAssertion<'a> {
	pub fn new(diff: &'a Diff) -> Self {
		Self {
			diff,
		}
	}

	pub fn is_insert(&self) -> &Columns {
		match self.diff {
			Diff::Insert {
				post,
			} => post,
			_ => panic!("Expected insert diff, found {:?}", self.diff),
		}
	}

	pub fn is_update(&self) -> (&Columns, &Columns) {
		match self.diff {
			Diff::Update {
				pre,
				post,
			} => (pre, post),
			_ => panic!("Expected update diff, found {:?}", self.diff),
		}
	}

	pub fn is_remove(&self) -> &Columns {
		match self.diff {
			Diff::Remove {
				pre,
			} => pre,
			_ => panic!("Expected remove diff, found {:?}", self.diff),
		}
	}
}

pub struct RowAssertion<'a> {
	row: &'a Row,
}

impl<'a> RowAssertion<'a> {
	pub fn new(row: &'a Row) -> Self {
		Self {
			row,
		}
	}

	pub fn has_number(&self, number: impl Into<RowNumber>) -> &Self {
		let expected = number.into();
		assert_eq!(
			self.row.number, expected,
			"Expected row number {:?}, found {:?}",
			expected, self.row.number
		);
		self
	}

	pub fn has_values(&self, expected: &[Value]) -> &Self {
		let actual = get_values(&self.row.shape, &self.row.encoded);
		assert_eq!(actual, expected, "Row values mismatch. Expected: {:?}, Actual: {:?}", expected, actual);
		self
	}

	pub fn has_field(&self, field_name: &str, expected: Value) -> &Self {
		let values = get_values(&self.row.shape, &self.row.encoded);
		let field_index =
			self.row.shape
				.find_field_index(field_name)
				.unwrap_or_else(|| panic!("Field '{}' not found in layout", field_name));

		assert_eq!(
			values[field_index], expected,
			"Field '{}' mismatch. Expected: {:?}, Actual: {:?}",
			field_name, expected, values[field_index]
		);
		self
	}

	pub fn values(&self) -> Vec<Value> {
		get_values(&self.row.shape, &self.row.encoded)
	}
}

pub struct StateAssertion<'a> {
	store: &'a TestStateStore,
}

impl<'a> StateAssertion<'a> {
	pub fn new(store: &'a TestStateStore) -> Self {
		Self {
			store,
		}
	}

	pub fn is_empty(&self) -> &Self {
		assert!(self.store.is_empty(), "Expected empty state, found {} entries", self.store.len());
		self
	}

	pub fn has_entries(&self, count: usize) -> &Self {
		self.store.assert_count(count);
		self
	}

	pub fn has_key(&self, key: &EncodedKey) -> &Self {
		self.store.assert_exists(key);
		self
	}

	pub fn not_has_key(&self, key: &EncodedKey) -> &Self {
		self.store.assert_not_exists(key);
		self
	}

	pub fn key_has_values(&self, key: &EncodedKey, expected: &[Value], shape: &RowShape) -> &Self {
		self.store.assert_value(key, expected, shape);
		self
	}

	pub fn all_keys<F>(&self, predicate: F) -> &Self
	where
		F: Fn(&EncodedKey) -> bool,
	{
		for key in self.store.keys() {
			assert!(predicate(key), "Key {:?} did not match predicate", key);
		}
		self
	}
}

pub trait Assertable {
	type Assertion<'a>
	where
		Self: 'a;

	fn assert(&self) -> Self::Assertion<'_>;
}

impl Assertable for Change {
	type Assertion<'a>
		= ChangeAssertion<'a>
	where
		Self: 'a;

	fn assert(&self) -> ChangeAssertion<'_> {
		ChangeAssertion::new(self)
	}
}

impl Assertable for Row {
	type Assertion<'a>
		= RowAssertion<'a>
	where
		Self: 'a;

	fn assert(&self) -> RowAssertion<'_> {
		RowAssertion::new(self)
	}
}

impl Assertable for TestStateStore {
	type Assertion<'a>
		= StateAssertion<'a>
	where
		Self: 'a;

	fn assert(&self) -> StateAssertion<'_> {
		StateAssertion::new(self)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::encoded::shape::RowShape;
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::testing::{
		builders::{TestChangeBuilder, TestRowBuilder},
		helpers::encode_key,
		state::TestStateStore,
	};

	#[test]
	fn test_flow_change_assertions() {
		let change = TestChangeBuilder::new()
			.insert_row(1, vec![Value::Int8(10i64)])
			.update_row(2, vec![Value::Int8(20i64)], vec![Value::Int8(30i64)])
			.remove_row(3, vec![Value::Int8(40i64)])
			.build();

		change.assert()
			.has_diffs(3)
			.has_insert()
			.has_update()
			.has_remove()
			.has_inserts(1)
			.has_updates(1)
			.has_removes(1);

		// Need to keep assertion alive for lifetime
		let change_assert = change.assert();
		let diff_assert = change_assert.diff_at(0);
		let insert_columns = diff_assert.is_insert();
		// Convert to Row for assertion (Columns has to_row())
		let insert_row = insert_columns.to_single_row();
		insert_row.assert().has_number(1).has_values(&[Value::Int8(10i64)]);
	}

	#[test]
	fn test_row_assertions() {
		let row = TestRowBuilder::new(42)
			.with_values(vec![Value::Int8(100i64), Value::Utf8("test".into())])
			.build();

		row.assert().has_number(42).has_values(&[Value::Int8(100i64), Value::Utf8("test".into())]);

		assert_eq!(row.assert().values().len(), 2);
	}

	#[test]
	fn test_state_assertions() {
		let mut store = TestStateStore::new();
		let shape = RowShape::testing(&[Type::Int8]);
		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		store.set_value(key1.clone(), &[Value::Int8(10i64)], &shape);
		store.set_value(key2.clone(), &[Value::Int8(20i64)], &shape);

		store.assert()
			.has_entries(2)
			.has_key(&key1)
			.has_key(&key2)
			.key_has_values(&key1, &[Value::Int8(10i64)], &shape)
			.all_keys(|k| k.0.len() == 6); // "key1" and "key2" are 6 bytes (4 chars + 2-byte terminator 0xffff)
	}

	#[test]
	#[should_panic(expected = "Expected 5 diffs, found 1")]
	fn test_assertion_failure() {
		let change = TestChangeBuilder::new().insert_row(1, vec![Value::Int8(10i64)]).build();

		change.assert().has_diffs(5);
	}
}
