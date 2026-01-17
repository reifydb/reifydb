// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, schema::Schema},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::value::{Value, row_number::RowNumber};

use crate::{
	flow::{FlowChange, FlowDiff},
	testing::state::TestStateStore,
};

/// Assertions for FlowChange outputs
pub struct FlowChangeAssertion<'a> {
	change: &'a FlowChange,
}

impl<'a> FlowChangeAssertion<'a> {
	/// Create a new FlowChange assertion
	pub fn new(change: &'a FlowChange) -> Self {
		Self {
			change,
		}
	}

	/// Assert the number of diffs in the change
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

	/// Assert the change is empty (no diffs)
	pub fn is_empty(&self) -> &Self {
		assert!(self.change.diffs.is_empty(), "Expected empty change, found {} diffs", self.change.diffs.len());
		self
	}

	/// Assert the change has at least one insert
	pub fn has_insert(&self) -> &Self {
		let has_insert = self.change.diffs.iter().any(|d| matches!(d, FlowDiff::Insert { .. }));
		assert!(has_insert, "Expected at least one insert diff");
		self
	}

	/// Assert the change has at least one update
	pub fn has_update(&self) -> &Self {
		let has_update = self.change.diffs.iter().any(|d| matches!(d, FlowDiff::Update { .. }));
		assert!(has_update, "Expected at least one update diff");
		self
	}

	/// Assert the change has at least one remove
	pub fn has_remove(&self) -> &Self {
		let has_remove = self.change.diffs.iter().any(|d| matches!(d, FlowDiff::Remove { .. }));
		assert!(has_remove, "Expected at least one remove diff");
		self
	}

	/// Assert a specific diff exists at the given index
	pub fn diff_at(&self, index: usize) -> DiffAssertion<'_> {
		assert!(
			index < self.change.diffs.len(),
			"Diff index {} out of range (total: {})",
			index,
			self.change.diffs.len()
		);
		DiffAssertion::new(&self.change.diffs[index])
	}

	/// Get all insert diffs
	pub fn inserts(&self) -> Vec<&Columns> {
		self.change
			.diffs
			.iter()
			.filter_map(|d| match d {
				FlowDiff::Insert {
					post,
				} => Some(post),
				_ => None,
			})
			.collect()
	}

	/// Get all update diffs
	pub fn updates(&self) -> Vec<(&Columns, &Columns)> {
		self.change
			.diffs
			.iter()
			.filter_map(|d| match d {
				FlowDiff::Update {
					pre,
					post,
				} => Some((pre, post)),
				_ => None,
			})
			.collect()
	}

	/// Get all remove diffs
	pub fn removes(&self) -> Vec<&Columns> {
		self.change
			.diffs
			.iter()
			.filter_map(|d| match d {
				FlowDiff::Remove {
					pre,
				} => Some(pre),
				_ => None,
			})
			.collect()
	}

	/// Assert the number of inserts
	pub fn has_inserts(&self, count: usize) -> &Self {
		let actual = self.inserts().len();
		assert_eq!(actual, count, "Expected {} inserts, found {}", count, actual);
		self
	}

	/// Assert the number of updates
	pub fn has_updates(&self, count: usize) -> &Self {
		let actual = self.updates().len();
		assert_eq!(actual, count, "Expected {} updates, found {}", count, actual);
		self
	}

	/// Assert the number of removes
	pub fn has_removes(&self, count: usize) -> &Self {
		let actual = self.removes().len();
		assert_eq!(actual, count, "Expected {} removes, found {}", count, actual);
		self
	}
}

/// Assertions for a single diff
pub struct DiffAssertion<'a> {
	diff: &'a FlowDiff,
}

impl<'a> DiffAssertion<'a> {
	pub fn new(diff: &'a FlowDiff) -> Self {
		Self {
			diff,
		}
	}

	/// Assert this is an insert diff
	pub fn is_insert(&self) -> &Columns {
		match self.diff {
			FlowDiff::Insert {
				post,
			} => post,
			_ => panic!("Expected insert diff, found {:?}", self.diff),
		}
	}

	/// Assert this is an update diff
	pub fn is_update(&self) -> (&Columns, &Columns) {
		match self.diff {
			FlowDiff::Update {
				pre,
				post,
			} => (pre, post),
			_ => panic!("Expected update diff, found {:?}", self.diff),
		}
	}

	/// Assert this is a remove diff
	pub fn is_remove(&self) -> &Columns {
		match self.diff {
			FlowDiff::Remove {
				pre,
			} => pre,
			_ => panic!("Expected remove diff, found {:?}", self.diff),
		}
	}
}

/// Assertions for Row values
pub struct RowAssertion<'a> {
	row: &'a Row,
}

impl<'a> RowAssertion<'a> {
	/// Create a new row assertion
	pub fn new(row: &'a Row) -> Self {
		Self {
			row,
		}
	}

	/// Assert the row number
	pub fn has_number(&self, number: impl Into<RowNumber>) -> &Self {
		let expected = number.into();
		assert_eq!(
			self.row.number, expected,
			"Expected row number {:?}, found {:?}",
			expected, self.row.number
		);
		self
	}

	/// Assert the row values match (using the row's layout)
	pub fn has_values(&self, expected: &[Value]) -> &Self {
		let actual = super::helpers::get_values(&self.row.schema, &self.row.encoded);
		assert_eq!(actual, expected, "Row values mismatch. Expected: {:?}, Actual: {:?}", expected, actual);
		self
	}

	/// Assert a specific field value (for named layouts)
	pub fn has_field(&self, field_name: &str, expected: Value) -> &Self {
		let values = super::helpers::get_values(&self.row.schema, &self.row.encoded);
		let field_index =
			self.row.schema
				.find_field_index(field_name)
				.unwrap_or_else(|| panic!("Field '{}' not found in layout", field_name));

		assert_eq!(
			values[field_index], expected,
			"Field '{}' mismatch. Expected: {:?}, Actual: {:?}",
			field_name, expected, values[field_index]
		);
		self
	}

	/// Get the values from the row
	pub fn values(&self) -> Vec<Value> {
		super::helpers::get_values(&self.row.schema, &self.row.encoded)
	}
}

/// Assertions for state store
pub struct StateAssertion<'a> {
	store: &'a TestStateStore,
}

impl<'a> StateAssertion<'a> {
	/// Create a new state assertion
	pub fn new(store: &'a TestStateStore) -> Self {
		Self {
			store,
		}
	}

	/// Assert the state is empty
	pub fn is_empty(&self) -> &Self {
		assert!(self.store.is_empty(), "Expected empty state, found {} entries", self.store.len());
		self
	}

	/// Assert the state has a specific number of entries
	pub fn has_entries(&self, count: usize) -> &Self {
		self.store.assert_count(count);
		self
	}

	/// Assert a key exists
	pub fn has_key(&self, key: &EncodedKey) -> &Self {
		self.store.assert_exists(key);
		self
	}

	/// Assert a key does not exist
	pub fn not_has_key(&self, key: &EncodedKey) -> &Self {
		self.store.assert_not_exists(key);
		self
	}

	/// Assert a key has specific values
	pub fn key_has_values(&self, key: &EncodedKey, expected: &[Value], schema: &Schema) -> &Self {
		self.store.assert_value(key, expected, schema);
		self
	}

	/// Assert all keys match a predicate
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

/// Helper to create assertions
pub trait Assertable {
	type Assertion<'a>
	where
		Self: 'a;

	fn assert(&self) -> Self::Assertion<'_>;
}

impl Assertable for FlowChange {
	type Assertion<'a>
		= FlowChangeAssertion<'a>
	where
		Self: 'a;

	fn assert(&self) -> FlowChangeAssertion<'_> {
		FlowChangeAssertion::new(self)
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
	use reifydb_core::encoded::schema::Schema;
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::testing::{
		builders::{TestFlowChangeBuilder, TestRowBuilder},
		helpers::encode_key,
		state::TestStateStore,
	};

	#[test]
	fn test_flow_change_assertions() {
		let change = TestFlowChangeBuilder::new()
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
		let schema = Schema::testing(&[Type::Int8]);
		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		store.set_value(key1.clone(), &[Value::Int8(10i64)], &schema);
		store.set_value(key2.clone(), &[Value::Int8(20i64)], &schema);

		store.assert()
			.has_entries(2)
			.has_key(&key1)
			.has_key(&key2)
			.key_has_values(&key1, &[Value::Int8(10i64)], &schema)
			.all_keys(|k| k.0.len() == 6); // "key1" and "key2" are 6 bytes (4 chars + 2-byte terminator 0xffff)
	}

	#[test]
	#[should_panic(expected = "Expected 5 diffs, found 1")]
	fn test_assertion_failure() {
		let change = TestFlowChangeBuilder::new().insert_row(1, vec![Value::Int8(10i64)]).build();

		change.assert().has_diffs(5);
	}
}
