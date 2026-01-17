// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::encoded::{
	encoded::EncodedValues,
	key::{EncodedKey, IntoEncodedKey},
	layout::EncodedValuesLayout,
};
use reifydb_type::{
	util::cowvec::CowVec,
	value::{Value, r#type::Type},
};

/// Test helper for FFISingleStateful operators
pub struct SingleStatefulTestHelper {
	layout: EncodedValuesLayout,
	state: Option<Vec<u8>>,
}

impl SingleStatefulTestHelper {
	/// Create a new single stateful test helper
	pub fn new(layout: EncodedValuesLayout) -> Self {
		Self {
			layout,
			state: None,
		}
	}

	/// Create with a simple counter layout (single int8)
	pub fn counter() -> Self {
		Self::new(EncodedValuesLayout::new(&[Type::Int8]))
	}

	/// Set the current state
	pub fn set_state(&mut self, values: &[Value]) {
		let mut encoded = self.layout.allocate_deprecated();
		self.layout.set_values(&mut encoded, values);
		self.state = Some(encoded.0.to_vec());
	}

	/// Get the current state
	pub fn get_state(&self) -> Option<Vec<Value>> {
		self.state.as_ref().map(|bytes| {
			let encoded = EncodedValues(CowVec::new(bytes.clone()));
			super::helpers::get_values(&self.layout, &encoded)
		})
	}

	/// Assert the state matches expected values
	pub fn assert_state(&self, expected: &[Value]) {
		let actual = self.get_state().expect("No state set");
		assert_eq!(actual, expected, "State mismatch");
	}

	/// Clear the state
	pub fn clear(&mut self) {
		self.state = None;
	}

	/// Check if state exists
	pub fn has_state(&self) -> bool {
		self.state.is_some()
	}
}

/// Test helper for FFIKeyedStateful operators
pub struct KeyedStatefulTestHelper {
	layout: EncodedValuesLayout,
	states: HashMap<EncodedKey, EncodedValues>,
}

impl KeyedStatefulTestHelper {
	/// Create a new keyed stateful test helper
	pub fn new(layout: EncodedValuesLayout) -> Self {
		Self {
			layout,
			states: HashMap::new(),
		}
	}

	/// Create with a simple counter layout (single int8)
	pub fn counter() -> Self {
		Self::new(EncodedValuesLayout::new(&[Type::Int8]))
	}

	/// Create with a sum layout (single int8 or int4)
	pub fn sum() -> Self {
		Self::new(EncodedValuesLayout::new(&[Type::Int4]))
	}

	/// Set state for a key
	pub fn set_state<K>(&mut self, key: K, values: &[Value])
	where
		K: IntoEncodedKey,
	{
		let mut encoded = self.layout.allocate_deprecated();
		self.layout.set_values(&mut encoded, values);
		self.states.insert(key.into_encoded_key(), encoded);
	}

	/// Get state for a key
	pub fn get_state<K>(&self, key: K) -> Option<Vec<Value>>
	where
		K: IntoEncodedKey,
	{
		self.states
			.get(&key.into_encoded_key())
			.map(|encoded| super::helpers::get_values(&self.layout, encoded))
	}

	/// Assert state for a key matches expected values
	pub fn assert_state<K>(&self, key: K, expected: &[Value])
	where
		K: IntoEncodedKey,
	{
		let key_encoded = key.into_encoded_key();
		let actual = self
			.states
			.get(&key_encoded)
			.map(|encoded| super::helpers::get_values(&self.layout, encoded))
			.expect("No state for key");
		assert_eq!(actual, expected, "State mismatch for key");
	}

	/// Remove state for a key
	pub fn remove_state<K>(&mut self, key: K) -> Option<Vec<Value>>
	where
		K: IntoEncodedKey,
	{
		self.states
			.remove(&key.into_encoded_key())
			.map(|encoded| super::helpers::get_values(&self.layout, &encoded))
	}

	/// Check if a key has state
	pub fn has_state<K>(&self, key: K) -> bool
	where
		K: IntoEncodedKey,
	{
		self.states.contains_key(&key.into_encoded_key())
	}

	/// Get the number of keys with state
	pub fn state_count(&self) -> usize {
		self.states.len()
	}

	/// Clear all states
	pub fn clear(&mut self) {
		self.states.clear();
	}

	/// Get all keys
	pub fn keys(&self) -> Vec<&EncodedKey> {
		self.states.keys().collect()
	}

	/// Assert the number of states
	pub fn assert_count(&self, expected: usize) {
		assert_eq!(self.state_count(), expected, "Expected {} states, found {}", expected, self.state_count());
	}
}

/// Test helper for FFIWindowStateful operators
pub struct WindowStatefulTestHelper {
	layout: EncodedValuesLayout,
	windows: HashMap<i64, HashMap<EncodedKey, EncodedValues>>, // window_id -> key -> state
	window_size: i64,
}

impl WindowStatefulTestHelper {
	/// Create a new window stateful test helper
	pub fn new(layout: EncodedValuesLayout, window_size: i64) -> Self {
		Self {
			layout,
			windows: HashMap::new(),
			window_size,
		}
	}

	/// Create with a counter layout for time windows
	pub fn time_window_counter(window_size_seconds: i64) -> Self {
		Self::new(EncodedValuesLayout::new(&[Type::Int8]), window_size_seconds)
	}

	/// Create with a sum layout for count windows
	pub fn count_window_sum(window_size_count: i64) -> Self {
		Self::new(EncodedValuesLayout::new(&[Type::Int4]), window_size_count)
	}

	/// Set state for a window and key
	pub fn set_window_state<K>(&mut self, window_id: i64, key: K, values: &[Value])
	where
		K: IntoEncodedKey,
	{
		let mut encoded = self.layout.allocate_deprecated();
		self.layout.set_values(&mut encoded, values);

		self.windows.entry(window_id).or_insert_with(HashMap::new).insert(key.into_encoded_key(), encoded);
	}

	/// Get state for a window and key
	pub fn get_window_state<K>(&self, window_id: i64, key: K) -> Option<Vec<Value>>
	where
		K: IntoEncodedKey,
	{
		self.windows
			.get(&window_id)
			.and_then(|window| window.get(&key.into_encoded_key()))
			.map(|encoded| super::helpers::get_values(&self.layout, encoded))
	}

	/// Assert state for a window and key
	pub fn assert_window_state<K>(&self, window_id: i64, key: K, expected: &[Value])
	where
		K: IntoEncodedKey,
	{
		let key_encoded = key.into_encoded_key();
		let actual = self
			.windows
			.get(&window_id)
			.and_then(|window| window.get(&key_encoded))
			.map(|encoded| super::helpers::get_values(&self.layout, encoded))
			.expect("No state for window and key");
		assert_eq!(actual, expected, "State mismatch for window {} and key", window_id);
	}

	/// Get all states for a window
	pub fn get_window(&self, window_id: i64) -> Option<&HashMap<EncodedKey, EncodedValues>> {
		self.windows.get(&window_id)
	}

	/// Remove a window
	pub fn remove_window(&mut self, window_id: i64) -> Option<HashMap<EncodedKey, EncodedValues>> {
		self.windows.remove(&window_id)
	}

	/// Check if a window exists
	pub fn has_window(&self, window_id: i64) -> bool {
		self.windows.contains_key(&window_id)
	}

	/// Get the number of windows
	pub fn window_count(&self) -> usize {
		self.windows.len()
	}

	/// Get the number of keys in a window
	pub fn window_key_count(&self, window_id: i64) -> usize {
		self.windows.get(&window_id).map(|w| w.len()).unwrap_or(0)
	}

	/// Clear all windows
	pub fn clear(&mut self) {
		self.windows.clear();
	}

	/// Get all window IDs
	pub fn window_ids(&self) -> Vec<i64> {
		let mut ids: Vec<_> = self.windows.keys().copied().collect();
		ids.sort();
		ids
	}

	/// Assert the number of windows
	pub fn assert_window_count(&self, expected: usize) {
		assert_eq!(
			self.window_count(),
			expected,
			"Expected {} windows, found {}",
			expected,
			self.window_count()
		);
	}

	/// Calculate the window ID for a timestamp
	pub fn window_for_timestamp(&self, timestamp: i64) -> i64 {
		timestamp / self.window_size
	}
}

/// Common test scenarios for stateful operators
pub mod scenarios {
	use super::*;
	use crate::{flow::FlowChange, testing::builders::TestFlowChangeBuilder};

	/// Create a sequence of inserts for testing counters
	pub fn counter_inserts(count: usize) -> Vec<FlowChange> {
		use reifydb_type::value::row_number::RowNumber;
		(0..count)
			.map(|i| {
				TestFlowChangeBuilder::new()
					.insert_row(RowNumber(i as u64), vec![Value::Int8(1i64)])
					.build()
			})
			.collect()
	}

	/// Create a sequence of keyed inserts for group-by testing
	pub fn grouped_inserts(groups: &[(&str, i32)]) -> FlowChange {
		use reifydb_type::value::row_number::RowNumber;
		let mut builder = TestFlowChangeBuilder::new();
		for (i, (key, value)) in groups.iter().enumerate() {
			builder = builder
				.insert_row(RowNumber(i as u64), vec![Value::Utf8((*key).into()), Value::Int4(*value)]);
		}
		builder.build()
	}

	/// Create a sequence of updates for testing state changes
	pub fn state_updates(row_number: i64, old_value: i8, new_value: i8) -> FlowChange {
		use reifydb_type::value::row_number::RowNumber;
		TestFlowChangeBuilder::new()
			.update_row(
				RowNumber(row_number as u64),
				vec![Value::Int8(old_value as i64)],
				vec![Value::Int8(new_value as i64)],
			)
			.build()
	}

	/// Create a windowed sequence of events
	pub fn windowed_events(window_size: i64, events_per_window: usize, windows: usize) -> Vec<(i64, FlowChange)> {
		use reifydb_type::value::row_number::RowNumber;
		let mut result = Vec::new();

		for window in 0..windows {
			let base_time = window as i64 * window_size;

			for event in 0..events_per_window {
				let timestamp = base_time + (event as i64 * (window_size / events_per_window as i64));
				let change = TestFlowChangeBuilder::new()
					.insert_row(
						RowNumber(timestamp as u64),
						vec![Value::Int8(1i64), Value::Int8(timestamp as i64)],
					)
					.build();
				result.push((timestamp, change));
			}
		}

		result
	}
}

#[cfg(test)]
pub mod tests {
	use super::{scenarios::*, *};

	#[test]
	fn test_single_stateful_helper() {
		let mut helper = SingleStatefulTestHelper::counter();

		assert!(!helper.has_state());

		helper.set_state(&[Value::Int8(42i64)]);
		assert!(helper.has_state());
		helper.assert_state(&[Value::Int8(42i64)]);

		helper.clear();
		assert!(!helper.has_state());
	}

	#[test]
	fn test_keyed_stateful_helper() {
		let mut helper = KeyedStatefulTestHelper::sum();

		helper.set_state("key1", &[Value::Int4(100)]);
		helper.set_state("key2", &[Value::Int4(200)]);

		helper.assert_count(2);
		helper.assert_state("key1", &[Value::Int4(100)]);
		helper.assert_state("key2", &[Value::Int4(200)]);

		assert!(helper.has_state("key1"));
		assert!(!helper.has_state("key3"));

		let removed = helper.remove_state("key1");
		assert_eq!(removed, Some(vec![Value::Int4(100)]));
		helper.assert_count(1);
	}

	#[test]
	fn test_window_stateful_helper() {
		let mut helper = WindowStatefulTestHelper::time_window_counter(60);

		let window1 = helper.window_for_timestamp(30);
		let window2 = helper.window_for_timestamp(90);

		helper.set_window_state(window1, "key1", &[Value::Int8(10i64)]);
		helper.set_window_state(window2, "key1", &[Value::Int8(20i64)]);

		helper.assert_window_count(2);
		helper.assert_window_state(window1, "key1", &[Value::Int8(10i64)]);
		helper.assert_window_state(window2, "key1", &[Value::Int8(20i64)]);

		assert_eq!(helper.window_ids(), vec![window1, window2]);
		assert_eq!(helper.window_key_count(window1), 1);
	}

	#[test]
	fn test_scenarios() {
		// Test counter inserts
		let changes = counter_inserts(3);
		assert_eq!(changes.len(), 3);

		// Test grouped inserts
		let grouped = grouped_inserts(&[("a", 10), ("b", 20), ("a", 30)]);
		assert_eq!(grouped.diffs.len(), 3);

		// Test state updates
		let update = state_updates(1, 10, 20);
		assert_eq!(update.diffs.len(), 1);

		// Test windowed events
		let windowed = windowed_events(60, 2, 2);
		assert_eq!(windowed.len(), 4); // 2 windows * 2 events per window
	}

	#[test]
	fn test_into_encoded_key_with_strings() {
		// This test verifies that IntoEncodedKey works with string literals
		let mut helper = KeyedStatefulTestHelper::sum();

		// Test with &str literals
		helper.set_state("string_key_1", &[Value::Int4(42)]);
		helper.set_state("string_key_2", &[Value::Int4(100)]);

		// Test with String
		let key = String::from("dynamic_key");
		helper.set_state(key.clone(), &[Value::Int4(200)]);

		// Test with numeric keys
		helper.set_state(123u32, &[Value::Int4(300)]);
		helper.set_state(456u64, &[Value::Int4(400)]);

		// Verify all keys work
		assert_eq!(helper.get_state("string_key_1"), Some(vec![Value::Int4(42)]));
		assert_eq!(helper.get_state("string_key_2"), Some(vec![Value::Int4(100)]));
		assert_eq!(helper.get_state(key), Some(vec![Value::Int4(200)]));
		assert_eq!(helper.get_state(123u32), Some(vec![Value::Int4(300)]));
		assert_eq!(helper.get_state(456u64), Some(vec![Value::Int4(400)]));

		assert_eq!(helper.state_count(), 5);
	}
}
