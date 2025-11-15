//! State management utilities for operators

mod raw;

use std::collections::HashMap;

use bincode::{config::standard, decode_from_slice, encode_to_vec, serde::Compat};
use serde::{Serialize, de::DeserializeOwned};

use crate::{context::OperatorContext, error::Result};

/// State manager providing type-safe state operations
pub struct State<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> State<'a> {
	/// Create a new state manager
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	/// Get a value from state
	pub fn get<T: DeserializeOwned>(&self, key: impl AsRef<str>) -> Result<Option<T>> {
		let bytes = raw::raw_state_get(self.ctx, key.as_ref())?;
		bytes.map(|b| {
			let compat_result: (Compat<T>, _) = decode_from_slice(&b, standard())?;
			Ok::<T, crate::error::FFIError>(compat_result.0.0)
		})
		.transpose()
		.map_err(Into::into)
	}

	/// Set a value in state
	pub fn set<T: Serialize>(&mut self, key: impl AsRef<str>, value: &T) -> Result<()> {
		let compat = Compat(value);
		let bytes = encode_to_vec(&compat, standard())?;
		raw::raw_state_set(self.ctx, key.as_ref(), &bytes)
	}

	/// Remove a value from state
	pub fn remove(&mut self, key: impl AsRef<str>) -> Result<()> {
		raw::raw_state_remove(self.ctx, key.as_ref())
	}

	/// Update a value in state
	pub fn update<T, F>(&mut self, key: impl AsRef<str>, f: F) -> Result<()>
	where
		T: Serialize + DeserializeOwned + Default,
		F: FnOnce(&mut T),
	{
		let mut value = self.get::<T>(key.as_ref())?.unwrap_or_default();
		f(&mut value);
		self.set(key.as_ref(), &value)
	}

	/// Check if a key exists
	pub fn contains(&self, key: impl AsRef<str>) -> Result<bool> {
		Ok(raw::raw_state_get(self.ctx, key.as_ref())?.is_some())
	}

	/// Clear all state
	pub fn clear(&mut self) -> Result<()> {
		raw::raw_state_clear(self.ctx)
	}

	/// Scan state entries with a prefix
	pub fn scan_prefix(&self, prefix: impl AsRef<str>) -> Result<Vec<(String, Vec<u8>)>> {
		raw::raw_state_prefix(self.ctx, prefix.as_ref())
	}

	/// Get all keys with a prefix
	pub fn keys_with_prefix(&self, prefix: impl AsRef<str>) -> Result<Vec<String>> {
		let entries = self.scan_prefix(prefix)?;
		Ok(entries.into_iter().map(|(k, _)| k).collect())
	}

	// ==================== Pattern Helpers ====================

	/// Increment a counter
	pub fn increment_counter(&mut self, key: impl AsRef<str>) -> Result<i64> {
		let count = self.get::<i64>(key.as_ref())?.unwrap_or(0) + 1;
		self.set(key.as_ref(), &count)?;
		Ok(count)
	}

	/// Decrement a counter
	pub fn decrement_counter(&mut self, key: impl AsRef<str>) -> Result<i64> {
		let count = self.get::<i64>(key.as_ref())?.unwrap_or(0) - 1;
		self.set(key.as_ref(), &count)?;
		Ok(count)
	}

	/// Add to a counter
	pub fn add_to_counter(&mut self, key: impl AsRef<str>, delta: i64) -> Result<i64> {
		let count = self.get::<i64>(key.as_ref())?.unwrap_or(0) + delta;
		self.set(key.as_ref(), &count)?;
		Ok(count)
	}

	/// Accumulate values in a list
	pub fn accumulate<T>(&mut self, key: impl AsRef<str>, value: T) -> Result<Vec<T>>
	where
		T: Serialize + DeserializeOwned,
	{
		let mut values = self.get::<Vec<T>>(key.as_ref())?.unwrap_or_default();
		values.push(value);
		self.set(key.as_ref(), &values)?;
		Ok(values)
	}

	/// Get or insert a value
	pub fn get_or_insert<T, F>(&mut self, key: impl AsRef<str>, f: F) -> Result<T>
	where
		T: Serialize + DeserializeOwned + Clone,
		F: FnOnce() -> T,
	{
		if let Some(value) = self.get::<T>(key.as_ref())? {
			Ok(value)
		} else {
			let value = f();
			self.set(key.as_ref(), &value)?;
			Ok(value)
		}
	}

	/// Get or insert with default
	pub fn get_or_default<T>(&mut self, key: impl AsRef<str>) -> Result<T>
	where
		T: Serialize + DeserializeOwned + Default + Clone,
	{
		self.get_or_insert(key, T::default)
	}

	/// Update a map entry
	pub fn update_map_entry<K, V, F>(&mut self, map_key: impl AsRef<str>, entry_key: K, f: F) -> Result<()>
	where
		K: Serialize + DeserializeOwned + std::hash::Hash + Eq,
		V: Serialize + DeserializeOwned + Default,
		F: FnOnce(&mut V),
	{
		let mut map = self.get::<HashMap<K, V>>(map_key.as_ref())?.unwrap_or_default();
		f(map.entry(entry_key).or_default());
		self.set(map_key.as_ref(), &map)
	}

	/// Merge a map into state
	pub fn merge_map<K, V>(&mut self, key: impl AsRef<str>, other: HashMap<K, V>) -> Result<()>
	where
		K: Serialize + DeserializeOwned + std::hash::Hash + Eq,
		V: Serialize + DeserializeOwned,
	{
		let mut map = self.get::<HashMap<K, V>>(key.as_ref())?.unwrap_or_default();
		map.extend(other);
		self.set(key.as_ref(), &map)
	}
}

/// Builder pattern for complex state operations
pub struct StateBuilder<'a> {
	state: State<'a>,
}

impl<'a> StateBuilder<'a> {
	pub fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			state: State::new(ctx),
		}
	}

	pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
		self.state.get(key)
	}

	pub fn set<T: Serialize>(mut self, key: &str, value: &T) -> Result<Self> {
		self.state.set(key, value)?;
		Ok(self)
	}

	pub fn update<T, F>(mut self, key: &str, f: F) -> Result<Self>
	where
		T: Serialize + DeserializeOwned + Default,
		F: FnOnce(&mut T),
	{
		self.state.update(key, f)?;
		Ok(self)
	}

	pub fn build(self) -> State<'a> {
		self.state
	}
}
