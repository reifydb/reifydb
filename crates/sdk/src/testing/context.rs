// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	ops::Bound,
	sync::Arc,
};

use postcard::to_stdvec;
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, value_type::ValueType},
};

#[derive(Default)]
struct DictionaryData {
	by_name: HashMap<String, (u64, u8)>,
	find: HashMap<(u64, Vec<u8>), (u128, u8)>,
	get: HashMap<(u64, u128), Vec<u8>>,
}

#[derive(Clone)]
pub struct TestContext {
	state_store: Arc<Mutex<HashMap<EncodedKey, EncodedRow>>>,
	store: Arc<Mutex<BTreeMap<EncodedKey, EncodedRow>>>,
	dictionaries: Arc<Mutex<DictionaryData>>,
	version: CommitVersion,
	logs: Arc<Mutex<Vec<String>>>,
}

impl Default for TestContext {
	fn default() -> Self {
		Self::new(CommitVersion(1))
	}
}

impl TestContext {
	pub fn new(version: CommitVersion) -> Self {
		Self {
			state_store: Arc::new(Mutex::new(HashMap::new())),
			store: Arc::new(Mutex::new(BTreeMap::new())),
			dictionaries: Arc::new(Mutex::new(DictionaryData::default())),
			version,
			logs: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn seed_dictionary(&self, name: &str, id: u64, id_type: ValueType, entries: &[(u128, Value)]) {
		let id_type_byte = id_type.to_u8();
		let mut dicts = self.dictionaries.lock();
		dicts.by_name.insert(name.to_string(), (id, id_type_byte));
		for (entry_id, value) in entries {
			let value_bytes = to_stdvec(value).expect("serialize dictionary value");
			dicts.find.insert((id, value_bytes.clone()), (*entry_id, id_type_byte));
			dicts.get.insert((id, *entry_id), value_bytes);
		}
	}

	pub fn dictionary_id_by_name(&self, name: &str) -> Option<u64> {
		self.dictionaries.lock().by_name.get(name).map(|(id, _)| *id)
	}

	pub fn dictionary_find(&self, dictionary: u64, value_bytes: &[u8]) -> Option<(u128, u8)> {
		self.dictionaries.lock().find.get(&(dictionary, value_bytes.to_vec())).copied()
	}

	pub fn dictionary_get(&self, dictionary: u64, id: u128) -> Option<Vec<u8>> {
		self.dictionaries.lock().get.get(&(dictionary, id)).cloned()
	}

	pub fn state_store(&self) -> &Arc<Mutex<HashMap<EncodedKey, EncodedRow>>> {
		&self.state_store
	}

	pub fn logs(&self) -> Vec<String> {
		self.logs.lock().clone()
	}

	pub fn clear_logs(&self) {
		self.logs.lock().clear();
	}

	pub fn version(&self) -> CommitVersion {
		self.version
	}

	pub fn set_version(&mut self, version: CommitVersion) {
		self.version = version;
	}

	pub fn get_state(&self, key: &EncodedKey) -> Option<Vec<u8>> {
		self.state_store.lock().get(key).map(|v| v.0.to_vec())
	}

	pub fn set_state(&self, key: EncodedKey, value: Vec<u8>) {
		self.state_store.lock().insert(key, EncodedRow(CowVec::new(value)));
	}

	pub fn remove_state(&self, key: &EncodedKey) -> Option<Vec<u8>> {
		self.state_store.lock().remove(key).map(|v| v.0.to_vec())
	}

	pub fn has_state(&self, key: &EncodedKey) -> bool {
		self.state_store.lock().contains_key(key)
	}

	pub fn state_count(&self) -> usize {
		self.state_store.lock().len()
	}

	pub fn clear_state(&self) {
		self.state_store.lock().clear();
	}

	pub fn state_keys(&self) -> Vec<EncodedKey> {
		self.state_store.lock().keys().cloned().collect()
	}

	pub fn store(&self) -> &Arc<Mutex<BTreeMap<EncodedKey, EncodedRow>>> {
		&self.store
	}

	pub fn get_store(&self, key: &EncodedKey) -> Option<EncodedRow> {
		self.store.lock().get(key).cloned()
	}

	pub fn set_store(&self, key: EncodedKey, value: EncodedRow) {
		self.store.lock().insert(key, value);
	}

	pub fn store_range(&self, start: Bound<EncodedKey>, end: Bound<EncodedKey>) -> Vec<(EncodedKey, EncodedRow)> {
		self.store.lock().range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect()
	}

	pub fn store_prefix(&self, prefix: &EncodedKey) -> Vec<(EncodedKey, EncodedRow)> {
		self.store
			.lock()
			.iter()
			.filter(|(k, _)| k.as_slice().starts_with(prefix.as_slice()))
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::testing::helpers::encode_key;

	#[test]
	fn test_context_state_operations() {
		let ctx = TestContext::default();
		let key = encode_key("test_key");
		let value = vec![1, 2, 3];

		// Test set and get
		ctx.set_state(key.clone(), value.clone());
		assert_eq!(ctx.get_state(&key), Some(value.clone()));
		assert!(ctx.has_state(&key));

		// Test remove
		let removed = ctx.remove_state(&key);
		assert_eq!(removed, Some(value));
		assert!(!ctx.has_state(&key));
		assert_eq!(ctx.get_state(&key), None);
	}

	#[test]
	fn test_context_logs() {
		let ctx = TestContext::default();

		// Simulate logging (would be done through callbacks in real usage)
		ctx.logs.lock().push("Log 1".to_string());
		ctx.logs.lock().push("Log 2".to_string());

		let logs = ctx.logs();
		assert_eq!(logs.len(), 2);
		assert_eq!(logs[0], "Log 1");
		assert_eq!(logs[1], "Log 2");

		ctx.clear_logs();
		assert_eq!(ctx.logs().len(), 0);
	}

	#[test]
	fn test_context_state_inspection() {
		let ctx = TestContext::default();

		ctx.set_state(encode_key("key1"), vec![1]);
		ctx.set_state(encode_key("key2"), vec![2]);
		ctx.set_state(encode_key("key3"), vec![3]);

		assert_eq!(ctx.state_count(), 3);

		let keys = ctx.state_keys();
		assert_eq!(keys.len(), 3);

		ctx.clear_state();
		assert_eq!(ctx.state_count(), 0);
	}
}
