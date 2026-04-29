// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{Arc, Mutex},
};

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
};
use reifydb_type::util::cowvec::CowVec;

#[derive(Clone)]
pub struct TestContext {
	state_store: Arc<Mutex<HashMap<EncodedKey, EncodedRow>>>,
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
			version,
			logs: Arc::new(Mutex::new(Vec::new())),
		}
	}

	pub fn state_store(&self) -> &Arc<Mutex<HashMap<EncodedKey, EncodedRow>>> {
		&self.state_store
	}

	pub fn logs(&self) -> Vec<String> {
		self.logs.lock().unwrap().clone()
	}

	pub fn clear_logs(&self) {
		self.logs.lock().unwrap().clear();
	}

	pub fn version(&self) -> CommitVersion {
		self.version
	}

	pub fn set_version(&mut self, version: CommitVersion) {
		self.version = version;
	}

	pub fn get_state(&self, key: &EncodedKey) -> Option<Vec<u8>> {
		self.state_store.lock().unwrap().get(key).map(|v| v.0.to_vec())
	}

	pub fn set_state(&self, key: EncodedKey, value: Vec<u8>) {
		self.state_store.lock().unwrap().insert(key, EncodedRow(CowVec::new(value)));
	}

	pub fn remove_state(&self, key: &EncodedKey) -> Option<Vec<u8>> {
		self.state_store.lock().unwrap().remove(key).map(|v| v.0.to_vec())
	}

	pub fn has_state(&self, key: &EncodedKey) -> bool {
		self.state_store.lock().unwrap().contains_key(key)
	}

	pub fn state_count(&self) -> usize {
		self.state_store.lock().unwrap().len()
	}

	pub fn clear_state(&self) {
		self.state_store.lock().unwrap().clear();
	}

	pub fn state_keys(&self) -> Vec<EncodedKey> {
		self.state_store.lock().unwrap().keys().cloned().collect()
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
		ctx.logs.lock().unwrap().push("Log 1".to_string());
		ctx.logs.lock().unwrap().push("Log 2".to_string());

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
