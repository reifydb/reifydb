// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::RefCell,
	collections::{BTreeMap, HashMap, HashSet},
	ops::Bound,
	sync::Arc,
};

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey, tag::type_tag_byte, value::encode_value};
use reifydb_core::common::CommitVersion;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, value_type::ValueType},
};

thread_local! {





	static SHARED_DICTS: RefCell<Option<Arc<Mutex<DictionaryData>>>> = const { RefCell::new(None) };
}

fn shared_dicts() -> Arc<Mutex<DictionaryData>> {
	SHARED_DICTS.with(|s| {
		let mut s = s.borrow_mut();
		if s.is_none() {
			*s = Some(Arc::new(Mutex::new(DictionaryData::default())));
		}
		s.as_ref().unwrap().clone()
	})
}

pub fn seed_test_dictionary(name: &str, id: u64, id_type: ValueType, entries: Vec<(u128, Value)>) {
	let store = shared_dicts();
	let mut store = store.lock();
	store.register(name, id, id_type, &entries);

	store.auto_intern.insert(id);
}

#[derive(Default)]
struct DictionaryData {
	by_name: HashMap<String, (u64, u8)>,
	id_type_by_dict: HashMap<u64, u8>,
	find: HashMap<(u64, Vec<u8>), (u128, u8)>,
	get: HashMap<(u64, u128), Vec<u8>>,
	next_id: HashMap<u64, u128>,
	auto_intern: HashSet<u64>,
}

impl DictionaryData {
	fn register(&mut self, name: &str, id: u64, id_type: ValueType, entries: &[(u128, Value)]) {
		let id_type_byte = type_tag_byte(&id_type);
		self.by_name.insert(name.to_string(), (id, id_type_byte));
		self.id_type_by_dict.insert(id, id_type_byte);
		let mut next = self.next_id.get(&id).copied().unwrap_or(0);
		for (entry_id, value) in entries {
			let value_bytes = encode_value(value).expect("serialize dictionary value");
			self.find.insert((id, value_bytes.clone()), (*entry_id, id_type_byte));
			self.get.insert((id, *entry_id), value_bytes);
			next = next.max(*entry_id + 1);
		}
		self.next_id.insert(id, next);
	}

	fn find_or_intern(&mut self, dictionary: u64, value_bytes: &[u8]) -> Option<(u128, u8)> {
		if let Some(v) = self.find.get(&(dictionary, value_bytes.to_vec())) {
			return Some(*v);
		}
		if !self.auto_intern.contains(&dictionary) {
			return None;
		}
		let id_type_byte = *self.id_type_by_dict.get(&dictionary)?;
		let entry_id = self.next_id.get(&dictionary).copied().unwrap_or(0);
		self.find.insert((dictionary, value_bytes.to_vec()), (entry_id, id_type_byte));
		self.get.insert((dictionary, entry_id), value_bytes.to_vec());
		self.next_id.insert(dictionary, entry_id + 1);
		Some((entry_id, id_type_byte))
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmedTimer {
	pub at: DateTime,
	pub kind: TimerKind,
	pub key: Vec<u8>,
}

#[derive(Clone)]
pub struct TestContext {
	state_store: Arc<Mutex<HashMap<EncodedKey, EncodedBytes>>>,
	store: Arc<Mutex<BTreeMap<EncodedKey, EncodedBytes>>>,
	dictionaries: Arc<Mutex<DictionaryData>>,
	version: CommitVersion,
	logs: Arc<Mutex<Vec<String>>>,
	armed_timers: Arc<Mutex<Vec<ArmedTimer>>>,
	flow_watermark: Arc<Mutex<Option<DateTime>>>,
}

impl Default for TestContext {
	fn default() -> Self {
		Self::new(CommitVersion(1))
	}
}

impl TestContext {
	pub fn new(version: CommitVersion) -> Self {
		let dictionaries = SHARED_DICTS
			.with(|s| s.borrow().clone())
			.unwrap_or_else(|| Arc::new(Mutex::new(DictionaryData::default())));
		Self {
			state_store: Arc::new(Mutex::new(HashMap::new())),
			store: Arc::new(Mutex::new(BTreeMap::new())),
			dictionaries,
			version,
			logs: Arc::new(Mutex::new(Vec::new())),
			armed_timers: Arc::new(Mutex::new(Vec::new())),
			flow_watermark: Arc::new(Mutex::new(None)),
		}
	}

	pub fn seed_dictionary(&self, name: &str, id: u64, id_type: ValueType, entries: &[(u128, Value)]) {
		self.dictionaries.lock().register(name, id, id_type, entries);
	}

	pub fn seed_dictionary_interning(&self, name: &str, id: u64, id_type: ValueType, entries: &[(u128, Value)]) {
		let mut d = self.dictionaries.lock();
		d.register(name, id, id_type, entries);
		d.auto_intern.insert(id);
	}

	pub fn dictionary_id_by_name(&self, name: &str) -> Option<u64> {
		self.dictionaries.lock().by_name.get(name).map(|(id, _)| *id)
	}

	pub fn dictionary_find(&self, dictionary: u64, value_bytes: &[u8]) -> Option<(u128, u8)> {
		self.dictionaries.lock().find_or_intern(dictionary, value_bytes)
	}

	pub fn dictionary_get(&self, dictionary: u64, id: u128) -> Option<Vec<u8>> {
		self.dictionaries.lock().get.get(&(dictionary, id)).cloned()
	}

	pub fn state_store(&self) -> &Arc<Mutex<HashMap<EncodedKey, EncodedBytes>>> {
		&self.state_store
	}

	pub fn logs(&self) -> Vec<String> {
		self.logs.lock().clone()
	}

	pub fn flow_watermark(&self) -> Option<DateTime> {
		*self.flow_watermark.lock()
	}

	pub fn set_flow_watermark(&self, at: DateTime) {
		let mut watermark = self.flow_watermark.lock();
		*watermark = Some(watermark.map_or(at, |current| current.max(at)));
	}

	pub fn armed_timers(&self) -> Vec<ArmedTimer> {
		self.armed_timers.lock().clone()
	}

	pub fn arm_timer(&self, timer: ArmedTimer) {
		self.armed_timers.lock().push(timer);
	}

	pub fn disarm_timer(&self, timer: &ArmedTimer) {
		self.armed_timers.lock().retain(|armed| armed != timer);
	}

	pub fn take_due_timers(&self, at: DateTime) -> Vec<ArmedTimer> {
		let mut armed = self.armed_timers.lock();
		let mut due: Vec<ArmedTimer> = armed.iter().filter(|timer| timer.at <= at).cloned().collect();
		armed.retain(|timer| timer.at > at);
		due.sort_by(|a, b| a.at.cmp(&b.at).then((a.kind as u8).cmp(&(b.kind as u8))).then(a.key.cmp(&b.key)));
		due
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
		self.state_store.lock().insert(key, EncodedBytes(CowVec::new(value)));
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

	pub fn store(&self) -> &Arc<Mutex<BTreeMap<EncodedKey, EncodedBytes>>> {
		&self.store
	}

	pub fn get_store(&self, key: &EncodedKey) -> Option<EncodedBytes> {
		self.store.lock().get(key).cloned()
	}

	pub fn set_store(&self, key: EncodedKey, value: EncodedBytes) {
		self.store.lock().insert(key, value);
	}

	pub fn store_range(&self, start: Bound<EncodedKey>, end: Bound<EncodedKey>) -> Vec<(EncodedKey, EncodedBytes)> {
		self.store.lock().range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect()
	}

	pub fn store_prefix(&self, prefix: &EncodedKey) -> Vec<(EncodedKey, EncodedBytes)> {
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
	use crate::helpers::encode_key;

	#[test]
	fn test_context_state_operations() {
		let ctx = TestContext::default();
		let key = encode_key("test_key");
		let value = vec![1, 2, 3];

		ctx.set_state(key.clone(), value.clone());
		assert_eq!(ctx.get_state(&key), Some(value.clone()));
		assert!(ctx.has_state(&key));

		let removed = ctx.remove_state(&key);
		assert_eq!(removed, Some(value));
		assert!(!ctx.has_state(&key));
		assert_eq!(ctx.get_state(&key), None);
	}

	#[test]
	fn test_context_logs() {
		let ctx = TestContext::default();

		// Production pushes these through the FFI callbacks; here we write the sink directly.
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
