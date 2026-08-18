// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_value::Result;

use super::FlowTransaction;

impl FlowTransaction<'_, '_> {
	pub fn set(&mut self, key: &EncodedKey, value: impl Into<EncodedBytes>) -> Result<()> {
		self.txn.set(key, value.into())
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.remove(key)
	}

	pub fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		self.txn.remove_silent(key)
	}

	pub fn set_batch(&mut self, keys: &[EncodedKey], values: &[EncodedBytes]) -> Result<()> {
		for (key, value) in keys.iter().zip(values.iter()) {
			self.txn.set(key, value.clone())?;
		}
		Ok(())
	}

	pub fn remove_batch(&mut self, keys: &[EncodedKey]) -> Result<()> {
		for key in keys {
			self.txn.remove(key)?;
		}
		Ok(())
	}

	pub fn remove_silent_batch(&mut self, keys: &[EncodedKey]) -> Result<()> {
		for key in keys {
			self.txn.remove_silent(key)?;
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::common::CommitVersion;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::{interceptor::interceptors::Interceptors, transaction::admin::AdminTransaction};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::flow::operator::stateful::test_utils::test::create_test_transaction;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
	}

	fn get_row(parent: &mut AdminTransaction, key: &EncodedKey) -> Option<EncodedBytes> {
		parent.get(key).unwrap().map(|m| m.bytes.clone())
	}

	#[test]
	fn test_set_buffers_to_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Value should be in pending buffer
		assert_eq!(txn.pending().get(&key), Some(&value));
	}

	#[test]
	fn test_set_multiple_keys() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("key1"), make_value("value1")).unwrap();
		txn.set(&make_key("key2"), make_value("value2")).unwrap();
		txn.set(&make_key("key3"), make_value("value3")).unwrap();

		assert_eq!(txn.pending().get(&make_key("key1")), Some(&make_value("value1")));
		assert_eq!(txn.pending().get(&make_key("key2")), Some(&make_value("value2")));
		assert_eq!(txn.pending().get(&make_key("key3")), Some(&make_value("value3")));
	}

	#[test]
	fn test_set_overwrites_same_key() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();
		txn.set(&key, make_value("value2")).unwrap();

		// Should have only one entry with latest value
		assert_eq!(txn.pending().get(&key), Some(&make_value("value2")));
	}

	#[test]
	fn test_remove_buffers_to_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		txn.remove(&key).unwrap();

		// Key should be marked for removal in pending buffer
		assert!(txn.pending().is_removed(&key));
	}

	#[test]
	fn test_remove_multiple_keys() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.remove(&make_key("key1")).unwrap();
		txn.remove(&make_key("key2")).unwrap();
		txn.remove(&make_key("key3")).unwrap();

		assert!(txn.pending().is_removed(&make_key("key1")));
		assert!(txn.pending().is_removed(&make_key("key2")));
		assert!(txn.pending().is_removed(&make_key("key3")));
	}

	#[test]
	fn test_set_then_remove() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();
		assert_eq!(txn.pending().get(&key), Some(&make_value("value1")));

		txn.remove(&key).unwrap();
		assert!(txn.pending().is_removed(&key));
		assert_eq!(txn.pending().get(&key), None);
	}

	#[test]
	fn test_remove_then_set() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		txn.remove(&key).unwrap();
		assert!(txn.pending().is_removed(&key));

		txn.set(&key, make_value("value1")).unwrap();
		assert!(!txn.pending().is_removed(&key));
		assert_eq!(txn.pending().get(&key), Some(&make_value("value1")));
	}

	#[test]
	fn test_writes_not_visible_to_parent() {
		let mut parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		let value = make_value("value1");

		// Set in FlowTransaction
		txn.set(&key, value.clone()).unwrap();

		// Parent should not see the write
		assert_eq!(get_row(&mut parent, &key), None);
	}

	#[test]
	fn test_removes_not_visible_to_parent() {
		let mut parent = create_test_transaction();

		// Set a value in parent
		let key = make_key("key1");
		let value = make_value("value1");
		parent.set(&key, value.clone()).unwrap();
		assert_eq!(get_row(&mut parent, &key), Some(value.clone()));

		// Create FlowTransaction and remove the key
		let parent_version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			parent_version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		txn.remove(&key).unwrap();

		// Parent should still see the value
		assert_eq!(get_row(&mut parent, &key), Some(value));
	}

	#[test]
	fn test_mixed_writes_and_removes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("write1"), make_value("v1")).unwrap();
		txn.remove(&make_key("remove1")).unwrap();
		txn.set(&make_key("write2"), make_value("v2")).unwrap();
		txn.remove(&make_key("remove2")).unwrap();

		assert_eq!(txn.pending().get(&make_key("write1")), Some(&make_value("v1")));
		assert_eq!(txn.pending().get(&make_key("write2")), Some(&make_value("v2")));
		assert!(txn.pending().is_removed(&make_key("remove1")));
		assert!(txn.pending().is_removed(&make_key("remove2")));
	}
}
