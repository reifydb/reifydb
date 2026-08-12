// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound::{Excluded, Included};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::common::CommitVersion;
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{interceptor::interceptors::Interceptors, multi::RangeScope};
use reifydb_value::{Result, util::cowvec::CowVec, value::identity::IdentityId};

use reifydb_flow::transaction::{FlowTransaction, deferred::DeferredTransaction};

use crate::common::create_test_transaction;

fn make_key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn make_value(s: &str) -> EncodedBytes {
	EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
}

#[test]
fn test_get_from_pending() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let key = make_key("key1");
	let value = make_value("value1");

	txn.set(&key, value.clone()).unwrap();

	let result = txn.get(&key).unwrap();
	assert_eq!(result, Some(value));
}

#[test]
fn test_get_from_committed() {
	let t = TestEngine::new();

	let key = make_key("key1");
	let value = make_value("value1");

	{
		let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
		cmd_txn.set(&key, value.clone()).unwrap();
		cmd_txn.commit().unwrap();
	}

	let parent = t.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();

	let mut txn = DeferredTransaction::new(
		&parent,
		version,
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let result = txn.get(&key).unwrap();
	assert_eq!(result, Some(value));
}

#[test]
fn test_get_pending_shadows_committed() {
	let mut parent = create_test_transaction();

	let key = make_key("key1");
	parent.set(&key, make_value("old")).unwrap();
	let version = parent.version();

	let mut txn = DeferredTransaction::new(
		&parent,
		version,
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let new_value = make_value("new");
	txn.set(&key, new_value.clone()).unwrap();

	let result = txn.get(&key).unwrap();
	assert_eq!(result, Some(new_value));
}

#[test]
fn test_get_removed_returns_none() {
	let mut parent = create_test_transaction();

	let key = make_key("key1");
	parent.set(&key, make_value("value1")).unwrap();
	let version = parent.version();

	let mut txn = DeferredTransaction::new(
		&parent,
		version,
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	txn.remove(&key).unwrap();

	let result = txn.get(&key).unwrap();
	assert_eq!(result, None);
}

#[test]
fn test_get_nonexistent_key() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let result = txn.get(&make_key("missing")).unwrap();
	assert_eq!(result, None);
}

#[test]
fn test_contains_key_pending() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let key = make_key("key1");
	txn.set(&key, make_value("value1")).unwrap();

	assert!(txn.contains_key(&key).unwrap());
}

#[test]
fn test_contains_key_committed() {
	let t = TestEngine::new();

	let key = make_key("key1");

	{
		let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
		cmd_txn.set(&key, make_value("value1")).unwrap();
		cmd_txn.commit().unwrap();
	}

	let parent = t.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	let mut txn = DeferredTransaction::new(
		&parent,
		version,
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	assert!(txn.contains_key(&key).unwrap());
}

#[test]
fn test_contains_key_removed_returns_false() {
	let mut parent = create_test_transaction();

	let key = make_key("key1");
	parent.set(&key, make_value("value1")).unwrap();
	let version = parent.version();

	let mut txn = DeferredTransaction::new(
		&parent,
		version,
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);
	txn.remove(&key).unwrap();

	assert!(!txn.contains_key(&key).unwrap());
}

#[test]
fn test_contains_key_nonexistent() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	assert!(!txn.contains_key(&make_key("missing")).unwrap());
}

#[test]
fn test_scan_empty() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let mut iter = txn.range(EncodedKeyRange::all(), RangeScope::All, 1024);
	assert!(iter.next().is_none());
}

#[test]
fn test_scan_only_pending() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	txn.set(&make_key("b"), make_value("2")).unwrap();
	txn.set(&make_key("a"), make_value("1")).unwrap();
	txn.set(&make_key("c"), make_value("3")).unwrap();

	let items: Vec<_> =
		txn.range(EncodedKeyRange::all(), RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

	assert_eq!(items.len(), 3);
	assert_eq!(items[0].key, make_key("a"));
	assert_eq!(items[1].key, make_key("b"));
	assert_eq!(items[2].key, make_key("c"));
}

#[test]
fn test_scan_filters_removes() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	txn.set(&make_key("a"), make_value("1")).unwrap();
	txn.remove(&make_key("b")).unwrap();
	txn.set(&make_key("c"), make_value("3")).unwrap();

	let items: Vec<_> =
		txn.range(EncodedKeyRange::all(), RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(items[0].key, make_key("a"));
	assert_eq!(items[1].key, make_key("c"));
}

#[test]
fn test_range_empty() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
	let mut iter = txn.range(range, RangeScope::All, 1024);
	assert!(iter.next().is_none());
}

#[test]
fn test_range_only_pending() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	txn.set(&make_key("a"), make_value("1")).unwrap();
	txn.set(&make_key("b"), make_value("2")).unwrap();
	txn.set(&make_key("c"), make_value("3")).unwrap();
	txn.set(&make_key("d"), make_value("4")).unwrap();

	let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
	let items: Vec<_> = txn.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

	assert_eq!(items.len(), 2);
	assert_eq!(items[0].key, make_key("b"));
	assert_eq!(items[1].key, make_key("c"));
}

#[test]
fn test_prefix_empty() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	let prefix = make_key("test_");
	let iter = txn.prefix(&prefix).unwrap();
	assert!(iter.items.into_iter().next().is_none());
}

#[test]
fn test_prefix_only_pending() {
	let parent = create_test_transaction();
	let mut txn = DeferredTransaction::new(
		&parent,
		CommitVersion(1),
		Catalog::testing(),
		Interceptors::new(),
		Clock::Mock(MockClock::from_millis(1000)),
	);

	txn.set(&make_key("test_a"), make_value("1")).unwrap();
	txn.set(&make_key("test_b"), make_value("2")).unwrap();
	txn.set(&make_key("other_c"), make_value("3")).unwrap();

	let prefix = make_key("test_");
	let iter = txn.prefix(&prefix).unwrap();
	let items: Vec<_> = iter.items.into_iter().collect();

	assert_eq!(items.len(), 2);
	assert_eq!(items[0].key, make_key("test_a"));
	assert_eq!(items[1].key, make_key("test_b"));
}
