// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::encoded::key::{EncodedKey, EncodedKeyRange};
use reifydb_transaction::multi::conflict::ConflictManager;

fn make_key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes().to_vec())
}

#[test]
fn test_basic_conflict_detection() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	let key = make_key("test");
	cm1.mark_read(&key);
	cm2.mark_write(&key);

	assert!(cm1.has_conflict(&cm2));
	assert!(!cm2.has_conflict(&cm1));
}

#[test]
fn test_write_write_conflict() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	let key = make_key("test");
	cm1.mark_write(&key);
	cm2.mark_write(&key);

	assert!(cm1.has_conflict(&cm2));
	assert!(cm2.has_conflict(&cm1));
}

#[test]
fn test_no_conflict_different_keys() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	cm1.mark_read(&make_key("key1"));
	cm1.mark_write(&make_key("key1"));
	cm2.mark_read(&make_key("key2"));
	cm2.mark_write(&make_key("key2"));

	assert!(!cm1.has_conflict(&cm2));
	assert!(!cm2.has_conflict(&cm1));
}

#[test]
fn test_range_conflict() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	cm1.mark_range(EncodedKeyRange::parse("a..z"));
	cm2.mark_write(&make_key("m"));

	assert!(cm1.has_conflict(&cm2));
}

#[test]
fn test_deduplication() {
	let mut cm = ConflictManager::new();
	let key = make_key("test");

	cm.mark_read(&key);
	cm.mark_read(&key);
	cm.mark_read(&key);

	assert_eq!(cm.get_read_keys().len(), 1);
}

#[test]
fn test_performance_with_many_keys() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	for i in 0..1000 {
		cm1.mark_read(&make_key(&format!("read_{}", i)));
		cm2.mark_write(&make_key(&format!("write_{}", i)));
	}

	let shared_key = make_key("shared");
	cm1.mark_read(&shared_key);
	cm2.mark_write(&shared_key);

	assert!(cm1.has_conflict(&cm2));
}

#[test]
fn test_iter_functionality() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	cm1.mark_iter();
	cm2.mark_write(&make_key("any_key"));

	assert!(cm1.has_conflict(&cm2));
}

#[test]
fn test_sweep_line_many_ranges_many_keys() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	for i in 0..20 {
		let start = format!("r_{:02}_a", i);
		let end = format!("r_{:02}_z", i);
		cm1.mark_range(EncodedKeyRange::parse(&format!("{}..{}", start, end)));
	}

	for i in 0..100 {
		cm2.mark_write(&make_key(&format!("write_{:04}", i)));
	}
	cm2.mark_write(&make_key("r_10_m"));

	assert!(cm1.has_conflict(&cm2));
}

#[test]
fn test_sweep_line_no_conflict() {
	let mut cm1 = ConflictManager::new();
	let mut cm2 = ConflictManager::new();

	for i in 0..10 {
		let start = format!("r_{:02}_a", i);
		let end = format!("r_{:02}_z", i);
		cm1.mark_range(EncodedKeyRange::parse(&format!("{}..{}", start, end)));
	}

	for i in 0..100 {
		cm2.mark_write(&make_key(&format!("write_{:04}", i)));
	}

	assert!(!cm1.has_conflict(&cm2));
}

#[test]
fn test_disabled_marks_are_noop() {
	let mut cm = ConflictManager::new();
	cm.set_disabled();

	cm.reserve_writes(10_000);

	let k = make_key("k");
	cm.mark_read(&k);
	cm.mark_write(&k);
	cm.mark_range(EncodedKeyRange::parse("a..z"));
	cm.mark_iter();

	assert!(cm.get_read_keys().is_empty());
	assert!(cm.get_write_keys().is_empty());
	assert!(!cm.has_range_operations());

	let mut other = ConflictManager::new();
	other.mark_write(&k);
	assert!(!cm.has_conflict(&other));
}
