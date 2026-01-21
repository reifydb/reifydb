// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Integration tests for CDC consumer watermark functionality

use reifydb_cdc::consume::{checkpoint::CdcCheckpoint, watermark::compute_watermark};
use reifydb_cdc::storage::memory::MemoryCdcStorage;
use reifydb_cdc::storage::CdcStorage;
use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::cdc::{Cdc, CdcChange, CdcConsumerId, CdcSequencedChange},
};
use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::util::cowvec::CowVec;

fn make_cdc(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		12345 + version,
		vec![CdcSequencedChange {
			sequence: 1,
			change: CdcChange::Insert {
				key: EncodedKey::new(vec![version as u8]),
				post: EncodedValues(CowVec::new(vec![version as u8])),
			},
		}],
	)
}

#[test]
fn test_compute_watermark_with_single_consumer() {
	let engine = create_test_engine();
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(42)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark, CommitVersion(42), "Watermark should match single consumer checkpoint");
}

#[test]
fn test_compute_watermark_with_multiple_consumers_at_same_checkpoint() {
	let engine = create_test_engine();
	let checkpoint = CommitVersion(100);

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), checkpoint).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), checkpoint).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), checkpoint).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark, checkpoint, "Watermark should match when all consumers at same checkpoint");
}

#[test]
fn test_compute_watermark_finds_minimum_across_consumers() {
	let engine = create_test_engine();

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(100)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(85)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(95)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer4"), CommitVersion(110)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark, CommitVersion(85), "Watermark should be minimum across all consumers");
}

#[test]
fn test_compute_watermark_advances_as_slow_consumer_catches_up() {
	let engine = create_test_engine();

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("fast_consumer"), CommitVersion(100)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(50)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark1 = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark1, CommitVersion(50));

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(80)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark2 = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark2, CommitVersion(80));

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(100)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark3 = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark3, CommitVersion(100));
}

#[test]
fn test_compute_watermark_with_consumer_at_version_one() {
	let engine = create_test_engine();
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(1)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark, CommitVersion(1), "Watermark should handle consumer at version 1");
}

#[test]
fn test_compute_watermark_with_very_large_version_numbers() {
	let engine = create_test_engine();

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(u64::MAX - 100)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(u64::MAX - 200)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(u64::MAX - 50)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark, CommitVersion(u64::MAX - 200), "Watermark should handle large version numbers");
}

#[test]
fn test_compute_watermark_changes_when_new_consumer_added() {
	let engine = create_test_engine();

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(500)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(510)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark_before = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark_before, CommitVersion(500));

	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("new_consumer"), CommitVersion(100)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark_after = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark_after, CommitVersion(100), "Watermark should be pulled down by new lagging consumer");
}

#[test]
fn test_compute_watermark_stability_with_consumer_updates() {
	let engine = create_test_engine();

	// Initial checkpoint
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(10)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	assert_eq!(compute_watermark(&mut query_txn).unwrap(), CommitVersion(10));

	// Update to higher checkpoint
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(20)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	assert_eq!(compute_watermark(&mut query_txn).unwrap(), CommitVersion(20));

	// Update again
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(30)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	assert_eq!(compute_watermark(&mut query_txn).unwrap(), CommitVersion(30));
}

#[test]
fn test_compute_watermark_with_many_consumers() {
	let engine = create_test_engine();

	let mut txn = engine.begin_command().unwrap();
	for i in 0..100 {
		let consumer_id = CdcConsumerId::new(&format!("consumer_{}", i));
		let version = CommitVersion(100 + (i * 10)); // Spread out versions
		CdcCheckpoint::persist(&mut txn, &consumer_id, version).unwrap();
	}

	// Add one consumer with minimum version
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("minimum_consumer"), CommitVersion(50)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	assert_eq!(watermark, CommitVersion(50), "Watermark should find minimum among many consumers");
}

#[test]
fn test_slow_consumer_prevents_cdc_cleanup_until_caught_up() {
	let storage = MemoryCdcStorage::new();
	let engine = create_test_engine();

	// Populate CDC with entries at versions 10, 20, 30, 40, 50
	for version in [10u64, 20, 30, 40, 50] {
		storage.write(&make_cdc(version)).unwrap();
	}
	assert_eq!(storage.len(), 5);

	// Fast consumer at 50, slow consumer at 20
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("fast_consumer"), CommitVersion(50)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(20)).unwrap();
	txn.commit().unwrap();

	// Watermark = min(50, 20) = 20
	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(20));

	// Cleanup: only version 10 removed (< 20)
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 1);
	assert!(storage.read(CommitVersion(10)).unwrap().is_none());
	assert!(storage.read(CommitVersion(20)).unwrap().is_some()); // Retained!
	assert_eq!(storage.len(), 4);

	// Slow consumer catches up to 50
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(50)).unwrap();
	txn.commit().unwrap();

	// Watermark = min(50, 50) = 50
	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(50));

	// Cleanup: versions 20, 30, 40 now removed
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 3);
	assert!(storage.read(CommitVersion(50)).unwrap().is_some()); // Still retained
	assert_eq!(storage.len(), 1);
}

#[test]
fn test_cdc_entry_at_watermark_is_retained() {
	let storage = MemoryCdcStorage::new();
	let engine = create_test_engine();

	// CDC entries at versions 1, 2, 3, 4, 5
	for version in 1..=5 {
		storage.write(&make_cdc(version)).unwrap();
	}

	// Consumer at exactly version 3
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(3)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();

	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 2); // Versions 1, 2 removed
	assert!(storage.read(CommitVersion(3)).unwrap().is_some()); // Version 3 retained!
	assert_eq!(storage.len(), 3); // Versions 3, 4, 5 remain
}

#[test]
fn test_incremental_cleanup_as_slow_consumer_advances() {
	let storage = MemoryCdcStorage::new();
	let engine = create_test_engine();

	// Populate CDC with entries at versions 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
	for version in (10..=100).step_by(10) {
		storage.write(&make_cdc(version)).unwrap();
	}
	assert_eq!(storage.len(), 10);

	// Fast consumer at 100, slow consumer starts at 10
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("fast_consumer"), CommitVersion(100)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(10)).unwrap();
	txn.commit().unwrap();

	// Initial watermark = min(100, 10) = 10, no cleanup possible
	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(10));
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 0);
	assert_eq!(storage.len(), 10);

	// Slow consumer advances to 30
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(30)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(30));
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 2); // Versions 10, 20 removed
	assert_eq!(storage.len(), 8);

	// Slow consumer advances to 70
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(70)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(70));
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 4); // Versions 30, 40, 50, 60 removed
	assert_eq!(storage.len(), 4); // Versions 70, 80, 90, 100 remain

	// Verify remaining entries
	assert!(storage.read(CommitVersion(70)).unwrap().is_some());
	assert!(storage.read(CommitVersion(80)).unwrap().is_some());
	assert!(storage.read(CommitVersion(90)).unwrap().is_some());
	assert!(storage.read(CommitVersion(100)).unwrap().is_some());
}

#[test]
fn test_multiple_slow_consumers_constrain_cleanup() {
	let storage = MemoryCdcStorage::new();
	let engine = create_test_engine();

	// Populate CDC with entries at versions 10, 20, 30, 40, 50
	for version in [10u64, 20, 30, 40, 50] {
		storage.write(&make_cdc(version)).unwrap();
	}
	assert_eq!(storage.len(), 5);

	// Three consumers: fast=50, medium=30, slow=20
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("fast_consumer"), CommitVersion(50)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("medium_consumer"), CommitVersion(30)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(20)).unwrap();
	txn.commit().unwrap();

	// Watermark = min(50, 30, 20) = 20
	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(20));

	// Only version 10 can be cleaned up
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 1);
	assert_eq!(storage.len(), 4);

	// Slow consumer catches up to medium (30), but medium is still the minimum
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(35)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(30)); // medium_consumer is now the slowest

	// Version 20 can now be cleaned up
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 1);
	assert_eq!(storage.len(), 3); // Versions 30, 40, 50 remain

	// All consumers catch up to 50
	let mut txn = engine.begin_command().unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(50)).unwrap();
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("medium_consumer"), CommitVersion(50)).unwrap();
	txn.commit().unwrap();

	let mut query_txn = engine.begin_query().unwrap();
	let watermark = compute_watermark(&mut query_txn).unwrap();
	assert_eq!(watermark, CommitVersion(50));

	// Versions 30, 40 can now be cleaned up
	let result = storage.drop_before(watermark).unwrap();
	assert_eq!(result.count, 2);
	assert_eq!(storage.len(), 1); // Only version 50 remains
	assert!(storage.read(CommitVersion(50)).unwrap().is_some());
}
