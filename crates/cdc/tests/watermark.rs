// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Integration tests for CDC consumer watermark functionality

use reifydb_cdc::consume::{checkpoint::CdcCheckpoint, watermark::compute_watermark};
use reifydb_core::{common::CommitVersion, interface::cdc::CdcConsumerId};
use reifydb_engine::test_utils::create_test_engine;

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
