//! Integration tests for CDC consumer watermark functionality

use reifydb_catalog::MaterializedCatalog;
use reifydb_cdc::{CdcCheckpoint, compute_watermark};
use reifydb_core::{
	CommitVersion, Result,
	event::EventBus,
	interceptor::StandardInterceptorFactory,
	interface::{CdcConsumerId, Engine},
	util::mock_time_set,
};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion};

fn create_test_engine() -> StandardEngine {
	#[cfg(debug_assertions)]
	mock_time_set(1000);
	let store = TransactionStore::testing_memory();
	let eventbus = EventBus::new();
	let single = TransactionSingleVersion::svl(store.clone(), eventbus.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMultiVersion::optimistic(store, single.clone(), eventbus.clone());

	StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		MaterializedCatalog::new(),
	)
}

#[test]
fn test_compute_watermark_with_no_consumers() -> Result<()> {
	// Given: A fresh engine with no consumer checkpoints
	let engine = create_test_engine();

	// When: Computing the watermark
	let mut txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut txn)?;

	// Then: Should return CommitVersion(1) as safe default
	assert_eq!(watermark, CommitVersion(1), "Watermark with no consumers should be CommitVersion(1)");
	Ok(())
}

#[test]
fn test_compute_watermark_with_single_consumer() -> Result<()> {
	// Given: A consumer at checkpoint version 42
	let engine = create_test_engine();
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(42))?;
	txn.commit()?;

	// When: Computing the watermark
	let mut query_txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut query_txn)?;

	// Then: Watermark should match the single consumer's checkpoint
	assert_eq!(watermark, CommitVersion(42), "Watermark should match single consumer checkpoint");
	Ok(())
}

#[test]
fn test_compute_watermark_with_multiple_consumers_at_same_checkpoint() -> Result<()> {
	// Given: Multiple consumers all at the same checkpoint
	let engine = create_test_engine();
	let checkpoint = CommitVersion(100);

	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), checkpoint)?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), checkpoint)?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), checkpoint)?;
	txn.commit()?;

	// When: Computing the watermark
	let mut query_txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut query_txn)?;

	// Then: Watermark should be the common checkpoint
	assert_eq!(watermark, checkpoint, "Watermark should match when all consumers at same checkpoint");
	Ok(())
}

#[test]
fn test_compute_watermark_finds_minimum_across_consumers() -> Result<()> {
	// Given: Multiple consumers at different checkpoints
	let engine = create_test_engine();

	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(100))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(85))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(95))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer4"), CommitVersion(110))?;
	txn.commit()?;

	// When: Computing the watermark
	let mut query_txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut query_txn)?;

	// Then: Watermark should be the minimum checkpoint (consumer2)
	assert_eq!(watermark, CommitVersion(85), "Watermark should be minimum across all consumers");
	Ok(())
}

#[test]
fn test_compute_watermark_advances_as_slow_consumer_catches_up() -> Result<()> {
	// Given: Two consumers, one lagging behind
	let engine = create_test_engine();

	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("fast_consumer"), CommitVersion(100))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(50))?;
	txn.commit()?;

	// When: Computing initial watermark
	let mut query_txn = engine.begin_query()?;
	let watermark1 = compute_watermark(&mut query_txn)?;

	// Then: Watermark is held back by slow consumer
	assert_eq!(watermark1, CommitVersion(50));

	// Given: Slow consumer catches up
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(80))?;
	txn.commit()?;

	// When: Computing watermark again
	let mut query_txn = engine.begin_query()?;
	let watermark2 = compute_watermark(&mut query_txn)?;

	// Then: Watermark advances
	assert_eq!(watermark2, CommitVersion(80));

	// Given: Slow consumer catches up completely
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(100))?;
	txn.commit()?;

	// When: Computing watermark once more
	let mut query_txn = engine.begin_query()?;
	let watermark3 = compute_watermark(&mut query_txn)?;

	// Then: Watermark is now at both consumers' checkpoint
	assert_eq!(watermark3, CommitVersion(100));
	Ok(())
}

#[test]
fn test_compute_watermark_with_consumer_at_version_one() -> Result<()> {
	// Given: A consumer at the initial version (1)
	let engine = create_test_engine();
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(1))?;
	txn.commit()?;

	// When: Computing the watermark
	let mut query_txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut query_txn)?;

	// Then: Watermark should be CommitVersion(1)
	assert_eq!(watermark, CommitVersion(1), "Watermark should handle consumer at version 1");
	Ok(())
}

#[test]
fn test_compute_watermark_with_very_large_version_numbers() -> Result<()> {
	// Given: Consumers with large version numbers
	let engine = create_test_engine();

	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(u64::MAX - 100))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(u64::MAX - 200))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(u64::MAX - 50))?;
	txn.commit()?;

	// When: Computing the watermark
	let mut query_txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut query_txn)?;

	// Then: Watermark should correctly find minimum
	assert_eq!(watermark, CommitVersion(u64::MAX - 200), "Watermark should handle large version numbers");
	Ok(())
}

#[test]
fn test_compute_watermark_changes_when_new_consumer_added() -> Result<()> {
	// Given: Existing consumers at high checkpoints
	let engine = create_test_engine();

	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(500))?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(510))?;
	txn.commit()?;

	let mut query_txn = engine.begin_query()?;
	let watermark_before = compute_watermark(&mut query_txn)?;
	assert_eq!(watermark_before, CommitVersion(500));

	// When: A new consumer starts at a lower checkpoint
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("new_consumer"), CommitVersion(100))?;
	txn.commit()?;

	// Then: Watermark should drop to the new consumer's checkpoint
	let mut query_txn = engine.begin_query()?;
	let watermark_after = compute_watermark(&mut query_txn)?;
	assert_eq!(watermark_after, CommitVersion(100), "Watermark should be pulled down by new lagging consumer");
	Ok(())
}

#[test]
fn test_compute_watermark_stability_with_consumer_updates() -> Result<()> {
	// Given: A consumer that updates its checkpoint multiple times
	let engine = create_test_engine();

	// Initial checkpoint
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(10))?;
	txn.commit()?;

	let mut query_txn = engine.begin_query()?;
	assert_eq!(compute_watermark(&mut query_txn)?, CommitVersion(10));

	// Update to higher checkpoint
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(20))?;
	txn.commit()?;

	let mut query_txn = engine.begin_query()?;
	assert_eq!(compute_watermark(&mut query_txn)?, CommitVersion(20));

	// Update again
	let mut txn = engine.begin_command()?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(30))?;
	txn.commit()?;

	let mut query_txn = engine.begin_query()?;
	assert_eq!(compute_watermark(&mut query_txn)?, CommitVersion(30));

	Ok(())
}

#[test]
fn test_compute_watermark_with_many_consumers() -> Result<()> {
	// Given: Many consumers at various checkpoints
	let engine = create_test_engine();

	let mut txn = engine.begin_command()?;
	for i in 0..100 {
		let consumer_id = CdcConsumerId::new(&format!("consumer_{}", i));
		let version = CommitVersion(100 + (i * 10)); // Spread out versions
		CdcCheckpoint::persist(&mut txn, &consumer_id, version)?;
	}

	// Add one consumer with minimum version
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("minimum_consumer"), CommitVersion(50))?;
	txn.commit()?;

	// When: Computing watermark
	let mut query_txn = engine.begin_query()?;
	let watermark = compute_watermark(&mut query_txn)?;

	// Then: Should find the minimum among all consumers
	assert_eq!(watermark, CommitVersion(50), "Watermark should find minimum among many consumers");
	Ok(())
}
