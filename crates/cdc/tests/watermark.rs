// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Integration tests for CDC consumer watermark functionality

use reifydb_catalog::Catalog;
use reifydb_cdc::{CdcCheckpoint, compute_watermark};
use reifydb_core::{
	CommitVersion, Result, event::EventBus, interface::CdcConsumerId, ioc::IocContainer, util::mock_time_set,
};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{
	cdc::TransactionCdc, interceptor::StandardInterceptorFactory, multi::TransactionMulti,
	single::TransactionSingle,
};

async fn create_test_engine() -> Result<StandardEngine> {
	#[cfg(debug_assertions)]
	mock_time_set(1000);
	let store = TransactionStore::testing_memory().await;
	let eventbus = EventBus::new();
	let single = TransactionSingle::svl(store.clone(), eventbus.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMulti::new(store, single.clone(), eventbus.clone()).await?;

	Ok(StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		Catalog::default(),
		None,
		IocContainer::new(),
	)
	.await)
}

#[tokio::test]
async fn test_compute_watermark_with_no_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut txn).await?;

	assert_eq!(watermark, CommitVersion(1), "Watermark with no consumers should be CommitVersion(1)");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_with_single_consumer() -> Result<()> {
	let engine = create_test_engine().await?;
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(42)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark, CommitVersion(42), "Watermark should match single consumer checkpoint");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_with_multiple_consumers_at_same_checkpoint() -> Result<()> {
	let engine = create_test_engine().await?;
	let checkpoint = CommitVersion(100);

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), checkpoint).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), checkpoint).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), checkpoint).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark, checkpoint, "Watermark should match when all consumers at same checkpoint");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_finds_minimum_across_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(100)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(85)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(95)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer4"), CommitVersion(110)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark, CommitVersion(85), "Watermark should be minimum across all consumers");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_advances_as_slow_consumer_catches_up() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("fast_consumer"), CommitVersion(100)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(50)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark1 = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark1, CommitVersion(50));

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(80)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark2 = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark2, CommitVersion(80));

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("slow_consumer"), CommitVersion(100)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark3 = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark3, CommitVersion(100));
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_with_consumer_at_version_one() -> Result<()> {
	let engine = create_test_engine().await?;
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(1)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark, CommitVersion(1), "Watermark should handle consumer at version 1");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_with_very_large_version_numbers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(u64::MAX - 100)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(u64::MAX - 200)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(u64::MAX - 50)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark, CommitVersion(u64::MAX - 200), "Watermark should handle large version numbers");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_changes_when_new_consumer_added() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(500)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(510)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark_before = compute_watermark(&mut query_txn).await?;
	assert_eq!(watermark_before, CommitVersion(500));

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("new_consumer"), CommitVersion(100)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark_after = compute_watermark(&mut query_txn).await?;
	assert_eq!(watermark_after, CommitVersion(100), "Watermark should be pulled down by new lagging consumer");
	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_stability_with_consumer_updates() -> Result<()> {
	let engine = create_test_engine().await?;

	// Initial checkpoint
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(10)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	assert_eq!(compute_watermark(&mut query_txn).await?, CommitVersion(10));

	// Update to higher checkpoint
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(20)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	assert_eq!(compute_watermark(&mut query_txn).await?, CommitVersion(20));

	// Update again
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer"), CommitVersion(30)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	assert_eq!(compute_watermark(&mut query_txn).await?, CommitVersion(30));

	Ok(())
}

#[tokio::test]
async fn test_compute_watermark_with_many_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	for i in 0..100 {
		let consumer_id = CdcConsumerId::new(&format!("consumer_{}", i));
		let version = CommitVersion(100 + (i * 10)); // Spread out versions
		CdcCheckpoint::persist(&mut txn, &consumer_id, version).await?;
	}

	// Add one consumer with minimum version
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("minimum_consumer"), CommitVersion(50)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let watermark = compute_watermark(&mut query_txn).await?;

	assert_eq!(watermark, CommitVersion(50), "Watermark should find minimum among many consumers");
	Ok(())
}
