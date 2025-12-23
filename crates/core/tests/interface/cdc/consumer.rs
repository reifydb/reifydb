//! Integration tests for CDC consumer state retrieval functionality

use reifydb_catalog::MaterializedCatalog;
use reifydb_cdc::CdcCheckpoint;
use reifydb_core::{
	CommitVersion, Result,
	event::EventBus,
	interceptor::StandardInterceptorFactory,
	interface::{CdcConsumerId, Engine, get_all_consumer_states},
	util::mock_time_set,
};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMulti, single::TransactionSingle};

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
		MaterializedCatalog::new(),
	))
}

#[tokio::test]
async fn test_get_all_consumer_states_with_no_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut txn).await?;

	assert_eq!(states.len(), 0, "Should return empty vec when no consumers exist");
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_with_single_consumer() -> Result<()> {
	let engine = create_test_engine().await?;
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(42)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), 1, "Should return exactly one consumer");
	assert_eq!(states[0].consumer_id.as_ref(), "consumer1");
	assert_eq!(states[0].checkpoint, CommitVersion(42));
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_with_multiple_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(100)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(85)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(95)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), 3, "Should return all three consumers");

	// Find each consumer by ID and verify checkpoint
	let consumer1 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer1").unwrap();
	let consumer2 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer2").unwrap();
	let consumer3 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer3").unwrap();

	assert_eq!(consumer1.checkpoint, CommitVersion(100));
	assert_eq!(consumer2.checkpoint, CommitVersion(85));
	assert_eq!(consumer3.checkpoint, CommitVersion(95));
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_returns_all_consumer_ids() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	let consumer_names = vec!["alpha", "beta", "gamma", "delta", "epsilon"];
	for (i, name) in consumer_names.iter().enumerate() {
		CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new(*name), CommitVersion((i + 1) as u64 * 10))
			.await?;
	}
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), consumer_names.len());

	// Verify all consumer IDs are present
	for name in consumer_names {
		assert!(
			states.iter().any(|s| s.consumer_id.as_ref() == name),
			"Consumer '{}' should be in results",
			name
		);
	}
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_updates_after_checkpoint_change() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(50)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states_before = get_all_consumer_states(&mut query_txn).await?;
	assert_eq!(states_before.len(), 1);
	assert_eq!(states_before[0].checkpoint, CommitVersion(50));

	// Update checkpoint
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(75)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states_after = get_all_consumer_states(&mut query_txn).await?;
	assert_eq!(states_after.len(), 1);
	assert_eq!(states_after[0].checkpoint, CommitVersion(75), "Checkpoint should be updated");
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_with_new_consumer_added() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(500)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(510)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states_before = get_all_consumer_states(&mut query_txn).await?;
	assert_eq!(states_before.len(), 2);

	// Add a new consumer
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("new_consumer"), CommitVersion(100)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states_after = get_all_consumer_states(&mut query_txn).await?;
	assert_eq!(states_after.len(), 3, "Should now have three consumers");

	let new_consumer = states_after
		.iter()
		.find(|s| s.consumer_id.as_ref() == "new_consumer")
		.expect("New consumer should be present");
	assert_eq!(new_consumer.checkpoint, CommitVersion(100));
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_with_large_version_numbers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(u64::MAX - 100)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer2"), CommitVersion(u64::MAX - 200)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer3"), CommitVersion(u64::MAX - 50)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), 3);

	let consumer1 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer1").unwrap();
	let consumer2 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer2").unwrap();
	let consumer3 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer3").unwrap();

	assert_eq!(consumer1.checkpoint, CommitVersion(u64::MAX - 100));
	assert_eq!(consumer2.checkpoint, CommitVersion(u64::MAX - 200));
	assert_eq!(consumer3.checkpoint, CommitVersion(u64::MAX - 50));
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_with_many_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	for i in 0..100 {
		let consumer_id = CdcConsumerId::new(&format!("consumer_{}", i));
		let version = CommitVersion(100 + (i * 10));
		CdcCheckpoint::persist(&mut txn, &consumer_id, version).await?;
	}
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), 100, "Should return all 100 consumers");

	// Verify a few specific consumers
	let consumer_0 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer_0").unwrap();
	assert_eq!(consumer_0.checkpoint, CommitVersion(100));

	let consumer_50 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer_50").unwrap();
	assert_eq!(consumer_50.checkpoint, CommitVersion(100 + 50 * 10));

	let consumer_99 = states.iter().find(|s| s.consumer_id.as_ref() == "consumer_99").unwrap();
	assert_eq!(consumer_99.checkpoint, CommitVersion(100 + 99 * 10));
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_with_consumer_at_version_one() -> Result<()> {
	let engine = create_test_engine().await?;
	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("consumer1"), CommitVersion(1)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), 1);
	assert_eq!(states[0].checkpoint, CommitVersion(1), "Should handle version 1");
	Ok(())
}

#[tokio::test]
async fn test_get_all_consumer_states_preserves_order_independence() -> Result<()> {
	let engine = create_test_engine().await?;

	let mut txn = engine.begin_command().await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("zebra"), CommitVersion(10)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("alpha"), CommitVersion(20)).await?;
	CdcCheckpoint::persist(&mut txn, &CdcConsumerId::new("middle"), CommitVersion(30)).await?;
	txn.commit().await?;

	let mut query_txn = engine.begin_query().await?;
	let states = get_all_consumer_states(&mut query_txn).await?;

	assert_eq!(states.len(), 3);

	// Verify all consumers are present regardless of insertion order
	assert!(states.iter().any(|s| s.consumer_id.as_ref() == "zebra" && s.checkpoint == CommitVersion(10)));
	assert!(states.iter().any(|s| s.consumer_id.as_ref() == "alpha" && s.checkpoint == CommitVersion(20)));
	assert!(states.iter().any(|s| s.consumer_id.as_ref() == "middle" && s.checkpoint == CommitVersion(30)));
	Ok(())
}
