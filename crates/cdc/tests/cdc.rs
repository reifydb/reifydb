// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	time::Duration,
};

use Key::Row;
use async_trait::async_trait;
use reifydb_catalog::MaterializedCatalog;
use reifydb_cdc::{CdcConsume, CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	EncodedKey, Result,
	diagnostic::Diagnostic,
	event::EventBus,
	interceptor::StandardInterceptorFactory,
	interface::{
		Cdc, CdcChange, CdcConsumerId, CdcConsumerKey, EncodableKey, Engine as EngineInterface, Key,
		MultiVersionCommandTransaction, MultiVersionQueryTransaction, SourceId, TableId,
	},
	key::RowKey,
	util::{CowVec, mock_time_set},
	value::encoded::EncodedValues,
};
use reifydb_engine::{StandardCommandTransaction, StandardEngine};
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMulti, single::TransactionSingle};
use reifydb_type::{Fragment, RowNumber};
use tokio::time::sleep;

#[tokio::test]
async fn test_consumer_lifecycle() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_id = CdcConsumerId::flow_consumer();

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(100), None);
	let mut test_instance = PollConsumer::new(config, engine, consumer);

	assert!(!test_instance.is_running());

	test_instance.start().expect("Failed to start consumer");
	assert!(test_instance.is_running());

	sleep(Duration::from_millis(50)).await;
	assert!(test_instance.is_running());

	test_instance.stop().expect("Failed to stop consumer");
	assert!(!test_instance.is_running());

	test_instance.stop().expect("Should be able to stop already stopped consumer");

	assert!(!test_instance.is_running());
	Ok(())
}

#[tokio::test]
async fn test_event_processing() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	insert_test_events(&engine, 5).await.expect("Failed to insert test events");

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), None);
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	sleep(Duration::from_millis(200)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 5, "Should have processed 5 changes");

	let transactions = consumer_clone.get_transactions();
	assert_eq!(transactions.len(), 5, "Should have 5 transactions");

	// Each transaction should have one change
	for (i, cdc) in transactions.iter().enumerate() {
		assert_eq!(cdc.changes.len(), 1, "Each transaction should have 1 change");
		if let CdcChange::Insert {
			key,
			..
		} = &cdc.changes[0].change
		{
			if let Some(Row(table_row)) = Key::decode(key) {
				assert_eq!(table_row.source, TableId(1));
				assert_eq!(table_row.row, RowNumber((i + 1) as u64));
			} else {
				panic!("Expected Row key");
			}
		}
	}

	assert!(consumer_clone.get_process_count() >= 1, "Should have processed at least once");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_checkpoint_persistence() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	insert_test_events(&engine, 3).await.expect("Failed to insert test events");

	let config = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(50), None);
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");
	sleep(Duration::from_millis(150)).await;
	test_instance.stop().expect("Failed to stop consumer");

	let changes_first_run = consumer_clone.get_total_changes();
	assert_eq!(changes_first_run, 3, "Should have processed 3 changes in first run");

	insert_test_events(&engine, 2).await.expect("Failed to insert more test events");

	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let config2 = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(50), None);
	let mut test_instance2 = PollConsumer::new(config2, engine.clone(), consumer2);

	test_instance2.start().expect("Failed to start consumer");
	sleep(Duration::from_millis(150)).await;
	test_instance2.stop().expect("Failed to stop consumer");

	let changes_second_run = consumer2_clone.get_total_changes();
	assert_eq!(changes_second_run, 2, "Should have processed only 2 new changes");

	let mut txn = engine.begin_query().await.expect("Failed to begin transaction");
	let consumer_key = CdcConsumerKey {
		consumer: consumer_id,
	}
	.encode();

	let checkpoint =
		txn.get(&consumer_key).await.expect("Failed to get checkpoint").expect("Checkpoint should exist");

	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&checkpoint.values[0..8]);
	let stored_version = u64::from_be_bytes(buffer);

	assert!(stored_version >= 3, "Checkpoint should be after initial events");
	Ok(())
}

#[tokio::test]
async fn test_error_handling() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	insert_test_events(&engine, 3).await.expect("Failed to insert test events");

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), None);
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");
	sleep(Duration::from_millis(100)).await;

	let changes_before_error = consumer_clone.get_total_changes();
	assert_eq!(changes_before_error, 3, "Should have processed 3 changes before error");

	consumer_clone.set_should_fail(true);

	insert_test_events(&engine, 2).await.expect("Failed to insert more test events");
	sleep(Duration::from_millis(150)).await;

	let changes_during_error = consumer_clone.get_total_changes();
	assert_eq!(changes_during_error, 3, "Should not have processed new changes during error");

	consumer_clone.set_should_fail(false);
	sleep(Duration::from_millis(150)).await;

	let changes_after_recovery = consumer_clone.get_total_changes();
	assert_eq!(changes_after_recovery, 5, "Should have processed new changes after recovery");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_empty_events_handling() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), None);
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	sleep(Duration::from_millis(150)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 0, "Should have no changes to process");
	assert_eq!(consumer_clone.get_process_count(), 0, "Should not have called consume");

	insert_test_events(&engine, 1).await.expect("Failed to insert test event");
	sleep(Duration::from_millis(100)).await;

	let changes_after_insert = consumer_clone.get_total_changes();
	assert_eq!(changes_after_insert, 1, "Should have processed 1 change");
	assert!(consumer_clone.get_process_count() >= 1, "Should have called consume");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_multiple_consumers() -> Result<()> {
	let engine = create_test_engine().await?;

	let consumer1 = TestConsumer::new();
	let consumer1_clone = consumer1.clone();
	let consumer_id1 = CdcConsumerId::new("consumer-1");

	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let consumer_id2 = CdcConsumerId::new("consumer-2");

	insert_test_events(&engine, 3).await.expect("Failed to insert test events");

	let config1 = PollConsumerConfig::new(consumer_id1.clone(), Duration::from_millis(50), None);
	let mut test_instance1 = PollConsumer::new(config1, engine.clone(), consumer1);

	let config2 = PollConsumerConfig::new(consumer_id2.clone(), Duration::from_millis(75), None);
	let mut test_instance2 = PollConsumer::new(config2, engine.clone(), consumer2);

	test_instance1.start().expect("Failed to start consumer 1");
	test_instance2.start().expect("Failed to start consumer 2");

	sleep(Duration::from_millis(200)).await;

	let changes1 = consumer1_clone.get_total_changes();
	let changes2 = consumer2_clone.get_total_changes();

	assert_eq!(changes1, 3, "Consumer 1 should have processed 3 changes");
	assert_eq!(changes2, 3, "Consumer 2 should have processed 3 changes");

	insert_test_events(&engine, 2).await.expect("Failed to insert more test events");

	sleep(Duration::from_millis(200)).await;

	let changes1_after = consumer1_clone.get_total_changes();
	let changes2_after = consumer2_clone.get_total_changes();

	assert_eq!(changes1_after, 5, "Consumer 1 should have processed 5 changes total");
	assert_eq!(changes2_after, 5, "Consumer 2 should have processed 5 changes total");

	let mut txn = engine.begin_query().await.expect("Failed to begin transaction");

	let consumer1_key = CdcConsumerKey {
		consumer: consumer_id1,
	}
	.encode();
	let consumer2_key = CdcConsumerKey {
		consumer: consumer_id2,
	}
	.encode();

	let checkpoint1 =
		txn.get(&consumer1_key).await.expect("Failed to get checkpoint 1").expect("Checkpoint 1 should exist");

	let checkpoint2 =
		txn.get(&consumer2_key).await.expect("Failed to get checkpoint 2").expect("Checkpoint 2 should exist");

	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&checkpoint1.values[0..8]);
	let version1 = u64::from_be_bytes(buffer);

	buffer.copy_from_slice(&checkpoint2.values[0..8]);
	let version2 = u64::from_be_bytes(buffer);

	// Both consumers should have processed all events, but their exact
	// checkpoint versions might differ slightly due to independent polling
	// intervals
	assert!(version1 >= 5, "Consumer 1 should have processed all events");
	assert!(version2 >= 5, "Consumer 2 should have processed all events");

	test_instance1.stop().expect("Failed to stop consumer 1");
	test_instance2.stop().expect("Failed to stop consumer 2");
	Ok(())
}

#[tokio::test]
async fn test_non_table_events_filtered() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	let mut txn = engine.begin_command().await.expect("Failed to begin transaction");

	let table_key = RowKey {
		source: SourceId::table(1),
		row: RowNumber(1),
	};
	txn.set(&table_key.encode(), EncodedValues(CowVec::new(b"table_value".to_vec())))
		.await
		.expect("Failed to set table encoded");

	let non_table_key = EncodedKey(CowVec::new(b"non_table_key".to_vec()));
	txn.set(&non_table_key, EncodedValues(CowVec::new(b"non_table_value".to_vec())))
		.await
		.expect("Failed to set non-table encoded");

	txn.commit().await.expect("Failed to commit transaction");

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), None);
	let mut test_instance = PollConsumer::new(config, engine, consumer);

	test_instance.start().expect("Failed to start consumer");
	sleep(Duration::from_millis(150)).await;
	test_instance.stop().expect("Failed to stop consumer");

	// The transaction contains both changes, but it was included because it has at least one table encoded
	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 2, "Should have processed 2 changes (both in same transaction)");

	let transactions = consumer_clone.get_transactions();
	assert_eq!(transactions.len(), 1, "Should have 1 transaction");
	assert_eq!(transactions[0].changes.len(), 2, "Transaction should have 2 changes");

	// Find the table change (could be in any order)
	let table_change = transactions[0]
		.changes
		.iter()
		.find(|c| matches!(Key::decode(c.key()), Some(Row(_))))
		.expect("Should have at least one table change");

	if let CdcChange::Insert {
		key,
		..
	} = &table_change.change
	{
		if let Some(Row(table_row)) = Key::decode(key) {
			assert_eq!(table_row.source, TableId(1));
			assert_eq!(table_row.row, RowNumber(1));
		} else {
			panic!("Expected Row key");
		}
	}
	Ok(())
}

#[tokio::test]
async fn test_rapid_start_stop() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_id = CdcConsumerId::flow_consumer();

	for _ in 0..5 {
		let config = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(100), None);
		let mut test_instance = PollConsumer::new(config, engine.clone(), consumer.clone());

		test_instance.start().expect("Failed to start consumer");
		assert!(test_instance.is_running());

		sleep(Duration::from_millis(10)).await;

		test_instance.stop().expect("Failed to stop consumer");
		assert!(!test_instance.is_running());
	}
	Ok(())
}

#[tokio::test]
async fn test_batch_size_limits_processing() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	// Insert 25 events
	insert_test_events(&engine, 25).await.expect("Failed to insert test events");

	// Set batch size to 10
	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), Some(10));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	// Wait for processing - should take at least 3 cycles (10, 10, 5)
	sleep(Duration::from_millis(300)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 25, "Should have processed all 25 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count >= 3, "Should have been called at least 3 times (for batches of 10, 10, 5)");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_batch_size_one_processes_sequentially() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	// Insert 5 events
	insert_test_events(&engine, 5).await.expect("Failed to insert test events");

	// Set batch size to 1
	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), Some(1));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	// Wait for processing
	sleep(Duration::from_millis(400)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 5, "Should have processed all 5 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count >= 5, "Should have been called at least 5 times (one per event)");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_batch_size_none_processes_all_at_once() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	// Insert 20 events
	insert_test_events(&engine, 20).await.expect("Failed to insert test events");

	// Set batch size to None (unbounded)
	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), None);
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	// Wait for processing
	sleep(Duration::from_millis(150)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 20, "Should have processed all 20 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count <= 2, "Should have been called at most 2 times with unbounded batch");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_batch_size_larger_than_events() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	// Insert 5 events
	insert_test_events(&engine, 5).await.expect("Failed to insert test events");

	// Set batch size to 100 (much larger than number of events)
	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), Some(100));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	// Wait for processing
	sleep(Duration::from_millis(150)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 5, "Should have processed all 5 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count <= 2, "Should have processed efficiently in 1-2 calls");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_batch_size_with_checkpoint_resume() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	// Insert 15 events
	insert_test_events(&engine, 15).await.expect("Failed to insert test events");

	// Set batch size to 5
	let config = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(50), Some(5));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	// Wait for partial processing
	sleep(Duration::from_millis(180)).await;
	test_instance.stop().expect("Failed to stop consumer");

	let changes_first_run = consumer_clone.get_total_changes();
	assert!(changes_first_run >= 5, "Should have processed at least one batch of 5");

	// Insert more events
	insert_test_events(&engine, 3).await.expect("Failed to insert more test events");

	// Start a new consumer with same ID and batch size
	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let config2 = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(50), Some(5));
	let mut test_instance2 = PollConsumer::new(config2, engine.clone(), consumer2);

	test_instance2.start().expect("Failed to start consumer");
	sleep(Duration::from_millis(250)).await;
	test_instance2.stop().expect("Failed to stop consumer");

	let changes_second_run = consumer2_clone.get_total_changes();
	let total_expected = 18 - changes_first_run;
	assert_eq!(changes_second_run, total_expected, "Should have processed remaining events plus new ones");

	test_instance2.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_batch_size_exact_match() -> Result<()> {
	let engine = create_test_engine().await?;
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = CdcConsumerId::flow_consumer();

	// Insert exactly 10 events
	insert_test_events(&engine, 10).await.expect("Failed to insert test events");

	// Set batch size to 10 (exact match)
	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50), Some(10));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	// Wait for processing
	sleep(Duration::from_millis(150)).await;

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 10, "Should have processed all 10 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count <= 2, "Should have processed in 1-2 calls with exact batch size match");

	test_instance.stop().expect("Failed to stop consumer");
	Ok(())
}

#[tokio::test]
async fn test_multiple_consumers_different_batch_sizes() -> Result<()> {
	let engine = create_test_engine().await?;

	let consumer1 = TestConsumer::new();
	let consumer1_clone = consumer1.clone();
	let consumer_id1 = CdcConsumerId::new("consumer-batch-3");

	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let consumer_id2 = CdcConsumerId::new("consumer-unbounded");

	// Insert 10 events
	insert_test_events(&engine, 10).await.expect("Failed to insert test events");

	// Consumer 1 with batch size 3
	let config1 = PollConsumerConfig::new(consumer_id1.clone(), Duration::from_millis(50), Some(3));
	let mut test_instance1 = PollConsumer::new(config1, engine.clone(), consumer1);

	// Consumer 2 with no batch limit (None)
	let config2 = PollConsumerConfig::new(consumer_id2.clone(), Duration::from_millis(75), None);
	let mut test_instance2 = PollConsumer::new(config2, engine.clone(), consumer2);

	test_instance1.start().expect("Failed to start consumer 1");
	test_instance2.start().expect("Failed to start consumer 2");

	// Wait for processing
	sleep(Duration::from_millis(400)).await;

	let changes1 = consumer1_clone.get_total_changes();
	let changes2 = consumer2_clone.get_total_changes();

	// Both should process all events
	assert_eq!(changes1, 10, "Consumer 1 should have processed all 10 changes");
	assert_eq!(changes2, 10, "Consumer 2 should have processed all 10 changes");

	// Consumer 1 should have more process calls due to smaller batch size
	let process_count1 = consumer1_clone.get_process_count();
	let process_count2 = consumer2_clone.get_process_count();

	assert!(process_count1 >= 4, "Consumer 1 should have at least 4 calls (10 events / batch size 3)");
	assert!(process_count2 <= 2, "Consumer 2 should have at most 2 calls (unbounded)");

	test_instance1.stop().expect("Failed to stop consumer 1");
	test_instance2.stop().expect("Failed to stop consumer 2");
	Ok(())
}

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
		None,
	)
	.await)
}

struct TestConsumer {
	cdc_received: Arc<Mutex<Vec<Cdc>>>,
	process_count: Arc<AtomicUsize>,
	should_fail: Arc<AtomicBool>,
}

impl TestConsumer {
	fn new() -> Self {
		Self {
			cdc_received: Arc::new(Mutex::new(Vec::new())),
			process_count: Arc::new(AtomicUsize::new(0)),
			should_fail: Arc::new(AtomicBool::new(false)),
		}
	}

	fn set_should_fail(&self, should_fail: bool) {
		self.should_fail.store(should_fail, Ordering::SeqCst);
	}

	fn get_transactions(&self) -> Vec<Cdc> {
		self.cdc_received.lock().unwrap().clone()
	}

	fn get_total_changes(&self) -> usize {
		self.cdc_received.lock().unwrap().iter().map(|cdc| cdc.changes.len()).sum()
	}

	fn get_process_count(&self) -> usize {
		self.process_count.load(Ordering::SeqCst)
	}
}

impl Clone for TestConsumer {
	fn clone(&self) -> Self {
		Self {
			cdc_received: Arc::clone(&self.cdc_received),
			process_count: Arc::clone(&self.process_count),
			should_fail: Arc::clone(&self.should_fail),
		}
	}
}

#[async_trait]
impl CdcConsume for TestConsumer {
	async fn consume(&self, _txn: &mut StandardCommandTransaction, transactions: Vec<Cdc>) -> Result<()> {
		if self.should_fail.load(Ordering::SeqCst) {
			return Err(reifydb_type::Error(Diagnostic {
				code: "TEST_ERROR".to_string(),
				statement: None,
				message: "Test failure".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
			}));
		}

		let mut received = self.cdc_received.lock().unwrap();
		received.extend(transactions);
		self.process_count.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
}

async fn insert_test_events(engine: &StandardEngine, count: usize) -> Result<()> {
	for i in 0..count {
		let mut txn = engine.begin_command().await?;
		let key = RowKey {
			source: SourceId::table(1),
			row: RowNumber((i + 1) as u64),
		};
		let value = format!("value_{}", i);
		txn.set(&key.encode(), EncodedValues(CowVec::new(value.into_bytes()))).await?;
		txn.commit().await?;
	}

	Ok(())
}
