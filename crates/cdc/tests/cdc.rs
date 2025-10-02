// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread,
	time::Duration,
};

use Key::Row;
use reifydb_catalog::MaterializedCatalog;
use reifydb_cdc::{CdcConsume, CdcConsumer, PollConsumer, PollConsumerConfig};
use reifydb_core::{
	EncodedKey, Result,
	diagnostic::Diagnostic,
	event::EventBus,
	interceptor::StandardInterceptorFactory,
	interface::{
		Cdc, CdcChange, CdcConsumerKey, ConsumerId, EncodableKey, Engine as EngineInterface, Key,
		MultiVersionCommandTransaction, QueryTransaction, SingleVersionQueryTransaction, SourceId, TableId,
	},
	key::RowKey,
	util::{CowVec, mock_time_set},
	value::encoded::EncodedValues,
};
use reifydb_engine::{EngineTransaction, StandardCdcTransaction, StandardCommandTransaction, StandardEngine};
use reifydb_store_transaction::memory::Memory;
use reifydb_transaction::{mvcc::transaction::serializable::Serializable, svl::SingleVersionLock};
use reifydb_type::{OwnedFragment, RowNumber};

#[test]
fn test_consumer_lifecycle() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_id = ConsumerId::flow_consumer();

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(100));
	let mut test_instance = PollConsumer::new(config, engine, consumer);

	assert!(!test_instance.is_running());

	test_instance.start().expect("Failed to start consumer");
	assert!(test_instance.is_running());

	thread::sleep(Duration::from_millis(50));
	assert!(test_instance.is_running());

	test_instance.stop().expect("Failed to stop consumer");
	assert!(!test_instance.is_running());

	test_instance.stop().expect("Should be able to stop already stopped consumer");

	assert!(!test_instance.is_running());
}

#[test]
fn test_event_processing() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	insert_test_events(&engine, 5).expect("Failed to insert test events");

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	thread::sleep(Duration::from_millis(200));

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
}

#[test]
fn test_checkpoint_persistence() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	insert_test_events(&engine, 3).expect("Failed to insert test events");

	let config = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(50));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(150));
	test_instance.stop().expect("Failed to stop consumer");

	let changes_first_run = consumer_clone.get_total_changes();
	assert_eq!(changes_first_run, 3, "Should have processed 3 changes in first run");

	insert_test_events(&engine, 2).expect("Failed to insert more test events");

	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let config2 = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(50));
	let mut test_instance2 = PollConsumer::new(config2, engine.clone(), consumer2);

	test_instance2.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(150));
	test_instance2.stop().expect("Failed to stop consumer");

	let changes_second_run = consumer2_clone.get_total_changes();
	assert_eq!(changes_second_run, 2, "Should have processed only 2 new changes");

	let txn = engine.begin_query().expect("Failed to begin transaction");
	let mut txn = txn.begin_single_query().expect("Failed to begin transaction");
	let consumer_key = CdcConsumerKey {
		consumer: consumer_id,
	}
	.encode();

	let checkpoint = txn.get(&consumer_key).expect("Failed to get checkpoint").expect("Checkpoint should exist");

	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&checkpoint.values[0..8]);
	let stored_version = u64::from_be_bytes(buffer);

	assert!(stored_version >= 3, "Checkpoint should be after initial events");
}

#[test]
fn test_error_handling() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	insert_test_events(&engine, 3).expect("Failed to insert test events");

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(100));

	let changes_before_error = consumer_clone.get_total_changes();
	assert_eq!(changes_before_error, 3, "Should have processed 3 changes before error");

	consumer_clone.set_should_fail(true);

	insert_test_events(&engine, 2).expect("Failed to insert more test events");
	thread::sleep(Duration::from_millis(150));

	let changes_during_error = consumer_clone.get_total_changes();
	assert_eq!(changes_during_error, 3, "Should not have processed new changes during error");

	consumer_clone.set_should_fail(false);
	thread::sleep(Duration::from_millis(150));

	let changes_after_recovery = consumer_clone.get_total_changes();
	assert_eq!(changes_after_recovery, 5, "Should have processed new changes after recovery");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_empty_events_handling() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50));
	let mut test_instance = PollConsumer::new(config, engine.clone(), consumer);

	test_instance.start().expect("Failed to start consumer");

	thread::sleep(Duration::from_millis(150));

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 0, "Should have no changes to process");
	assert_eq!(consumer_clone.get_process_count(), 0, "Should not have called consume");

	insert_test_events(&engine, 1).expect("Failed to insert test event");
	thread::sleep(Duration::from_millis(100));

	let changes_after_insert = consumer_clone.get_total_changes();
	assert_eq!(changes_after_insert, 1, "Should have processed 1 change");
	assert!(consumer_clone.get_process_count() >= 1, "Should have called consume");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_multiple_consumers() {
	let engine = create_test_engine();

	let consumer1 = TestConsumer::new();
	let consumer1_clone = consumer1.clone();
	let consumer_id1 = ConsumerId::new("consumer-1");

	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let consumer_id2 = ConsumerId::new("consumer-2");

	insert_test_events(&engine, 3).expect("Failed to insert test events");

	let config1 = PollConsumerConfig::new(consumer_id1.clone(), Duration::from_millis(50));
	let mut test_instance1 = PollConsumer::new(config1, engine.clone(), consumer1);

	let config2 = PollConsumerConfig::new(consumer_id2.clone(), Duration::from_millis(75));
	let mut test_instance2 = PollConsumer::new(config2, engine.clone(), consumer2);

	test_instance1.start().expect("Failed to start consumer 1");
	test_instance2.start().expect("Failed to start consumer 2");

	thread::sleep(Duration::from_millis(200));

	let changes1 = consumer1_clone.get_total_changes();
	let changes2 = consumer2_clone.get_total_changes();

	assert_eq!(changes1, 3, "Consumer 1 should have processed 3 changes");
	assert_eq!(changes2, 3, "Consumer 2 should have processed 3 changes");

	insert_test_events(&engine, 2).expect("Failed to insert more test events");

	thread::sleep(Duration::from_millis(200));

	let changes1_after = consumer1_clone.get_total_changes();
	let changes2_after = consumer2_clone.get_total_changes();

	assert_eq!(changes1_after, 5, "Consumer 1 should have processed 5 changes total");
	assert_eq!(changes2_after, 5, "Consumer 2 should have processed 5 changes total");

	let txn = engine.begin_query().expect("Failed to begin transaction");
	let mut txn = txn.begin_single_query().unwrap();

	let consumer1_key = CdcConsumerKey {
		consumer: consumer_id1,
	}
	.encode();
	let consumer2_key = CdcConsumerKey {
		consumer: consumer_id2,
	}
	.encode();

	let checkpoint1 =
		txn.get(&consumer1_key).expect("Failed to get checkpoint 1").expect("Checkpoint 1 should exist");

	let checkpoint2 =
		txn.get(&consumer2_key).expect("Failed to get checkpoint 2").expect("Checkpoint 2 should exist");

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
}

#[test]
fn test_non_table_events_filtered() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	let mut txn = engine.begin_command().expect("Failed to begin transaction");

	let table_key = RowKey {
		source: SourceId::table(1),
		row: RowNumber(1),
	};
	txn.set(&table_key.encode(), EncodedValues(CowVec::new(b"table_value".to_vec())))
		.expect("Failed to set table encoded");

	let non_table_key = EncodedKey(CowVec::new(b"non_table_key".to_vec()));
	txn.set(&non_table_key, EncodedValues(CowVec::new(b"non_table_value".to_vec())))
		.expect("Failed to set non-table encoded");

	txn.commit().expect("Failed to commit transaction");

	let config = PollConsumerConfig::new(consumer_id, Duration::from_millis(50));
	let mut test_instance = PollConsumer::new(config, engine, consumer);

	test_instance.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(150));
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
}

#[test]
fn test_rapid_start_stop() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_id = ConsumerId::flow_consumer();

	for _ in 0..5 {
		let config = PollConsumerConfig::new(consumer_id.clone(), Duration::from_millis(100));
		let mut test_instance = PollConsumer::new(config, engine.clone(), consumer.clone());

		test_instance.start().expect("Failed to start consumer");
		assert!(test_instance.is_running());

		thread::sleep(Duration::from_millis(10));

		test_instance.stop().expect("Failed to stop consumer");
		assert!(!test_instance.is_running());
	}
}

type TestTransaction = EngineTransaction<
	Serializable<Memory, SingleVersionLock<Memory>>,
	SingleVersionLock<Memory>,
	StandardCdcTransaction<Memory>,
>;

fn create_test_engine() -> StandardEngine<TestTransaction> {
	#[cfg(debug_assertions)]
	mock_time_set(1000);
	let memory = Memory::new();
	let eventbus = EventBus::new();
	let single = SingleVersionLock::new(memory.clone(), eventbus.clone());
	let cdc = StandardCdcTransaction::new(memory.clone());
	let multi = Serializable::new(memory, single.clone(), eventbus.clone());

	StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		MaterializedCatalog::new(),
	)
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

impl CdcConsume<TestTransaction> for TestConsumer {
	fn consume(
		&self,
		_txn: &mut StandardCommandTransaction<TestTransaction>,
		transactions: Vec<Cdc>,
	) -> Result<()> {
		if self.should_fail.load(Ordering::SeqCst) {
			return Err(reifydb_type::Error(Diagnostic {
				code: "TEST_ERROR".to_string(),
				statement: None,
				message: "Test failure".to_string(),
				column: None,
				fragment: OwnedFragment::None,
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

fn insert_test_events(engine: &StandardEngine<TestTransaction>, count: usize) -> Result<()> {
	for i in 0..count {
		let mut txn = engine.begin_command()?;
		let key = RowKey {
			source: SourceId::table(1),
			row: RowNumber((i + 1) as u64),
		};
		let value = format!("value_{}", i);
		txn.set(&key.encode(), EncodedValues(CowVec::new(value.into_bytes())))?;
		txn.commit()?;
	}

	Ok(())
}
