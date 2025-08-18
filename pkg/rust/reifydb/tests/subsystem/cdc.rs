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

use Key::TableRow;
use reifydb::subsystem::cdc::PollConsumer;
use reifydb_core::{
	EncodedKey, Result, RowId,
	diagnostic::Diagnostic,
	hook::Hooks,
	interface::{
		CdcConsume, CdcConsumer, CdcConsumerKey, CdcEvent,
		CommandTransaction, ConsumerId, EncodableKey,
		Engine as EngineInterface, Key, StandardCdcTransaction,
		StandardTransaction, TableId, VersionedCommandTransaction,
		VersionedQueryTransaction, key::TableRowKey,
	},
	row::EncodedRow,
	util::{CowVec, MockClock},
};
use reifydb_engine::StandardEngine;
use reifydb_storage::memory::Memory;
use reifydb_transaction::{
	mvcc::transaction::serializable::Serializable, svl::SingleVersionLock,
};

#[test]
fn test_consumer_lifecycle() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_id = ConsumerId::flow_consumer();

	let mut test_instance = PollConsumer::new(
		consumer_id,
		Duration::from_millis(100),
		engine,
		consumer,
	);

	assert!(!test_instance.is_running());

	test_instance.start().expect("Failed to start consumer");
	assert!(test_instance.is_running());

	thread::sleep(Duration::from_millis(50));
	assert!(test_instance.is_running());

	test_instance.stop().expect("Failed to stop consumer");
	assert!(!test_instance.is_running());

	test_instance
		.stop()
		.expect("Should be able to stop already stopped consumer");

	assert!(!test_instance.is_running());
}

#[test]
fn test_event_processing() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	insert_test_events(&engine, 5).expect("Failed to insert test events");

	let mut test_instance = PollConsumer::new(
		consumer_id,
		Duration::from_millis(50),
		engine.clone(),
		consumer,
	);

	test_instance.start().expect("Failed to start consumer");

	thread::sleep(Duration::from_millis(200));

	let events = consumer_clone.get_events();
	assert_eq!(events.len(), 5, "Should have processed 5 events");

	for (i, event) in events.iter().enumerate() {
		if let Some(TableRow(table_row)) = Key::decode(event.key()) {
			assert_eq!(table_row.table, TableId(1));
			assert_eq!(table_row.row, RowId(i as u64));
		} else {
			panic!("Expected TableRow key");
		}
	}

	assert!(
		consumer_clone.get_process_count() >= 1,
		"Should have processed at least once"
	);

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_checkpoint_persistence() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	insert_test_events(&engine, 3).expect("Failed to insert test events");

	let mut test_instance = PollConsumer::new(
		consumer_id.clone(),
		Duration::from_millis(50),
		engine.clone(),
		consumer,
	);

	test_instance.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(150));
	test_instance.stop().expect("Failed to stop consumer");

	let events_first_run = consumer_clone.get_events();
	assert_eq!(
		events_first_run.len(),
		3,
		"Should have processed 3 events in first run"
	);

	insert_test_events(&engine, 2)
		.expect("Failed to insert more test events");

	let consumer2 = TestConsumer::new();
	let consumer2_clone = consumer2.clone();
	let mut test_instance2 = PollConsumer::new(
		consumer_id.clone(),
		Duration::from_millis(50),
		engine.clone(),
		consumer2,
	);

	test_instance2.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(150));
	test_instance2.stop().expect("Failed to stop consumer");

	let events_second_run = consumer2_clone.get_events();
	assert_eq!(
		events_second_run.len(),
		2,
		"Should have processed only 2 new events"
	);

	let mut txn =
		engine.begin_query().expect("Failed to begin transaction");
	let consumer_key = CdcConsumerKey {
		consumer: consumer_id,
	}
	.encode();

	let checkpoint = txn
		.get(&consumer_key)
		.expect("Failed to get checkpoint")
		.expect("Checkpoint should exist");

	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&checkpoint.row[0..8]);
	let stored_version = u64::from_be_bytes(buffer);

	assert!(
		stored_version >= 3,
		"Checkpoint should be after initial events"
	);
}

#[test]
fn test_error_handling() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	insert_test_events(&engine, 3).expect("Failed to insert test events");

	let mut test_instance = PollConsumer::new(
		consumer_id,
		Duration::from_millis(50),
		engine.clone(),
		consumer,
	);

	test_instance.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(100));

	let events_before_error = consumer_clone.get_events();
	assert_eq!(
		events_before_error.len(),
		3,
		"Should have processed 3 events before error"
	);

	consumer_clone.set_should_fail(true);

	insert_test_events(&engine, 2)
		.expect("Failed to insert more test events");
	thread::sleep(Duration::from_millis(150));

	let events_during_error = consumer_clone.get_events();
	assert_eq!(
		events_during_error.len(),
		3,
		"Should not have processed new events during error"
	);

	consumer_clone.set_should_fail(false);
	thread::sleep(Duration::from_millis(150));

	let events_after_recovery = consumer_clone.get_events();
	assert_eq!(
		events_after_recovery.len(),
		5,
		"Should have processed new events after recovery"
	);

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_empty_events_handling() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_clone = consumer.clone();
	let consumer_id = ConsumerId::flow_consumer();

	let mut test_instance = PollConsumer::new(
		consumer_id,
		Duration::from_millis(50),
		engine.clone(),
		consumer,
	);

	test_instance.start().expect("Failed to start consumer");

	thread::sleep(Duration::from_millis(150));

	let events = consumer_clone.get_events();
	assert_eq!(events.len(), 0, "Should have no events to process");
	assert_eq!(
		consumer_clone.get_process_count(),
		0,
		"Should not have called consume"
	);

	insert_test_events(&engine, 1).expect("Failed to insert test event");
	thread::sleep(Duration::from_millis(100));

	let events_after_insert = consumer_clone.get_events();
	assert_eq!(
		events_after_insert.len(),
		1,
		"Should have processed 1 event"
	);
	assert!(
		consumer_clone.get_process_count() >= 1,
		"Should have called consume"
	);

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

	let mut test_instance1 = PollConsumer::new(
		consumer_id1.clone(),
		Duration::from_millis(50),
		engine.clone(),
		consumer1,
	);

	let mut test_instance2 = PollConsumer::new(
		consumer_id2.clone(),
		Duration::from_millis(75),
		engine.clone(),
		consumer2,
	);

	test_instance1.start().expect("Failed to start consumer 1");
	test_instance2.start().expect("Failed to start consumer 2");

	thread::sleep(Duration::from_millis(200));

	let events1 = consumer1_clone.get_events();
	let events2 = consumer2_clone.get_events();

	assert_eq!(
		events1.len(),
		3,
		"Consumer 1 should have processed 3 events"
	);
	assert_eq!(
		events2.len(),
		3,
		"Consumer 2 should have processed 3 events"
	);

	insert_test_events(&engine, 2)
		.expect("Failed to insert more test events");

	thread::sleep(Duration::from_millis(200));

	let events1_after = consumer1_clone.get_events();
	let events2_after = consumer2_clone.get_events();

	assert_eq!(
		events1_after.len(),
		5,
		"Consumer 1 should have processed 5 events total"
	);
	assert_eq!(
		events2_after.len(),
		5,
		"Consumer 2 should have processed 5 events total"
	);

	let mut txn =
		engine.begin_query().expect("Failed to begin transaction");

	let consumer1_key = CdcConsumerKey {
		consumer: consumer_id1,
	}
	.encode();
	let consumer2_key = CdcConsumerKey {
		consumer: consumer_id2,
	}
	.encode();

	let checkpoint1 = txn
		.get(&consumer1_key)
		.expect("Failed to get checkpoint 1")
		.expect("Checkpoint 1 should exist");

	let checkpoint2 = txn
		.get(&consumer2_key)
		.expect("Failed to get checkpoint 2")
		.expect("Checkpoint 2 should exist");

	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&checkpoint1.row[0..8]);
	let version1 = u64::from_be_bytes(buffer);

	buffer.copy_from_slice(&checkpoint2.row[0..8]);
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

	let mut txn =
		engine.begin_command().expect("Failed to begin transaction");

	let table_key = TableRowKey {
		table: TableId(1),
		row: RowId(1),
	};
	txn.set(
		&table_key.encode(),
		EncodedRow(CowVec::new(b"table_value".to_vec())),
	)
	.expect("Failed to set table row");

	let non_table_key = EncodedKey(CowVec::new(b"non_table_key".to_vec()));
	txn.set(
		&non_table_key,
		EncodedRow(CowVec::new(b"non_table_value".to_vec())),
	)
	.expect("Failed to set non-table row");

	txn.commit().expect("Failed to commit transaction");

	let mut test_instance = PollConsumer::new(
		consumer_id,
		Duration::from_millis(50),
		engine,
		consumer,
	);

	test_instance.start().expect("Failed to start consumer");
	thread::sleep(Duration::from_millis(150));
	test_instance.stop().expect("Failed to stop consumer");

	let events = consumer_clone.get_events();
	assert_eq!(events.len(), 1, "Should have processed only 1 table event");

	if let Some(TableRow(table_row)) = Key::decode(events[0].key()) {
		assert_eq!(table_row.table, TableId(1));
		assert_eq!(table_row.row, RowId(1));
	} else {
		panic!("Expected TableRow key");
	}
}

#[test]
fn test_rapid_start_stop() {
	let engine = create_test_engine();
	let consumer = TestConsumer::new();
	let consumer_id = ConsumerId::flow_consumer();

	for _ in 0..5 {
		let mut test_instance = PollConsumer::new(
			consumer_id.clone(),
			Duration::from_millis(100),
			engine.clone(),
			consumer.clone(),
		);

		test_instance.start().expect("Failed to start consumer");
		assert!(test_instance.is_running());

		thread::sleep(Duration::from_millis(10));

		test_instance.stop().expect("Failed to stop consumer");
		assert!(!test_instance.is_running());
	}
}

type TestTransaction = StandardTransaction<
	Serializable<Memory, SingleVersionLock<Memory>>,
	SingleVersionLock<Memory>,
	StandardCdcTransaction<Memory>,
>;

fn create_test_engine() -> StandardEngine<TestTransaction> {
	let clock = Arc::new(MockClock::new(1000));
	let memory = Memory::with_clock(Box::new(clock.clone()));
	let hooks = Hooks::new();
	let unversioned = SingleVersionLock::new(memory.clone(), hooks.clone());
	let cdc = StandardCdcTransaction::new(memory.clone());
	let versioned =
		Serializable::new(memory, unversioned.clone(), hooks.clone());

	StandardEngine::new(versioned, unversioned, cdc, hooks)
		.expect("Failed to create engine")
}

struct TestConsumer {
	events_received: Arc<Mutex<Vec<CdcEvent>>>,
	process_count: Arc<AtomicUsize>,
	should_fail: Arc<AtomicBool>,
}

impl TestConsumer {
	fn new() -> Self {
		Self {
			events_received: Arc::new(Mutex::new(Vec::new())),
			process_count: Arc::new(AtomicUsize::new(0)),
			should_fail: Arc::new(AtomicBool::new(false)),
		}
	}

	fn set_should_fail(&self, should_fail: bool) {
		self.should_fail.store(should_fail, Ordering::SeqCst);
	}

	fn get_events(&self) -> Vec<CdcEvent> {
		self.events_received.lock().unwrap().clone()
	}

	fn get_process_count(&self) -> usize {
		self.process_count.load(Ordering::SeqCst)
	}
}

impl Clone for TestConsumer {
	fn clone(&self) -> Self {
		Self {
			events_received: Arc::clone(&self.events_received),
			process_count: Arc::clone(&self.process_count),
			should_fail: Arc::clone(&self.should_fail),
		}
	}
}

impl CdcConsume<TestTransaction> for TestConsumer {
	fn consume(
		&self,
		_txn: &mut CommandTransaction<TestTransaction>,
		events: Vec<CdcEvent>,
	) -> Result<()> {
		if self.should_fail.load(Ordering::SeqCst) {
			return Err(reifydb_core::Error(Diagnostic {
				code: "TEST_ERROR".to_string(),
				statement: None,
				message: "Test failure".to_string(),
				column: None,
				span: None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
			}));
		}

		let mut received = self.events_received.lock().unwrap();
		received.extend(events);
		self.process_count.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
}

fn insert_test_events(
	engine: &StandardEngine<TestTransaction>,
	count: usize,
) -> Result<()> {
	for i in 0..count {
		let mut txn = engine.begin_command()?;
		let key = TableRowKey {
			table: TableId(1),
			row: RowId(i as u64),
		};
		let value = format!("value_{}", i);
		txn.set(
			&key.encode(),
			EncodedRow(CowVec::new(value.into_bytes())),
		)?;
		txn.commit()?;
	}

	Ok(())
}
