// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	env,
	os::unix::process::ExitStatusExt,
	process::Command,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicUsize, Ordering},
	},
	thread::sleep,
	time::Instant,
};

use reifydb_cdc::consume::{
	checkpoint::CdcCheckpoint,
	consumer::{CdcConsume, CdcConsumer},
	poll::{PollConsumer, PollConsumerConfig},
};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{config::ConfigKey, id::TableId, storage::StorageId},
		cdc::{Cdc, CdcChange, CdcConsumerId, ConsumerClass},
	},
	key::{EncodableKey, cdc::CdcConsumerKey, row::RowKey, typed::key::Key},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_cdc::storage::{CdcStorage, Cutoff};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::error::TransactionError;
use reifydb_value::{
	error::{Diagnostic, Error},
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, duration::Duration, identity::IdentityId, row_number::RowNumber},
};

#[test]
fn test_consumer_lifecycle() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(100).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	assert!(!test_instance.is_running());

	test_instance.start().expect("Failed to start consumer");
	assert!(test_instance.is_running());

	sleep(Duration::from_milliseconds(50).unwrap().to_std());
	assert!(test_instance.is_running());

	test_instance.stop().expect("Failed to stop consumer");
	assert!(!test_instance.is_running());

	test_instance.stop().expect("Should be able to stop already stopped consumer");

	assert!(!test_instance.is_running());
}

#[test]
fn test_event_processing() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();

	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 5);

	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(50).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	await_until("processes 5", || consumer_clone.get_total_changes() >= 5);

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 5, "Should have processed 5 changes");

	let transactions = consumer_clone.get_transactions();
	assert_eq!(transactions.len(), 5, "Should have 5 transactions");

	for (i, cdc) in transactions.iter().enumerate() {
		assert_eq!(cdc.changes.len(), 1, "Each transaction should have 1 change");
		if let CdcChange::Insert {
			key,
			..
		} = &cdc.changes[0]
		{
			if let Some(table_row) = RowKey::decode(key) {
				assert_eq!(table_row.storage, TableId(1));
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
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 3);

	let config = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		None,
	);
	let mut test_instance =
		PollConsumer::new(config, t.inner().clone(), consumer, cdc_store.clone(), runtime.clone());

	test_instance.start().expect("Failed to start consumer");
	await_until("first run processes 3", || consumer_clone.get_total_changes() >= 3);
	test_instance.stop().expect("Failed to stop consumer");

	let changes_first_run = consumer_clone.get_total_changes();
	assert_eq!(changes_first_run, 3, "Should have processed 3 changes in first run");

	insert_test_events(&t, 2);

	let consumer2 = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer2_clone = consumer2.clone();
	let config2 = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		None,
	);
	let pools2 = Pools::new(PoolConfig::default());
	let actor_system2 = ActorSystem::new(pools2, Clock::Real);
	let runtime2 = actor_system2.spawner();
	let mut test_instance2 = PollConsumer::new(config2, t.inner().clone(), consumer2, cdc_store, runtime2);

	test_instance2.start().expect("Failed to start consumer");
	await_until("second run processes 2 new", || consumer2_clone.get_total_changes() >= 2);
	test_instance2.stop().expect("Failed to stop consumer");

	let changes_second_run = consumer2_clone.get_total_changes();
	assert_eq!(changes_second_run, 2, "Should have processed only 2 new changes");

	let mut txn = t.begin_query(IdentityId::system()).expect("Failed to begin transaction");
	let consumer_key = CdcConsumerKey {
		consumer: consumer_id,
	}
	.encode();

	let checkpoint = txn.get(&consumer_key).expect("Failed to get checkpoint").expect("Checkpoint should exist");

	let mut buffer = [0u8; 8];
	buffer.copy_from_slice(&checkpoint.bytes[0..8]);
	let stored_version = u64::from_be_bytes(buffer);

	assert!(stored_version >= 3, "Checkpoint should be after initial events");
}

const ABORT_CHILD_ENV: &str = "REIFYDB_CDC_ABORT_CHILD";

#[test]
fn test_consumer_error_aborts_process() {
	// A checkpoint must never advance past events the consumer could not durably process, so a
	// consumer error aborts instead of recovering. An abort cannot be observed in-process, so the
	// failure runs in a re-exec'd child and the parent checks for SIGABRT.
	if env::var(ABORT_CHILD_ENV).is_ok() {
		run_abort_child();
		return;
	}

	let exe = env::current_exe().expect("Failed to resolve test binary path");
	let status = Command::new(exe)
		.args(["test_consumer_error_aborts_process", "--exact", "--nocapture", "--test-threads=1"])
		.env(ABORT_CHILD_ENV, "1")
		.status()
		.expect("Failed to run abort child process");

	assert_eq!(
		status.signal(),
		Some(6),
		"a consumer error must abort the process with SIGABRT, but the child exited with {status:?}"
	);
}

fn run_abort_child() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	consumer.set_should_fail(true);
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 3);

	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(50).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	// If the abort path regresses, control returns here and the child exits 0, failing the parent.
	sleep(poll_timeout().to_std());
}

#[test]
fn test_recovers_when_consume_reply_is_lost() {
	// Without the consume-wait backstop the actor stays in WaitingForConsume forever, so a single
	// dropped reply wedges the poll loop and nothing after it is ever consumed.
	let t = TestEngine::new();
	t.inner()
		.catalog()
		.cache()
		.set_config(
			ConfigKey::CdcConsumeWaitTimeout,
			CommitVersion(1),
			Value::Duration(Duration::from_milliseconds(150).unwrap()),
		)
		.expect("Failed to set consume wait timeout");

	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 3);

	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(50).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");
	await_until("processes initial 3", || consumer_clone.get_total_changes() >= 3);

	// New work arrives while replies are dropped: the consumer dispatches but never hears back.
	consumer_clone.set_drop_reply(true);
	let calls_before_drop = consumer_clone.get_call_count();
	insert_test_events(&t, 2);

	// The timeout must keep re-dispatching the un-acked batch rather than stalling.
	await_until("re-dispatches after lost reply", || consumer_clone.get_call_count() >= calls_before_drop + 2);
	assert_eq!(
		consumer_clone.get_total_changes(),
		3,
		"No new changes should be processed while replies are lost (checkpoint must not advance)"
	);

	// Once replies flow again, the still-un-acked batch is processed.
	consumer_clone.set_drop_reply(false);
	await_until("recovery processes 5", || consumer_clone.get_total_changes() >= 5);
	assert_eq!(consumer_clone.get_total_changes(), 5, "Should process the batch once replies resume");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_empty_events_handling() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(50).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	sleep(Duration::from_milliseconds(150).unwrap().to_std());

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 0, "Should have no changes to process");
	assert_eq!(consumer_clone.get_process_count(), 0, "Should not have called consume");

	insert_test_events(&t, 1);
	await_until("processes 1", || consumer_clone.get_total_changes() >= 1);

	let changes_after_insert = consumer_clone.get_total_changes();
	assert_eq!(changes_after_insert, 1, "Should have processed 1 change");
	assert!(consumer_clone.get_process_count() >= 1, "Should have called consume");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_multiple_consumers() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	let consumer_id1 = CdcConsumerId::new("consumer-1");
	let consumer1 = TestConsumer::new(t.inner().clone(), consumer_id1.clone());
	let consumer1_clone = consumer1.clone();

	let consumer_id2 = CdcConsumerId::new("consumer-2");
	let consumer2 = TestConsumer::new(t.inner().clone(), consumer_id2.clone());
	let consumer2_clone = consumer2.clone();

	insert_test_events(&t, 3);

	let config1 = PollConsumerConfig::new(
		consumer_id1.clone(),
		"cdc-poll-test-1",
		Duration::from_milliseconds(50).unwrap(),
		None,
	);
	let mut test_instance1 =
		PollConsumer::new(config1, t.inner().clone(), consumer1, cdc_store.clone(), runtime.clone());

	let config2 = PollConsumerConfig::new(
		consumer_id2.clone(),
		"cdc-poll-test-2",
		Duration::from_milliseconds(75).unwrap(),
		None,
	);
	let mut test_instance2 = PollConsumer::new(config2, t.inner().clone(), consumer2, cdc_store, runtime);

	test_instance1.start().expect("Failed to start consumer 1");
	test_instance2.start().expect("Failed to start consumer 2");

	await_until("both process 3", || {
		consumer1_clone.get_total_changes() >= 3 && consumer2_clone.get_total_changes() >= 3
	});

	let changes1 = consumer1_clone.get_total_changes();
	let changes2 = consumer2_clone.get_total_changes();

	assert_eq!(changes1, 3, "Consumer 1 should have processed 3 changes");
	assert_eq!(changes2, 3, "Consumer 2 should have processed 3 changes");

	insert_test_events(&t, 2);

	await_until("both process 5", || {
		consumer1_clone.get_total_changes() >= 5 && consumer2_clone.get_total_changes() >= 5
	});

	let changes1_after = consumer1_clone.get_total_changes();
	let changes2_after = consumer2_clone.get_total_changes();

	assert_eq!(changes1_after, 5, "Consumer 1 should have processed 5 changes total");
	assert_eq!(changes2_after, 5, "Consumer 2 should have processed 5 changes total");

	let mut txn = t.begin_query(IdentityId::system()).expect("Failed to begin transaction");

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
	buffer.copy_from_slice(&checkpoint1.bytes[0..8]);
	let version1 = u64::from_be_bytes(buffer);

	buffer.copy_from_slice(&checkpoint2.bytes[0..8]);
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
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();

	let mut txn = t.begin_command(IdentityId::system()).expect("Failed to begin transaction");

	let table_key = RowKey::encoded(StorageId::table(1), RowNumber(1));
	txn.set(&table_key, EncodedBytes(CowVec::new(b"table_value".to_vec()))).expect("Failed to set table encoded");

	let non_table_key = EncodedKey::new(b"non_table_key");
	txn.set(&non_table_key, EncodedBytes(CowVec::new(b"non_table_value".to_vec())))
		.expect("Failed to set non-table encoded");

	txn.commit().expect("Failed to commit transaction");

	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();
	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(50).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");
	await_until("processes 2", || consumer_clone.get_total_changes() >= 2);
	test_instance.stop().expect("Failed to stop consumer");

	// One encoded table change is enough to include the whole transaction, non-table changes and all.
	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 2, "Should have processed 2 changes (both in same transaction)");

	let transactions = consumer_clone.get_transactions();
	assert_eq!(transactions.len(), 1, "Should have 1 transaction");
	assert_eq!(transactions[0].changes.len(), 2, "Transaction should have 2 changes");

	let table_change = transactions[0]
		.changes
		.iter()
		.find(|c| RowKey::decode(c.key()).is_some())
		.expect("Should have at least one table change");

	if let CdcChange::Insert {
		key,
		..
	} = table_change
	{
		if let Some(table_row) = RowKey::decode(key) {
			assert_eq!(table_row.storage, TableId(1));
			assert_eq!(table_row.row, RowNumber(1));
		} else {
			panic!("Expected Row key");
		}
	}
}

#[test]
fn test_rapid_start_stop() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	for _ in 0..5 {
		let config = PollConsumerConfig::new(
			consumer_id.clone(),
			"cdc-poll-test",
			Duration::from_milliseconds(100).unwrap(),
			None,
		);
		let mut test_instance = PollConsumer::new(
			config,
			t.inner().clone(),
			consumer.clone(),
			cdc_store.clone(),
			runtime.clone(),
		);

		test_instance.start().expect("Failed to start consumer");
		assert!(test_instance.is_running());

		sleep(Duration::from_milliseconds(10).unwrap().to_std());

		test_instance.stop().expect("Failed to stop consumer");
		assert!(!test_instance.is_running());
	}
}

#[test]
fn test_batch_size_limits_processing() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 25);

	let config = PollConsumerConfig::new(
		consumer_id,
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		Some(10),
	);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	await_until("processes 25", || consumer_clone.get_total_changes() >= 25);

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 25, "Should have processed all 25 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count >= 3, "Should have been called at least 3 times (for batches of 10, 10, 5)");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_batch_size_one_processes_sequentially() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 5);

	let config = PollConsumerConfig::new(
		consumer_id,
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		Some(1),
	);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	await_until("processes 5", || consumer_clone.get_total_changes() >= 5);

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 5, "Should have processed all 5 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count >= 5, "Should have been called at least 5 times (one per event)");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_batch_size_none_processes_all_at_once() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 20);
	t.await_cdc();

	let config =
		PollConsumerConfig::new(consumer_id, "cdc-poll-test", Duration::from_milliseconds(50).unwrap(), None);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	await_until("processes 20", || consumer_clone.get_total_changes() >= 20);

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 20, "Should have processed all 20 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count <= 2, "Should have been called at most 2 times with unbounded batch");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_batch_size_larger_than_events() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 5);
	t.await_cdc();

	let config = PollConsumerConfig::new(
		consumer_id,
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		Some(100),
	);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	await_until("processes 5", || consumer_clone.get_total_changes() >= 5);

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 5, "Should have processed all 5 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count <= 2, "Should have processed efficiently in 1-2 calls");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_batch_size_with_checkpoint_resume() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 15);

	let config = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		Some(5),
	);
	let mut test_instance =
		PollConsumer::new(config, t.inner().clone(), consumer, cdc_store.clone(), runtime.clone());

	test_instance.start().expect("Failed to start consumer");

	// A real stop() drains the actor, so the durable checkpoint cannot run ahead of what this run
	// actually recorded.
	await_until("first run processes a batch", || consumer_clone.get_total_changes() >= 5);
	test_instance.stop().expect("Failed to stop consumer");

	let changes_first_run = consumer_clone.get_total_changes();
	assert!(changes_first_run >= 5, "Should have processed at least one batch of 5");

	insert_test_events(&t, 3);

	// Same consumer id, so the new instance must resume from the persisted checkpoint.
	let consumer2 = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer2_clone = consumer2.clone();
	let config2 = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		Some(5),
	);
	let pools2 = Pools::new(PoolConfig::default());
	let actor_system2 = ActorSystem::new(pools2, Clock::Real);
	let runtime2 = actor_system2.spawner();
	let mut test_instance2 = PollConsumer::new(config2, t.inner().clone(), consumer2, cdc_store, runtime2);

	// Resume must process exactly the leftover events plus the 3 new ones, each once.
	let total_expected = 18 - changes_first_run;
	test_instance2.start().expect("Failed to start consumer");
	await_until("second run drains remainder", || consumer2_clone.get_total_changes() >= total_expected);
	test_instance2.stop().expect("Failed to stop consumer");

	let changes_second_run = consumer2_clone.get_total_changes();
	assert_eq!(changes_second_run, total_expected, "Should have processed remaining events plus new ones");
}

#[test]
fn test_batch_size_exact_match() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::flow_consumer();
	let consumer = TestConsumer::new(t.inner().clone(), consumer_id.clone());
	let consumer_clone = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	insert_test_events(&t, 10);
	t.await_cdc();

	let config = PollConsumerConfig::new(
		consumer_id,
		"cdc-poll-test",
		Duration::from_milliseconds(50).unwrap(),
		Some(10),
	);
	let mut test_instance = PollConsumer::new(config, t.inner().clone(), consumer, cdc_store, runtime);

	test_instance.start().expect("Failed to start consumer");

	await_until("processes 10", || consumer_clone.get_total_changes() >= 10);

	let changes = consumer_clone.get_total_changes();
	assert_eq!(changes, 10, "Should have processed all 10 changes");

	let process_count = consumer_clone.get_process_count();
	assert!(process_count <= 2, "Should have processed in 1-2 calls with exact batch size match");

	test_instance.stop().expect("Failed to stop consumer");
}

#[test]
fn test_multiple_consumers_different_batch_sizes() {
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let runtime = actor_system.spawner();

	let consumer_id1 = CdcConsumerId::new("consumer-batch-3");
	let consumer1 = TestConsumer::new(t.inner().clone(), consumer_id1.clone());
	let consumer1_clone = consumer1.clone();

	let consumer_id2 = CdcConsumerId::new("consumer-unbounded");
	let consumer2 = TestConsumer::new(t.inner().clone(), consumer_id2.clone());
	let consumer2_clone = consumer2.clone();

	insert_test_events(&t, 10);
	t.await_cdc();

	let config1 = PollConsumerConfig::new(
		consumer_id1.clone(),
		"cdc-poll-test-1",
		Duration::from_milliseconds(50).unwrap(),
		Some(3),
	);
	let mut test_instance1 =
		PollConsumer::new(config1, t.inner().clone(), consumer1, cdc_store.clone(), runtime.clone());

	let config2 = PollConsumerConfig::new(
		consumer_id2.clone(),
		"cdc-poll-test-2",
		Duration::from_milliseconds(75).unwrap(),
		None,
	);
	let mut test_instance2 = PollConsumer::new(config2, t.inner().clone(), consumer2, cdc_store, runtime);

	test_instance1.start().expect("Failed to start consumer 1");
	test_instance2.start().expect("Failed to start consumer 2");

	await_until("both process 10", || {
		consumer1_clone.get_total_changes() >= 10 && consumer2_clone.get_total_changes() >= 10
	});

	let changes1 = consumer1_clone.get_total_changes();
	let changes2 = consumer2_clone.get_total_changes();

	assert_eq!(changes1, 10, "Consumer 1 should have processed all 10 changes");
	assert_eq!(changes2, 10, "Consumer 2 should have processed all 10 changes");

	let process_count1 = consumer1_clone.get_process_count();
	let process_count2 = consumer2_clone.get_process_count();

	assert!(process_count1 >= 4, "Consumer 1 should have at least 4 calls (10 events / batch size 3)");
	assert!(process_count2 <= 2, "Consumer 2 should have at most 2 calls (unbounded)");

	test_instance1.stop().expect("Failed to stop consumer 1");
	test_instance2.stop().expect("Failed to stop consumer 2");
}

struct TestConsumer {
	host: StandardEngine,
	consumer_key: EncodedKey,
	cdc_received: Arc<Mutex<Vec<Cdc>>>,
	process_count: Arc<AtomicUsize>,
	call_count: Arc<AtomicUsize>,
	should_fail: Arc<AtomicBool>,
	drop_reply: Arc<AtomicBool>,
}

impl TestConsumer {
	fn new(host: StandardEngine, consumer_id: CdcConsumerId) -> Self {
		let consumer_key = CdcConsumerKey {
			consumer: consumer_id,
		}
		.encode();
		Self {
			host,
			consumer_key,
			cdc_received: Arc::new(Mutex::new(Vec::new())),
			process_count: Arc::new(AtomicUsize::new(0)),
			call_count: Arc::new(AtomicUsize::new(0)),
			should_fail: Arc::new(AtomicBool::new(false)),
			drop_reply: Arc::new(AtomicBool::new(false)),
		}
	}

	fn set_should_fail(&self, should_fail: bool) {
		self.should_fail.store(should_fail, Ordering::SeqCst);
	}

	fn set_drop_reply(&self, drop_reply: bool) {
		self.drop_reply.store(drop_reply, Ordering::SeqCst);
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

	fn get_call_count(&self) -> usize {
		self.call_count.load(Ordering::SeqCst)
	}
}

impl Clone for TestConsumer {
	fn clone(&self) -> Self {
		Self {
			host: self.host.clone(),
			consumer_key: self.consumer_key.clone(),
			cdc_received: Arc::clone(&self.cdc_received),
			process_count: Arc::clone(&self.process_count),
			call_count: Arc::clone(&self.call_count),
			should_fail: Arc::clone(&self.should_fail),
			drop_reply: Arc::clone(&self.drop_reply),
		}
	}
}

impl CdcConsume for TestConsumer {
	fn consume(&self, transactions: Vec<Cdc>, reply: Box<dyn FnOnce(reifydb_value::Result<()>) + Send>) {
		self.call_count.fetch_add(1, Ordering::SeqCst);

		if self.drop_reply.load(Ordering::SeqCst) {
			// Simulate a lost reply: the callback is dropped without ever being invoked.
			drop(reply);
			return;
		}

		if self.should_fail.load(Ordering::SeqCst) {
			(reply)(Err(Error(Box::new(Diagnostic {
				code: "TEST_ERROR".to_string(),
				rql: None,
				message: "Test failure".to_string(),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			}))));
			return;
		}

		// The poll actor only sees progress through the persisted checkpoint.
		let latest_version = transactions.last().map(|c| c.version);
		if let Some(version) = latest_version {
			match self.host.begin_command(IdentityId::system()) {
				Ok(mut txn) => {
					if let Err(e) = CdcCheckpoint::persist(
						&mut txn,
						&self.consumer_key,
						version,
						ConsumerClass::Ephemeral,
					) {
						(reply)(Err(e));
						return;
					}
					if let Err(e) = txn.commit() {
						(reply)(Err(e));
						return;
					}
				}
				Err(e) => {
					(reply)(Err(e));
					return;
				}
			}
		}

		let mut received = self.cdc_received.lock().unwrap();
		received.extend(transactions);
		self.process_count.fetch_add(1, Ordering::SeqCst);
		(reply)(Ok(()));
	}
}

fn poll_timeout() -> Duration {
	Duration::from_seconds(5).unwrap()
}
fn poll_interval() -> Duration {
	Duration::from_milliseconds(10).unwrap()
}

fn await_until<F: Fn() -> bool>(label: &str, check: F) {
	let timeout = poll_timeout().to_std();
	let deadline = Instant::now() + timeout;
	while Instant::now() < deadline {
		if check() {
			return;
		}
		sleep(poll_interval().to_std());
	}
	panic!("await_until({label}) timed out after {timeout:?}");
}

fn insert_test_events(engine: &StandardEngine, count: usize) {
	for i in 0..count {
		let mut txn = engine.begin_command(IdentityId::system()).unwrap();
		let key = RowKey::encoded(StorageId::table(1), RowNumber((i + 1) as u64));
		let value = format!("value_{}", i);
		txn.set(&key, EncodedBytes(CowVec::new(value.into_bytes()))).unwrap();
		txn.commit().unwrap();
	}
}

struct ResyncConsumer {
	host: StandardEngine,
	cdc_received: Arc<Mutex<Vec<Cdc>>>,
	overtaken_calls: Arc<Mutex<Vec<(CommitVersion, CommitVersion)>>>,
}

impl ResyncConsumer {
	fn new(host: StandardEngine) -> Self {
		Self {
			host,
			cdc_received: Arc::new(Mutex::new(Vec::new())),
			overtaken_calls: Arc::new(Mutex::new(Vec::new())),
		}
	}

	fn received_versions(&self) -> Vec<CommitVersion> {
		self.cdc_received.lock().unwrap().iter().map(|c| c.version).collect()
	}

	fn overtaken_calls(&self) -> Vec<(CommitVersion, CommitVersion)> {
		self.overtaken_calls.lock().unwrap().clone()
	}
}

impl Clone for ResyncConsumer {
	fn clone(&self) -> Self {
		Self {
			host: self.host.clone(),
			cdc_received: Arc::clone(&self.cdc_received),
			overtaken_calls: Arc::clone(&self.overtaken_calls),
		}
	}
}

impl CdcConsume for ResyncConsumer {
	fn consume(&self, transactions: Vec<Cdc>, reply: Box<dyn FnOnce(reifydb_value::Result<()>) + Send>) {
		self.cdc_received.lock().unwrap().extend(transactions);
		(reply)(Ok(()));
	}

	fn overtaken(
		&self,
		cursor: CommitVersion,
		truncated_before: CommitVersion,
		reply: Box<dyn FnOnce(reifydb_value::Result<CommitVersion>) + Send>,
	) {
		self.overtaken_calls.lock().unwrap().push((cursor, truncated_before));
		let head = self.host.current_version().unwrap();
		(reply)(Ok(head));
	}
}

#[test]
fn an_overtaken_consumer_resyncs_through_its_hook_and_resumes_past_the_gap() {
	// A cursor below truncated history must be signalled, not silently skipped over: the hook gets
	// the stale cursor and the floor, the durable row flips to Invalidated, and consumption resumes
	// at the version the consumer chose without re-delivering anything earlier.
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::new("resync-consumer");

	insert_test_events(t.inner(), 5);
	let head = t.inner().current_version().unwrap();
	await_until("cdc produced up to head", || cdc_store.max_version().unwrap().unwrap_or(CommitVersion(0)) >= head);

	let mut txn = t.inner().begin_command(IdentityId::system()).unwrap();
	CdcCheckpoint::persist(&mut txn, &consumer_id, CommitVersion(1), ConsumerClass::Ephemeral).unwrap();
	txn.commit().unwrap();

	// Retention drops whole blocks, so the cutoff is set one past head; a cutoff at head would land inside the
	// block holding it and drop nothing at all.
	assert!(cdc_store.flush_pending(), "the history must be sealed before it can be truncated");
	cdc_store.drop_before(Cutoff::Version(CommitVersion(head.0 + 1)), usize::MAX).unwrap();
	let floor = cdc_store.truncated_before().unwrap();
	assert!(floor.0 > 2, "precondition: the truncation floor must be past the stale cursor");

	let consumer = ResyncConsumer::new(t.inner().clone());
	let probe = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let config = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-resync-test",
		Duration::from_milliseconds(20).unwrap(),
		None,
	);
	let mut poll =
		PollConsumer::new(config, t.inner().clone(), consumer, cdc_store.clone(), actor_system.spawner());
	poll.start().unwrap();

	await_until("overtaken hook invoked", || !probe.overtaken_calls().is_empty());
	let (cursor, reported_floor) = probe.overtaken_calls()[0];
	assert_eq!(cursor, CommitVersion(1), "the hook must receive the stale cursor");
	assert_eq!(reported_floor, floor, "the hook must receive the truncation floor");

	let mut query = t.inner().begin_query(IdentityId::system()).unwrap();
	let row = reifydb_cdc::consume::checkpoint::CdcCheckpoint::fetch_row(
		&mut reifydb_transaction::transaction::Transaction::Query(&mut query),
		&consumer_id,
	)
	.unwrap()
	.expect("the durable row must survive invalidation");
	assert_eq!(
		row.state,
		reifydb_core::interface::cdc::CheckpointState::Invalidated,
		"the actor must flip the durable checkpoint to Invalidated before asking for a resync"
	);
	drop(query);

	insert_test_events(t.inner(), 3);
	await_until("post-resync cdc delivered", || !probe.received_versions().is_empty());
	assert!(
		probe.received_versions().iter().all(|v| *v > head),
		"nothing from before the resync point may be re-delivered (head={}, got {:?})",
		head.0,
		probe.received_versions()
	);

	poll.stop().unwrap();
}

struct EvictedBatchConsumer {
	inner: ResyncConsumer,
	fail_once: Arc<AtomicBool>,
}

impl Clone for EvictedBatchConsumer {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
			fail_once: Arc::clone(&self.fail_once),
		}
	}
}

impl CdcConsume for EvictedBatchConsumer {
	fn consume(&self, transactions: Vec<Cdc>, reply: Box<dyn FnOnce(reifydb_value::Result<()>) + Send>) {
		if self.fail_once.swap(false, Ordering::SeqCst) {
			// What the dispatch path emits when its lease acquire finds the batch's history
			// already reclaimed.
			(reply)(Err(TransactionError::ConsumerOvertaken {
				version: CommitVersion(9),
				cutoff: CommitVersion(40),
			}
			.into()));
			return;
		}
		self.inner.consume(transactions, reply);
	}

	fn overtaken(
		&self,
		cursor: CommitVersion,
		truncated_before: CommitVersion,
		reply: Box<dyn FnOnce(reifydb_value::Result<CommitVersion>) + Send>,
	) {
		self.inner.overtaken(cursor, truncated_before, reply);
	}
}

#[test]
fn a_batch_that_lost_its_mvcc_history_resyncs_instead_of_aborting() {
	// A lease acquire that fails because the version history was reclaimed is an expected
	// consequence of ephemeral lag, so it must route into the resync protocol rather than abort the
	// process along with the flow hot path.
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::new("evicted-batch-consumer");

	let consumer = EvictedBatchConsumer {
		inner: ResyncConsumer::new(t.inner().clone()),
		fail_once: Arc::new(AtomicBool::new(true)),
	};
	let probe = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let config = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-evicted-batch-test",
		Duration::from_milliseconds(20).unwrap(),
		None,
	);
	let mut poll =
		PollConsumer::new(config, t.inner().clone(), consumer, cdc_store.clone(), actor_system.spawner());
	poll.start().unwrap();

	insert_test_events(t.inner(), 3);
	await_until("TXN_012 routed into the resync protocol", || !probe.inner.overtaken_calls().is_empty());

	insert_test_events(t.inner(), 2);
	await_until("consumption recovered after the MVCC resync", || !probe.inner.received_versions().is_empty());

	poll.stop().unwrap();
}

#[test]
fn an_invalidated_checkpoint_row_triggers_resync_at_startup() {
	// The invalidation happened in a previous process life; resuming the stale cursor as if the row
	// were valid would silently skip the gap that invalidation recorded.
	let t = TestEngine::new();
	let cdc_store = t.cdc_store();
	let consumer_id = CdcConsumerId::new("restarted-consumer");

	let mut txn = t.inner().begin_command(IdentityId::system()).unwrap();
	CdcCheckpoint::persist(&mut txn, &consumer_id, CommitVersion(7), ConsumerClass::Ephemeral).unwrap();
	CdcCheckpoint::invalidate(&mut txn, &consumer_id).unwrap();
	txn.commit().unwrap();

	let consumer = ResyncConsumer::new(t.inner().clone());
	let probe = consumer.clone();
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let config = PollConsumerConfig::new(
		consumer_id.clone(),
		"cdc-restart-resync-test",
		Duration::from_milliseconds(20).unwrap(),
		None,
	);
	let mut poll =
		PollConsumer::new(config, t.inner().clone(), consumer, cdc_store.clone(), actor_system.spawner());
	poll.start().unwrap();

	insert_test_events(t.inner(), 1);
	await_until("startup resync invoked", || !probe.overtaken_calls().is_empty());
	assert_eq!(
		probe.overtaken_calls()[0].0,
		CommitVersion(7),
		"the resync must report the invalidated row's cursor"
	);

	insert_test_events(t.inner(), 2);
	await_until("consumption recovered after startup resync", || !probe.received_versions().is_empty());

	poll.stop().unwrap();
}
