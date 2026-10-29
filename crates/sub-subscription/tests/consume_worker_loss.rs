// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::{Arc, mpsc::channel},
	thread,
};

use reifydb::testing::db::TestDb;
use reifydb_core::{
	commit::queue::{CommitPull, CommitQueue},
	common::CommitVersion,
	interface::{catalog::flow::FlowId, commit::CommitEntry},
};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::{ActorRef, SendError},
		system::ActorSystem,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_sub_subscription::{
	consumer::SubscriptionCdcConsumer,
	delivery::DeliveryBuffer,
	store::SubscriptionStore,
	tracker::{SubscribedObjects, SubscriptionPositionTracker, SubscriptionSourceTracker},
	worker::SubscriptionWorkerMessage,
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

struct StoppedWorker;

impl Actor for StoppedWorker {
	type State = ();
	type Message = SubscriptionWorkerMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, _msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		Directive::Stop
	}
}

fn probe() -> SubscriptionWorkerMessage {
	SubscriptionWorkerMessage::Unregister {
		flow_id: FlowId(0),
		reply: Box::new(|| {}),
	}
}

fn closed_worker() -> ActorRef<SubscriptionWorkerMessage> {
	let system = ActorSystem::testing(Clock::Real);
	let handle = system.spawn_coordination("closed-subscription-worker", StoppedWorker);
	let worker = handle.actor_ref().clone();
	system.shutdown();
	handle.join().expect("the stopped worker must finish");
	for _ in 0..2000 {
		match worker.send(probe()) {
			Err(SendError::Closed(_)) => return worker,
			_ => thread::sleep(Duration::from_milliseconds(5).unwrap().to_std()),
		}
	}
	panic!("the stopped worker's mailbox never closed");
}

fn queue(db: &TestDb) -> CommitQueue {
	db.engine().ioc().resolve::<CommitQueue>().expect("the commit queue is registered")
}

fn wait_for_consumer_caught_up(db: &TestDb) -> CommitVersion {
	let target = db.watermarks().tx().current().expect("current version");
	let timeout = Duration::from_seconds(10).unwrap();
	if !db.watermarks().reader().wait_for_subscription(target, timeout) {
		panic!("subscription reader did not reach {:?} within {:?}", target, timeout);
	}
	target
}

fn entries(db: &TestDb, after: CommitVersion, target: CommitVersion) -> Vec<Arc<CommitEntry>> {
	match queue(db).pull(after, target, ByteSize::from_bytes(u64::MAX)) {
		CommitPull::Hit {
			entries,
			..
		} => entries,
		CommitPull::Behind => panic!("the held floor must keep every commit after {after:?}"),
	}
}

#[test]
fn a_batch_sent_to_an_unreachable_worker_fails_naming_the_worker() {
	// A batch acked Ok while a worker never received it moves the checkpoint past changes that worker never saw.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	let _floor = queue(&db).floor("test");
	let before = db.watermarks().tx().current().expect("current version");
	db.command("INSERT app::t [{id: 1}]");
	let target = wait_for_consumer_caught_up(&db);

	let entries = entries(&db, before, target);
	assert!(!entries.is_empty(), "the insert must be in the queue before it can be dispatched");

	let store = Arc::new(SubscriptionStore::new(1024));
	let consumer = SubscriptionCdcConsumer::new(
		db.engine().clone(),
		vec![closed_worker()],
		SubscriptionSourceTracker::new(),
		SubscriptionPositionTracker::new(),
		store.clone(),
		Arc::new(DeliveryBuffer::new(store)),
		SubscribedObjects::new(),
	);

	let (tx, rx) = channel();
	consumer.consume(entries, Box::new(move |outcome| tx.send(outcome).unwrap()));

	let outcome = rx
		.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
		.expect("the batch reply must fire once every worker is accounted for");
	let error = outcome.expect_err("a batch that never reached a worker must not ack Ok");
	assert!(
		error.to_string().contains("subscription-worker-0"),
		"the failure must name the unreachable worker: {error}"
	);
}
