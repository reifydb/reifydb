// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Bound,
	sync::{Arc, mpsc::channel},
	thread,
};

use reifydb::testing::db::TestDb;
use reifydb_cdc::consume::{consumer::CdcConsume, is_relevant_cdc};
use reifydb_core::interface::{catalog::flow::FlowId, cdc::Cdc};
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::{ActorRef, SendError},
		system::ActorSystem,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_cdc::{storage::CdcStorage, store::CdcStore};
use reifydb_sub_subscription::{
	consumer::SubscriptionCdcConsumer,
	delivery::DeliveryBuffer,
	store::SubscriptionStore,
	tracker::{SubscriptionPositionTracker, SubscriptionSourceTracker},
	worker::SubscriptionWorkerMessage,
};
use reifydb_value::value::duration::Duration;

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

fn wait_for_consumer_caught_up(db: &TestDb) {
	let target = db.watermarks().tx().current().expect("current version");
	let timeout = Duration::from_seconds(10).unwrap();
	if !db.watermarks().cdc().wait_for_consumer(target, timeout) {
		panic!("CDC consumer did not reach {:?} within {:?}", target, timeout);
	}
}

fn relevant_cdcs(db: &TestDb) -> Vec<Cdc> {
	let cdc_store = db.engine().ioc().resolve::<CdcStore>().expect("cdc store registered");
	let batch = cdc_store.read_range(Bound::Unbounded, Bound::Unbounded, 1024).expect("read the cdc log");
	batch.items.into_iter().filter(is_relevant_cdc).collect()
}

#[test]
fn a_batch_sent_to_an_unreachable_worker_fails_naming_the_worker() {
	// A batch acked Ok while a worker never received it moves the checkpoint past changes that worker never saw.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4 }");
	db.command("INSERT app::t [{id: 1}]");
	wait_for_consumer_caught_up(&db);

	let cdcs = relevant_cdcs(&db);
	assert!(!cdcs.is_empty(), "the insert must be in the cdc log before it can be dispatched");

	let store = Arc::new(SubscriptionStore::new(1024));
	let consumer = SubscriptionCdcConsumer::new(
		db.engine().clone(),
		vec![closed_worker()],
		SubscriptionSourceTracker::new(),
		SubscriptionPositionTracker::new(),
		store.clone(),
		Arc::new(DeliveryBuffer::new(store)),
	);

	let (tx, rx) = channel();
	consumer.consume(cdcs, Box::new(move |outcome| tx.send(outcome).unwrap()));

	let outcome = rx
		.recv_timeout(Duration::from_seconds(10).unwrap().to_std())
		.expect("the batch reply must fire once every worker is accounted for");
	let error = outcome.expect_err("a batch that never reached a worker must not ack Ok");
	assert!(
		error.to_string().contains("subscription-worker-0"),
		"the failure must name the unreachable worker: {error}"
	);
}
