// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb::{
	Clock, MockClock,
	codec::wire::{RawChangePayload, WireFormat as ClientWireFormat},
	core::{
		interface::{catalog::id::SubscriptionId, change::StagedBatch},
		value::column::columns::Columns,
	},
	runtime::{context::rng::Rng, sync::mutex::Mutex},
	sub_core::{
		registry::SubscriptionRegistry,
		wire_sink::{BatchSubscribedEntry, WireSink},
	},
	sub_subscription::{poller::StoreBackedPoller, store::SubscriptionStore},
	subscription::{batch::BatchId, delivery::DeliveryResult},
	value::value::{Value, diff_type::DiffType, duration::Duration, frame::frame::Frame, uuid::Uuid7},
};

#[derive(Clone)]
struct RecordingSink {
	seen: Arc<Mutex<Vec<i64>>>,
}

impl RecordingSink {
	fn new() -> Self {
		Self {
			seen: Arc::new(Mutex::new(Vec::new())),
		}
	}

	fn values(&self) -> Vec<i64> {
		self.seen.lock().clone()
	}
}

impl WireSink for RecordingSink {
	type Format = ();

	fn client_wire_format(_format: Self::Format) -> ClientWireFormat {
		ClientWireFormat::Rbcf
	}

	fn send_subscribed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_batch_subscribed(&self, _batch_id: BatchId, _subscriptions: &[BatchSubscribedEntry]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_change(
		&self,
		_sub_id: SubscriptionId,
		_op: DiffType,
		columns: Columns,
		_format: Self::Format,
	) -> DeliveryResult {
		self.seen.lock().push(first_value(&columns));
		DeliveryResult::Delivered
	}

	fn send_remote_change(
		&self,
		_sub_id: SubscriptionId,
		_payload: RawChangePayload,
		_format: Self::Format,
	) -> DeliveryResult {
		panic!("the poller never delivers a remote change through this sink")
	}

	fn send_batch_envelope(
		&self,
		_batch_id: BatchId,
		_format: Self::Format,
		_entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult {
		panic!("this fixture registers no batch subscriptions")
	}

	fn send_batch_subscription_closed(&self, _batch_id: BatchId, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_closed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}
}

fn first_value(columns: &Columns) -> i64 {
	match columns.iter().next().expect("one column").data().get_value(0) {
		Value::Int8(value) => value,
		other => panic!("expected Int8, got {other:?}"),
	}
}

fn stage(id: SubscriptionId, values: &[i64]) -> HashMap<SubscriptionId, Vec<StagedBatch>> {
	let mut staged = HashMap::new();
	staged.insert(
		id,
		values.iter()
			.copied()
			.map(|value| (DiffType::Insert, Columns::single_row([("v", Value::Int8(value))])))
			.collect(),
	);
	staged
}

struct Fixture {
	store: Arc<SubscriptionStore>,
	registry: Arc<SubscriptionRegistry<RecordingSink>>,
	poller: StoreBackedPoller,
	sink: RecordingSink,
	id: SubscriptionId,
}

fn fixture(warming_cap: Option<usize>) -> Fixture {
	let clock = Clock::Mock(MockClock::from_millis(1000));
	let rng = Rng::seeded(42);

	let store = Arc::new(SubscriptionStore::new(64));
	let id = store.next_id();
	store.register(id);

	let registry = Arc::new(SubscriptionRegistry::<RecordingSink>::new(clock.clone()));
	let sink = RecordingSink::new();
	registry.subscribe(
		id,
		Uuid7::generate(&clock, &rng),
		sink.clone(),
		(),
		warming_cap,
		Duration::zero(),
		Duration::zero(),
	);

	let poller = StoreBackedPoller::new(store.clone(), 100);
	Fixture {
		store,
		registry,
		poller,
		sink,
		id,
	}
}

#[test]
fn a_staged_batch_reaches_the_sink_through_the_poller() {
	// The poller is the only thing joining the store to a sink. Every other test in this crate
	// observes delivery by reaching into the store directly, so without this one the entire
	// store-to-sink leg can be dead while the suite stays green.
	let f = fixture(None);

	f.store.commit_staged(stage(f.id, &[7, 8]));
	f.poller.poll_all(f.registry.as_ref());

	assert_eq!(f.sink.values(), vec![7, 8], "the poller must carry staged batches all the way to the sink");
	assert_eq!(f.store.pending_batches(), 0, "and must leave nothing behind in the store");
}

#[test]
fn a_warming_subscription_holds_its_changes_until_it_is_promoted() {
	// A subscription still hydrating must buffer rather than push live changes ahead of its
	// snapshot, and promoting must then release every held change in order rather than drop them.
	let f = fixture(Some(8));

	f.store.commit_staged(stage(f.id, &[1, 2]));
	f.poller.poll_all(f.registry.as_ref());
	assert!(f.sink.values().is_empty(), "a warming subscription must not reach the sink yet");

	f.registry.promote_to_live(f.id);
	assert_eq!(f.sink.values(), vec![1, 2], "promotion must release the buffered changes, in order");
}
