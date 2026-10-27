// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration as StdDuration};

use arrow_array::RecordBatch;
use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	wire::{RawChangePayload, WireFormat as ClientWireFormat},
};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::batch::single_row};
use reifydb_runtime::{
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	sync::mutex::Mutex,
};
use reifydb_sub_core::{
	registry::{ConnectionId, SubscriptionRegistry},
	wire_sink::{ChangeDelivery, SubscribedEntry, WireSink},
};
use reifydb_subscription::delivery::{DeliveryResult, SubscriptionDelivery};
use reifydb_value::{
	error::Diagnostic,
	value::{
		Value, column_view::ColumnView, diff_type::DiffType, duration::Duration, frame::frame::Frame,
		uuid::Uuid7,
	},
};
use tokio::{runtime::Builder, sync::Notify, time::timeout};

#[derive(Debug, Clone, PartialEq)]
enum Call {
	Change(Vec<(SubscriptionId, Vec<i64>)>),
	Closed(SubscriptionId),
}

#[derive(Clone)]
struct RecordingSink {
	calls: Arc<Mutex<Vec<Call>>>,
	refused: Vec<SubscriptionId>,
	open: bool,
}

impl RecordingSink {
	fn new() -> Self {
		Self {
			calls: Arc::new(Mutex::new(Vec::new())),
			refused: Vec::new(),
			open: true,
		}
	}

	fn refusing(refused: Vec<SubscriptionId>) -> Self {
		Self {
			refused,
			..Self::new()
		}
	}

	fn closed() -> Self {
		Self {
			open: false,
			..Self::new()
		}
	}

	fn calls(&self) -> Vec<Call> {
		self.calls.lock().clone()
	}
}

impl WireSink for RecordingSink {
	type Format = ();

	fn client_wire_format(_format: ()) -> ClientWireFormat {
		ClientWireFormat::Rbcf
	}

	fn send_subscribed(&self, _subscriptions: &[SubscribedEntry]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_change(&self, _format: (), entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery {
		if !self.open {
			return ChangeDelivery::Disconnected;
		}
		let (failed, sent): (Vec<_>, Vec<_>) =
			entries.into_iter().partition(|(id, _)| self.refused.contains(id));
		if !sent.is_empty() {
			self.calls.lock().push(Call::Change(
				sent.into_iter()
					.map(|(id, frames)| {
						(id, frames.iter().map(|frame| first_value(&frame.batch)).collect())
					})
					.collect(),
			));
		}
		ChangeDelivery::Delivered {
			failed: failed.into_iter().map(|(id, _)| id).collect(),
		}
	}

	fn send_subscription_closed(&self, subscription_id: SubscriptionId) -> DeliveryResult {
		self.calls.lock().push(Call::Closed(subscription_id));
		DeliveryResult::Delivered
	}
}

fn first_value(columns: &RecordBatch) -> i64 {
	match ColumnView::try_from((columns.column(0), columns.schema_ref().field(0))).expect("one column").get_value(0)
	{
		Value::Int8(value) => value,
		other => panic!("expected Int8, got {other:?}"),
	}
}

fn row(value: i64) -> RecordBatch {
	single_row([("v", Value::Int8(value))]).unwrap()
}

fn frame(value: i64) -> Frame {
	Frame::from(row(value)).with_op(DiffType::Insert)
}

fn millis(ms: i64) -> Duration {
	Duration::from_milliseconds(ms).unwrap()
}

struct Fixture {
	mock: MockClock,
	clock: Clock,
	rng: Rng,
	registry: SubscriptionRegistry<RecordingSink>,
}

impl Fixture {
	fn new() -> Self {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		Self {
			mock,
			registry: SubscriptionRegistry::new(clock.clone()),
			clock,
			rng: Rng::seeded(42),
		}
	}

	fn connection(&self) -> ConnectionId {
		Uuid7::generate(&self.clock, &self.rng)
	}

	fn local(&self, id: u64, connection: ConnectionId, sink: &RecordingSink, linger: i64) {
		self.registry.subscribe(
			SubscriptionId(id),
			connection,
			sink.clone(),
			(),
			None,
			Duration::zero(),
			millis(linger),
		);
	}

	fn deliver(&self, id: u64, value: i64) {
		assert!(matches!(
			self.registry.try_deliver(&SubscriptionId(id), DiffType::Insert, row(value)),
			DeliveryResult::Delivered
		));
	}
}

fn ids(call: &Call) -> Vec<u64> {
	match call {
		Call::Change(entries) => {
			let mut ids: Vec<u64> = entries.iter().map(|(id, _)| id.0).collect();
			ids.sort();
			ids
		}
		Call::Closed(id) => panic!("expected a change, got a close for {id:?}"),
	}
}

#[test]
fn one_flush_sends_one_envelope_per_connection() {
	// One envelope per subscription multiplies frames; one envelope across connections leaks rows between sockets.
	let f = Fixture::new();
	let (a, b) = (RecordingSink::new(), RecordingSink::new());
	let (conn_a, conn_b) = (f.connection(), f.connection());
	f.local(1, conn_a, &a, 0);
	f.local(2, conn_a, &a, 0);
	f.local(3, conn_b, &b, 0);
	for id in [1, 2, 3] {
		f.deliver(id, id as i64 * 10);
	}

	f.registry.flush();

	assert_eq!(a.calls().len(), 1, "connection A must get exactly one envelope");
	assert_eq!(ids(&a.calls()[0]), vec![1, 2]);
	assert_eq!(b.calls().len(), 1, "connection B must get exactly one envelope");
	assert_eq!(ids(&b.calls()[0]), vec![3]);
}

#[test]
fn a_lingering_subscription_does_not_hold_back_its_neighbour() {
	// Taking the linger as a maximum over the connection delays every subscription to the slowest one.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	let connection = f.connection();
	f.local(1, connection, &sink, 0);
	f.local(2, connection, &sink, 50);
	f.deliver(1, 1);
	f.deliver(2, 2);

	f.registry.flush();
	assert_eq!(sink.calls(), vec![Call::Change(vec![(SubscriptionId(1), vec![1])])]);

	f.mock.advance_millis(50);
	f.registry.flush();
	assert_eq!(
		sink.calls(),
		vec![
			Call::Change(vec![(SubscriptionId(1), vec![1])]),
			Call::Change(vec![(SubscriptionId(2), vec![2])])
		]
	);
}

#[test]
fn promoting_queues_the_warming_changes_and_wakes_the_poller() {
	// Sending at promote skips shaping; forgetting the wake strands the buffered changes until an unrelated poll.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	let waker = Arc::new(Notify::new());
	f.registry.register_waker(waker.clone());
	f.registry.subscribe(
		SubscriptionId(1),
		f.connection(),
		sink.clone(),
		(),
		Some(10),
		Duration::zero(),
		Duration::zero(),
	);
	f.deliver(1, 1);
	f.deliver(1, 2);

	f.registry.promote_to_live(SubscriptionId(1));

	let woken = Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap()
		.block_on(async { timeout(StdDuration::from_millis(50), waker.notified()).await.is_ok() });
	assert!(woken, "promote must wake the poller");
	assert!(sink.calls().is_empty(), "promote must queue, not send");
	f.registry.flush();
	assert_eq!(sink.calls(), vec![Call::Change(vec![(SubscriptionId(1), vec![1, 2])])]);
}

#[test]
fn a_remote_change_waits_for_the_flush() {
	// A remote change sent at push bypasses the per-connection envelope and lands out of order with local changes.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	f.registry.subscribe_remote(SubscriptionId(9), f.connection(), sink.clone(), ());
	let payload = RawChangePayload::Rbcf(encode_frames(&[frame(5)], &EncodeOptions::fast()).unwrap());

	assert!(f.registry.push_payload(SubscriptionId(9), payload));
	assert!(sink.calls().is_empty(), "push must queue, not send");

	f.registry.flush();
	assert_eq!(sink.calls(), vec![Call::Change(vec![(SubscriptionId(9), vec![5])])]);
}

#[test]
fn a_lagged_subscription_is_closed_and_forgotten() {
	// A silent terminate leaves the client waiting forever on a subscription that no longer delivers.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	f.local(1, f.connection(), &sink, 0);

	f.registry.terminate(&SubscriptionId(1), Diagnostic::default());

	assert_eq!(sink.calls(), vec![Call::Closed(SubscriptionId(1))]);
	assert!(!f.registry.active_subscriptions().contains(&SubscriptionId(1)));
	assert!(matches!(
		f.registry.try_deliver(&SubscriptionId(1), DiffType::Insert, row(1)),
		DeliveryResult::Disconnected
	));
}

#[test]
fn unsubscribe_skips_ids_another_connection_owns() {
	// Without the owner check one connection can end another's subscriptions by guessing ids.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	let (a, b) = (f.connection(), f.connection());
	f.local(1, a, &sink, 0);
	f.local(2, b, &sink, 0);

	let removed = f.registry.unsubscribe_owned(a, &[SubscriptionId(1), SubscriptionId(2)]);

	assert_eq!(removed, vec![SubscriptionId(1)]);
	assert!(f.registry.active_subscriptions().contains(&SubscriptionId(2)), "B's subscription must stay");
}

#[test]
fn cleanup_and_unsubscribe_never_hand_back_a_remote_id() {
	// A remote id is the remote server's number; handing it to engine cleanup drops a local subscription sharing
	// it.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	let connection = f.connection();
	f.local(1, connection, &sink, 0);
	f.registry.subscribe_remote(SubscriptionId(2), connection, sink.clone(), ());
	f.local(3, connection, &sink, 0);
	f.registry.subscribe_remote(SubscriptionId(4), connection, sink.clone(), ());

	let unsubscribed = f.registry.unsubscribe_owned(connection, &[SubscriptionId(3), SubscriptionId(4)]);
	let cleaned = f.registry.cleanup_connection(connection);

	assert_eq!(unsubscribed, vec![SubscriptionId(3)]);
	assert_eq!(cleaned, vec![SubscriptionId(1)]);
}

#[test]
fn the_poller_never_sees_a_remote_id() {
	// The poller would probe the local store with a remote server's id and drain another subscription's rows.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	let connection = f.connection();
	f.local(1, connection, &sink, 0);
	f.registry.subscribe_remote(SubscriptionId(2), connection, sink.clone(), ());

	assert_eq!(f.registry.active_subscriptions(), vec![SubscriptionId(1)]);
}

#[test]
fn a_closed_remote_subscription_sends_its_pending_changes_first() {
	// Closing before draining loses the last changes, or sends them after the close notice.
	let f = Fixture::new();
	let sink = RecordingSink::new();
	f.registry.subscribe_remote(SubscriptionId(9), f.connection(), sink.clone(), ());
	assert!(f.registry.push_frames(SubscriptionId(9), vec![frame(1), frame(2)]));

	assert!(f.registry.close(SubscriptionId(9)));

	assert_eq!(
		sink.calls(),
		vec![Call::Change(vec![(SubscriptionId(9), vec![1, 2])]), Call::Closed(SubscriptionId(9))]
	);
	assert!(!f.registry.push_frames(SubscriptionId(9), vec![frame(3)]), "a closed subscription must refuse more");
}

#[test]
fn a_failed_entry_closes_only_its_own_subscription() {
	// Dropping the whole connection on one bad entry kills every neighbour that encoded fine.
	let f = Fixture::new();
	let sink = RecordingSink::refusing(vec![SubscriptionId(2)]);
	let connection = f.connection();
	f.local(1, connection, &sink, 0);
	f.local(2, connection, &sink, 0);
	f.deliver(1, 1);
	f.deliver(2, 2);

	f.registry.flush();

	assert_eq!(
		sink.calls(),
		vec![Call::Change(vec![(SubscriptionId(1), vec![1])]), Call::Closed(SubscriptionId(2))]
	);
	assert_eq!(f.registry.active_subscriptions(), vec![SubscriptionId(1)]);
}

#[test]
fn a_connection_lost_during_flush_keeps_its_ids_for_cleanup() {
	// Forgetting the ids at flush leaves the engine side registered, since cleanup then finds nothing to drop.
	let f = Fixture::new();
	let sink = RecordingSink::closed();
	let connection = f.connection();
	f.local(1, connection, &sink, 0);
	f.deliver(1, 1);

	f.registry.flush();
	f.deliver(1, 2);
	f.registry.flush();

	assert_eq!(f.registry.cleanup_connection(connection), vec![SubscriptionId(1)]);
}
