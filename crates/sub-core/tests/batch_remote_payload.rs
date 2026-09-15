// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions},
	wire::{RawChangePayload, WireFormat as ClientWireFormat},
};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_sub_core::{
	registry::SubscriptionRegistry,
	wire_sink::{BatchSubscribedEntry, WireSink},
};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::value::{Value, diff_type::DiffType, duration::Duration, frame::frame::Frame, uuid::Uuid7};

#[derive(Clone)]
struct OpenSink;

impl WireSink for OpenSink {
	type Format = ();

	fn client_wire_format(_format: ()) -> ClientWireFormat {
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
		_columns: Columns,
		_format: (),
	) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_remote_change(
		&self,
		_sub_id: SubscriptionId,
		_payload: RawChangePayload,
		_format: (),
	) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_batch_envelope(
		&self,
		_batch_id: BatchId,
		_format: (),
		_entries: Vec<(SubscriptionId, Vec<Frame>)>,
	) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_batch_subscription_closed(&self, _batch_id: BatchId, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_closed(&self, _sub_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}
}

#[test]
fn a_remote_batch_change_that_fails_to_decode_stops_the_proxy() {
	// Queued as an empty change, undecodable bytes would keep the proxy alive and the subscriber never told.
	let clock = Clock::Mock(MockClock::from_millis(1000));
	let rng = Rng::seeded(42);
	let registry = SubscriptionRegistry::new(clock.clone());
	let subscription_id = SubscriptionId(42);
	let batch_id = registry.register_batch(
		Uuid7::generate(&clock, &rng),
		vec![(subscription_id, Duration::zero())],
		OpenSink,
		(),
		&clock,
		&rng,
	);
	let frames = vec![Frame::from(Columns::single_row([("v", Value::Int8(99))])).with_op(DiffType::Insert)];
	let bytes = encode_frames(&frames, &EncodeOptions::fast()).unwrap();
	let mut corrupt = bytes.clone();
	corrupt.truncate(corrupt.len() / 2);
	assert!(
		decode_frames(&corrupt).is_err(),
		"the truncated payload must be undecodable for this test to mean anything"
	);
	assert!(
		registry.push_batch_payload(batch_id, subscription_id, RawChangePayload::Rbcf(bytes)),
		"a decodable change must reach the batch for this test to mean anything"
	);

	let pushed = registry.push_batch_payload(batch_id, subscription_id, RawChangePayload::Rbcf(corrupt));

	assert!(!pushed, "an undecodable remote change was accepted as a delivery");
}
