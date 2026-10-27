// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions},
	wire::{RawChangePayload, WireFormat as ClientWireFormat},
};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::batch::single_row};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_sub_core::{
	registry::SubscriptionRegistry,
	wire_sink::{ChangeDelivery, SubscribedEntry, WireSink},
};
use reifydb_subscription::delivery::DeliveryResult;
use reifydb_value::value::{Value, diff_type::DiffType, frame::frame::Frame, uuid::Uuid7};

#[derive(Clone)]
struct OpenSink;

impl WireSink for OpenSink {
	type Format = ();

	fn client_wire_format(_format: ()) -> ClientWireFormat {
		ClientWireFormat::Rbcf
	}

	fn send_subscribed(&self, _subscriptions: &[SubscribedEntry]) -> DeliveryResult {
		DeliveryResult::Delivered
	}

	fn send_change(&self, _format: (), _entries: Vec<(SubscriptionId, Vec<Frame>)>) -> ChangeDelivery {
		ChangeDelivery::Delivered {
			failed: Vec::new(),
		}
	}

	fn send_subscription_closed(&self, _subscription_id: SubscriptionId) -> DeliveryResult {
		DeliveryResult::Delivered
	}
}

fn encoded_change() -> Vec<u8> {
	let frames = vec![Frame::from(single_row([("v", Value::Int8(99))]).unwrap()).with_op(DiffType::Insert)];
	encode_frames(&frames, &EncodeOptions::fast()).unwrap()
}

#[test]
fn a_remote_change_that_fails_to_decode_stops_the_proxy() {
	// Queued as an empty change, undecodable bytes would keep the proxy alive and the subscriber never told.
	let clock = Clock::Mock(MockClock::from_millis(1000));
	let rng = Rng::seeded(42);
	let registry = SubscriptionRegistry::new(clock.clone());
	let subscription_id = SubscriptionId(42);
	registry.subscribe_remote(subscription_id, Uuid7::generate(&clock, &rng), OpenSink, ());
	let bytes = encoded_change();
	let mut corrupt = bytes.clone();
	corrupt.truncate(corrupt.len() / 2);
	assert!(
		decode_frames(&corrupt).is_err(),
		"the truncated payload must be undecodable for this test to mean anything"
	);
	assert!(
		registry.push_payload(subscription_id, RawChangePayload::Rbcf(bytes)),
		"a decodable change must reach the subscription for this test to mean anything"
	);

	let pushed = registry.push_payload(subscription_id, RawChangePayload::Rbcf(corrupt));

	assert!(!pushed, "an undecodable remote change was accepted as a delivery");
}

#[test]
fn a_remote_change_for_an_unsubscribed_subscription_is_refused() {
	// Without the unknown-id check a released subscription keeps delivering and its proxy never stops.
	let clock = Clock::Mock(MockClock::from_millis(1000));
	let rng = Rng::seeded(42);
	let registry = SubscriptionRegistry::new(clock.clone());
	let connection_id = Uuid7::generate(&clock, &rng);
	let kept = SubscriptionId(1);
	let released = SubscriptionId(2);
	registry.subscribe_remote(kept, connection_id, OpenSink, ());
	registry.subscribe_remote(released, connection_id, OpenSink, ());
	assert!(
		registry.push_payload(released, RawChangePayload::Rbcf(encoded_change())),
		"a change must reach the subscription while it is still registered for this test to mean anything"
	);
	assert!(registry.unsubscribe(released), "the subscription must be registered before it can be released");

	let pushed = registry.push_payload(released, RawChangePayload::Rbcf(encoded_change()));

	assert!(!pushed, "a remote change for a released subscription was accepted, so its proxy keeps running");
	assert!(
		registry.push_payload(kept, RawChangePayload::Rbcf(encoded_change())),
		"releasing one subscription must not stop the ones that are still subscribed"
	);
}
