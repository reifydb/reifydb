// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::batch::batch};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_sub_core::wire_sink::{ChangeDelivery, WireSink};
use reifydb_sub_server::format::WireFormat;
use reifydb_sub_server_ws::subscription::registry::{PushMessage, SubscriptionRegistry, WsWireSink};
use reifydb_subscription::delivery::SubscriptionDelivery;
use reifydb_value::value::{
	container::digest_array::digest_array,
	diff_type::DiffType,
	digest::Digest,
	duration::Duration,
	frame::frame::Frame,
	uuid::Uuid7,
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};
use tokio::sync::mpsc::unbounded_channel;

fn change(column: (FieldRef, ArrayRef)) -> Vec<Frame> {
	let frame = Frame::from(batch(vec![column]).unwrap());
	vec![frame.with_op(DiffType::Insert)]
}

fn unencodable_digest() -> (FieldRef, ArrayRef) {
	// The Utf8 inner is what rbcf refuses, so the one row must be a real digest, not a none.
	let digest_type = ValueType::Digest {
		inner: Box::new(ValueType::Utf8),
		accuracy: 10_000,
	};
	named(
		"a",
		FieldType::from(digest_type),
		Arc::new(digest_array([Some(Digest::new(ValueType::Float8, 10_000).unwrap())])),
	)
}

#[test]
fn a_change_that_fails_to_rbcf_encode_is_announced_to_the_subscriber() {
	// A subscription dropped without a notice leaves its client waiting forever for changes.
	let columns = change(unencodable_digest()).remove(0).batch;
	assert!(
		encode_frames(&[Frame::from(columns.clone()).with_op(DiffType::Insert)], &EncodeOptions::fast())
			.is_err(),
		"the change must be unencodable for this test to mean anything"
	);
	let (push_tx, mut push_rx) = unbounded_channel();
	let sink = WsWireSink::new(push_tx);
	assert_eq!(
		sink.send_change(
			WireFormat::Rbcf,
			vec![(SubscriptionId(1), vec![Frame::from(columns.clone()).with_op(DiffType::Insert)])]
		),
		ChangeDelivery::Delivered {
			failed: vec![SubscriptionId(1)]
		},
		"the sink must name the entry it could not encode"
	);
	assert!(push_rx.try_recv().is_err(), "the unencodable entry must not reach the socket");

	let clock = Clock::Mock(MockClock::from_millis(1000));
	let registry = SubscriptionRegistry::new(clock.clone());
	registry.subscribe(
		SubscriptionId(1),
		Uuid7::generate(&clock, &Rng::seeded(1)),
		sink,
		WireFormat::Rbcf,
		None,
		Duration::zero(),
		Duration::zero(),
	);
	registry.try_deliver(&SubscriptionId(1), DiffType::Insert, columns);
	registry.flush();

	match push_rx.try_recv() {
		Ok(PushMessage::SubscriptionClosed {
			subscription_id,
		}) => assert_eq!(subscription_id, SubscriptionId(1)),
		other => panic!("the subscriber must be told its subscription closed, got {other:?}"),
	}
	assert!(push_rx.try_recv().is_err(), "nothing may follow the close notice");
	assert!(registry.active_subscriptions().is_empty(), "the closed subscription must be forgotten");
}
