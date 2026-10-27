// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	wire::RawChangePayload,
};
use reifydb_core::{
	interface::catalog::id::SubscriptionId,
	value::{batch::batch, column::factory},
};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_sub_server::format::WireFormat;
use reifydb_sub_server_ws::subscription::registry::{SubscriptionRegistry, WsWireSink};
use reifydb_subscription::delivery::SubscriptionDelivery;
use reifydb_value::value::{diff_type::DiffType, frame::frame::Frame, uuid::Uuid7};
use tokio::sync::mpsc::unbounded_channel;

fn change(column: (FieldRef, ArrayRef)) -> Vec<Frame> {
	let frame = Frame::from(batch(vec![column]).unwrap());
	vec![frame.with_op(DiffType::Insert)]
}

fn int4() -> (FieldRef, ArrayRef) {
	factory::int4("a", [7])
}

#[test]
fn a_remote_change_that_fails_to_decode_is_not_delivered_as_an_empty_change() {
	// An empty change is a valid delivery, so a payload the server cannot decode must never look like one.
	let mut corrupt = encode_frames(&change(int4()), &EncodeOptions::fast()).unwrap();
	corrupt.truncate(corrupt.len() / 2);

	for format in [WireFormat::Json, WireFormat::Frames, WireFormat::Rbcf] {
		let clock = Clock::Mock(MockClock::from_millis(1000));
		let registry = SubscriptionRegistry::new(clock.clone());
		let (push_tx, mut push_rx) = unbounded_channel();
		registry.subscribe_remote(
			SubscriptionId(1),
			Uuid7::generate(&clock, &Rng::seeded(1)),
			WsWireSink::new(push_tx),
			format,
		);

		let pushed = registry.push_payload(SubscriptionId(1), RawChangePayload::Rbcf(corrupt.clone()));
		registry.flush();

		assert!(!pushed, "{format:?}: a corrupt change was accepted");
		assert!(push_rx.try_recv().is_err(), "{format:?}: a corrupt change reached the socket");
	}
}
