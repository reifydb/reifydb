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
use reifydb_sub_core::wire_sink::WireSink;
use reifydb_sub_server::format::WireFormat;
use reifydb_sub_server_ws::subscription::registry::{PushMessage, WsWireSink};
use reifydb_subscription::delivery::DeliveryResult;
use reifydb_value::value::{diff_type::DiffType, frame::frame::Frame};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

fn sink() -> (WsWireSink, UnboundedReceiver<PushMessage>) {
	let (push_tx, push_rx) = unbounded_channel();
	(WsWireSink::new(push_tx), push_rx)
}

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

	for format in [WireFormat::Json, WireFormat::Frames] {
		let (sink, mut push_rx) = sink();

		let result =
			sink.send_remote_change(SubscriptionId(1), RawChangePayload::Rbcf(corrupt.clone()), format);

		assert!(
			!matches!(result, DeliveryResult::Delivered),
			"{format:?}: a corrupt change was delivered as {:?}",
			push_rx.try_recv()
		);
	}
}
