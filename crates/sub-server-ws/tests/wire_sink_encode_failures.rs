// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	frame::{encode::encode_frames, options::EncodeOptions},
	wire::RawChangePayload,
};
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_core::wire_sink::WireSink;
use reifydb_sub_server::format::WireFormat;
use reifydb_sub_server_ws::subscription::registry::{PushMessage, WsWireSink};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{
		container::number::NumberContainer,
		diff_type::DiffType,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	},
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

fn sink() -> (WsWireSink, UnboundedReceiver<PushMessage>) {
	let (push_tx, push_rx) = unbounded_channel();
	(WsWireSink::new(push_tx), push_rx)
}

fn change(data: FrameColumnData) -> Vec<Frame> {
	let frame = Frame::new(vec![FrameColumn {
		name: "a".to_string(),
		data,
	}]);
	vec![frame.with_op(DiffType::Insert)]
}

fn int4() -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(vec![7]))
}

fn option_layers(depth: usize) -> FrameColumnData {
	(0..depth).fold(int4(), |inner, _| FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BitVec::from_slice(&[true]),
	})
}

#[test]
fn a_batch_entry_that_fails_to_rbcf_encode_is_not_reported_as_delivered() {
	// Skipping the entry and answering Delivered leaves that subscriber missing a change with only a log line.
	let unencodable = change(option_layers(4));
	assert!(
		encode_frames(&unencodable, &EncodeOptions::fast()).is_err(),
		"the entry must be unencodable for this test to mean anything"
	);
	let (sink, mut push_rx) = sink();
	let entries = vec![(SubscriptionId(1), change(int4())), (SubscriptionId(2), unencodable)];

	let result = sink.send_batch_envelope("1".parse::<BatchId>().unwrap(), WireFormat::Rbcf, entries);

	assert!(
		!matches!(result, DeliveryResult::Delivered),
		"an encode failure was reported as delivered, pushed {:?}",
		push_rx.try_recv()
	);
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
