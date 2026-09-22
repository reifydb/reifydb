// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use arrow_buffer::BooleanBuffer;
use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_sub_core::wire_sink::WireSink;
use reifydb_sub_server::format::WireFormat;
use reifydb_sub_server_ws::subscription::registry::{PushMessage, WsWireSink};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::value::{
	diff_type::DiffType,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
};
use tokio::sync::mpsc::unbounded_channel;

fn change(data: FrameColumnData) -> Vec<Frame> {
	let frame = Frame::new(vec![FrameColumn {
		name: "a".to_string(),
		data,
	}]);
	vec![frame.with_op(DiffType::Insert)]
}

fn int4() -> FrameColumnData {
	FrameColumnData::Int4(Int32Array::from(vec![7]))
}

fn option_layers(depth: usize) -> FrameColumnData {
	(0..depth).fold(int4(), |inner, _| FrameColumnData::Option {
		inner: Box::new(inner),
		bitvec: BooleanBuffer::from(vec![true]),
	})
}

#[test]
fn a_batch_that_fails_to_rbcf_encode_is_closed_on_the_subscriber() {
	// The registry drops an undelivered batch, so a client not told it closed waits forever for its changes.
	let unencodable = change(option_layers(4));
	assert!(
		encode_frames(&unencodable, &EncodeOptions::fast()).is_err(),
		"the entry must be unencodable for this test to mean anything"
	);
	let (push_tx, mut push_rx) = unbounded_channel();
	let sink = WsWireSink::new(push_tx);
	let batch_id = "1".parse::<BatchId>().unwrap();
	let entries = vec![(SubscriptionId(1), change(int4())), (SubscriptionId(2), unencodable)];

	let result = sink.send_batch_envelope(batch_id, WireFormat::Rbcf, entries);

	assert!(matches!(result, DeliveryResult::Disconnected), "got {result:?}");
	let pushed = push_rx.try_recv();
	assert!(
		matches!(&pushed, Ok(PushMessage::BatchClosed { batch_id: closed }) if *closed == batch_id),
		"expected the batch to be closed on the subscriber, got {pushed:?}"
	);
	assert!(push_rx.try_recv().is_err(), "a failed batch must not also push a partial change");
}

#[test]
fn a_change_that_fails_to_rbcf_encode_is_announced_to_the_subscriber() {
	// The registry drops an undelivered subscription, so a client told nothing waits forever for its changes.
	let columns = Columns::from(change(option_layers(4)).remove(0));
	assert!(
		encode_frames(&[Frame::from(columns.clone()).with_op(DiffType::Insert)], &EncodeOptions::fast())
			.is_err(),
		"the change must be unencodable for this test to mean anything"
	);
	let (push_tx, mut push_rx) = unbounded_channel();
	let sink = WsWireSink::new(push_tx);

	let result = sink.send_change(SubscriptionId(1), DiffType::Insert, columns, WireFormat::Rbcf);

	assert!(matches!(result, DeliveryResult::Disconnected), "got {result:?}");
	assert!(push_rx.try_recv().is_ok(), "the subscriber was told nothing about the dropped change");
}
