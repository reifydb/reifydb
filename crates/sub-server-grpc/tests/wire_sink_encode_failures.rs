// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use arrow_buffer::BooleanBuffer;
use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_sub_core::wire_sink::WireSink;
use reifydb_sub_server_grpc::subscription::{GrpcWireSink, WireFormat};
use reifydb_subscription::{batch::BatchId, delivery::DeliveryResult};
use reifydb_value::value::{
	container::digest_array::digest_array,
	diff_type::DiffType,
	digest::Digest,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};
use tokio::sync::mpsc::unbounded_channel;

fn frame(data: FrameColumnData) -> Frame {
	Frame::new(vec![FrameColumn {
		name: "a".to_string(),
		data,
	}])
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

fn unencodable_digest() -> FrameColumnData {
	FrameColumnData::Digest {
		container: digest_array([None::<Digest>]),
		inner: ValueType::Utf8,
		accuracy: 10_000,
	}
}

#[test]
fn a_change_that_fails_to_rbcf_encode_reaches_the_subscriber_as_an_error_not_an_empty_change() {
	// Empty rbcf bytes read as an empty change on the client, so an encode failure must travel as a status.
	let columns = Columns::from(frame(unencodable_digest()));
	assert!(
		encode_frames(&[Frame::from(columns.clone()).with_op(DiffType::Insert)], &EncodeOptions::fast())
			.is_err(),
		"the change must be unencodable for this test to mean anything"
	);
	let (tx, mut rx) = unbounded_channel();
	let sink = GrpcWireSink::Single(tx);

	let result = sink.send_change(SubscriptionId(1), DiffType::Insert, columns, WireFormat::Rbcf);

	assert!(matches!(result, DeliveryResult::Disconnected), "got {result:?}");
	let pushed = rx.try_recv();
	assert!(matches!(&pushed, Ok(Err(_))), "expected an error status on the stream, got {pushed:?}");
}

#[test]
fn a_batch_entry_that_fails_to_rbcf_encode_reaches_the_subscriber_as_an_error_not_an_empty_entry() {
	// An entry with empty rbcf bytes reads as an empty change on the client, so the batch must fail as a status.
	let unencodable = vec![frame(option_layers(4)).with_op(DiffType::Insert)];
	assert!(
		encode_frames(&unencodable, &EncodeOptions::fast()).is_err(),
		"the entry must be unencodable for this test to mean anything"
	);
	let (tx, mut rx) = unbounded_channel();
	let sink = GrpcWireSink::Batch(tx);
	let entries = vec![
		(SubscriptionId(1), vec![frame(int4()).with_op(DiffType::Insert)]),
		(SubscriptionId(2), unencodable),
	];

	let result = sink.send_batch_envelope("1".parse::<BatchId>().unwrap(), WireFormat::Rbcf, entries);

	assert!(matches!(result, DeliveryResult::Disconnected), "got {result:?}");
	let pushed = rx.try_recv();
	assert!(matches!(&pushed, Ok(Err(_))), "expected an error status on the stream, got {pushed:?}");
}
