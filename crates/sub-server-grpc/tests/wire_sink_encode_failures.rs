// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_codec::frame::{encode::encode_frames, options::EncodeOptions};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::batch::batch};
use reifydb_sub_core::wire_sink::WireSink;
use reifydb_sub_server_grpc::subscription::{GrpcWireSink, WireFormat};
use reifydb_subscription::delivery::DeliveryResult;
use reifydb_value::value::{
	container::digest_array::digest_array,
	diff_type::DiffType,
	digest::Digest,
	frame::frame::Frame,
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};
use tokio::sync::mpsc::unbounded_channel;

fn frame(column: (FieldRef, ArrayRef)) -> Frame {
	Frame::from(batch(vec![column]).unwrap())
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
fn a_change_that_fails_to_rbcf_encode_reaches_the_subscriber_as_an_error_not_an_empty_change() {
	// Empty rbcf bytes read as an empty change on the client, so an encode failure must travel as a status.
	let columns = frame(unencodable_digest()).batch;
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
