// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::catch_unwind;

use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions},
	wire::RawChangePayload,
};
use reifydb_value::value::{
	container::number::NumberContainer,
	diff_type::DiffType,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
};

fn frame(op: Option<DiffType>) -> Frame {
	let mut frame = Frame::new(vec![FrameColumn {
		name: "a".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(vec![7])),
	}]);
	frame.op = op;
	frame
}

fn encode(op: Option<DiffType>) -> Vec<u8> {
	encode_frames(&[frame(op)], &EncodeOptions::default()).unwrap()
}

#[test]
fn an_unknown_rbcf_frame_op_is_a_decode_error_not_an_absent_op() {
	// An absent op reads as a query frame, so an unknown op byte must not silently drop the change kind.
	let without_op = encode(None);
	let mut bytes = encode(Some(DiffType::Insert));
	let op_at: Vec<usize> = (0..bytes.len()).filter(|at| bytes[*at] != without_op[*at]).collect();
	assert_eq!(op_at.len(), 1, "the op must live in exactly one header byte");
	bytes[op_at[0]] = 9;

	let result = decode_frames(&bytes).map(|frames| frames[0].op);

	assert!(result.is_err(), "expected a decode error, got {result:?}");
}

#[test]
fn a_corrupt_raw_change_payload_is_not_read_as_zero_frames() {
	// An empty change is a valid delivery, so bytes that fail to decode must never look like one.
	let mut bytes = encode(Some(DiffType::Insert));
	bytes.truncate(bytes.len() / 2);
	assert!(
		decode_frames(&bytes).is_err(),
		"the truncated payload must be undecodable for this test to mean anything"
	);

	let result = catch_unwind(|| RawChangePayload::Rbcf(bytes).into_frames().map(|f| f.len()));

	assert!(!matches!(result, Ok(Ok(0))), "a corrupt change payload decoded as zero frames");
}
