// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions},
	json::{from::frames_from_json, to::convert_frames},
};
use reifydb_value::value::{
	container::number::NumberContainer,
	diff_type::DiffType,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	row_number::RowNumber,
};
use serde_json::to_string;

fn frame_with_op(op: Option<DiffType>) -> Frame {
	let mut frame = Frame::with_row_numbers(
		vec![FrameColumn {
			name: "id".to_string(),
			data: FrameColumnData::Int4(NumberContainer::new(vec![7])),
		}],
		vec![RowNumber::new(42)],
	);
	frame.op = op;
	frame
}

#[test]
fn rbcf_carries_every_op_in_the_reserved_header_byte() {
	// The op lives in a byte the frame header already reserved, so it must survive without a
	// version bump and without changing the header size.
	for op in [DiffType::Insert, DiffType::Update, DiffType::Remove] {
		let encoded = encode_frames(&[frame_with_op(Some(op))], &EncodeOptions::default()).expect("encode");
		let decoded = decode_frames(&encoded).expect("decode");
		assert_eq!(decoded[0].op, Some(op));
		assert_eq!(decoded[0].row_numbers(), &[RowNumber::new(42)]);
	}
}

#[test]
fn an_absent_op_decodes_as_absent_not_as_insert() {
	// A plain query result has no op at all. Decoding the zero byte as Insert would make every
	// query response look like a change notification.
	let encoded = encode_frames(&[frame_with_op(None)], &EncodeOptions::default()).expect("encode");
	let decoded = decode_frames(&encoded).expect("decode");
	assert_eq!(decoded[0].op, None);
}

#[test]
fn the_op_never_becomes_a_column() {
	// Moving the op out of band is what makes a user column named `_op` safe; if the op ever came
	// back as a column the original collision would return.
	let encoded =
		encode_frames(&[frame_with_op(Some(DiffType::Remove))], &EncodeOptions::default()).expect("encode");
	let decoded = decode_frames(&encoded).expect("decode");
	assert_eq!(decoded[0].columns.len(), 1);
	assert_eq!(decoded[0].columns[0].name, "id");
}

#[test]
fn the_json_frames_format_round_trips_the_op() {
	// The frames format is a separate encoder from RBCF; both must report the same op or two
	// clients on the same subscription would disagree about what happened to the row.
	for op in [DiffType::Insert, DiffType::Update, DiffType::Remove] {
		let response = convert_frames(&[frame_with_op(Some(op))]);
		assert_eq!(response[0].op, Some(DiffType::as_u8(op)));

		let back = frames_from_json(&to_string(&response).unwrap()).expect("decode");
		assert_eq!(back[0].op, Some(op));
		assert_eq!(back[0].row_numbers(), &[RowNumber::new(42)]);
	}
}

#[test]
fn the_json_frames_format_omits_an_absent_op() {
	// A query response must not gain an `op` key just because change notifications carry one.
	let response = convert_frames(&[frame_with_op(None)]);
	assert_eq!(response[0].op, None);
	assert!(!to_string(&response).unwrap().contains("\"op\""));
}
