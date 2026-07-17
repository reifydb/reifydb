// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, frame::frame::Frame};

use crate::{ChangeKind, FrameChange};

pub(crate) fn frames_to_changes(frames: Vec<Frame>) -> Vec<FrameChange> {
	frames.into_iter()
		.map(|frame| {
			let kind = read_op_kind(&frame);
			FrameChange {
				kind,
				frame: strip_op_column(frame),
			}
		})
		.collect()
}

pub(crate) fn read_op_kind(frame: &Frame) -> ChangeKind {
	frame.columns
		.iter()
		.find(|c| c.name == "_op")
		.filter(|c| !c.data.is_empty())
		.map(|c| c.data.get_value(0))
		.and_then(value_to_op_int)
		.map(op_int_to_kind)
		.unwrap_or(ChangeKind::Insert)
}

pub(crate) fn strip_op_column(mut frame: Frame) -> Frame {
	frame.columns.retain(|c| c.name != "_op");
	frame
}

fn value_to_op_int(v: Value) -> Option<i64> {
	match v {
		Value::Int1(i) => Some(i as i64),
		Value::Int2(i) => Some(i as i64),
		Value::Int4(i) => Some(i as i64),
		Value::Int8(i) => Some(i),
		Value::Uint1(u) => Some(u as i64),
		Value::Uint2(u) => Some(u as i64),
		Value::Uint4(u) => Some(u as i64),
		Value::Uint8(u) => Some(u as i64),
		_ => None,
	}
}

fn op_int_to_kind(v: i64) -> ChangeKind {
	match v {
		2 => ChangeKind::Update,
		3 => ChangeKind::Remove,
		_ => ChangeKind::Insert,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{
		container::number::NumberContainer,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	};

	use super::*;

	fn frame_with_op(op: u8, id: i32) -> Frame {
		Frame::new(vec![
			FrameColumn {
				name: "_op".to_string(),
				data: FrameColumnData::Uint1(NumberContainer::from_vec(vec![op])),
			},
			FrameColumn {
				name: "id".to_string(),
				data: FrameColumnData::Int4(NumberContainer::from_vec(vec![id])),
			},
		])
	}

	#[test]
	fn derives_op_per_frame_and_strips_op() {
		// A batch member entry can concatenate frames of different ops. Each frame must
		// keep its own kind rather than all inheriting the first frame's op.
		let changes = frames_to_changes(vec![frame_with_op(1, 10), frame_with_op(2, 11), frame_with_op(3, 12)]);

		assert_eq!(changes.len(), 3);
		assert_eq!(changes[0].kind, ChangeKind::Insert);
		assert_eq!(changes[1].kind, ChangeKind::Update);
		assert_eq!(changes[2].kind, ChangeKind::Remove);

		for (change, expected_id) in changes.iter().zip([10, 11, 12]) {
			assert!(
				change.frame.columns.iter().all(|c| c.name != "_op"),
				"_op column must be stripped from each frame"
			);
			let id = change.frame.columns.iter().find(|c| c.name == "id").expect("id column preserved");
			assert_eq!(id.data.get_value(0), reifydb_value::value::Value::Int4(expected_id));
		}
	}

	#[test]
	fn unknown_op_defaults_to_insert() {
		let changes = frames_to_changes(vec![frame_with_op(99, 1)]);
		assert_eq!(changes[0].kind, ChangeKind::Insert);
	}
}
