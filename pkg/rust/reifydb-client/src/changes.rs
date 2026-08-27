// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{diff_type::DiffType, frame::frame::Frame};

use crate::{ChangeKind, FrameChange};

pub(crate) fn frames_to_changes(frames: Vec<Frame>) -> Vec<FrameChange> {
	frames.into_iter()
		.map(|frame| FrameChange {
			kind: read_op_kind(&frame),
			frame,
		})
		.collect()
}

pub(crate) fn read_op_kind(frame: &Frame) -> ChangeKind {
	match frame.op {
		Some(DiffType::Update) => ChangeKind::Update,
		Some(DiffType::Remove) => ChangeKind::Remove,
		_ => ChangeKind::Insert,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{
		Value,
		container::number::NumberContainer,
		frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	};

	use super::*;

	fn frame_with_op(op: DiffType, id: i32) -> Frame {
		Frame::new(vec![FrameColumn {
			name: "id".to_string(),
			data: FrameColumnData::Int4(NumberContainer::from_vec(vec![id])),
		}])
		.with_op(op)
	}

	#[test]
	fn derives_op_per_frame_and_keeps_every_user_column() {
		// A batch member entry can concatenate frames of different ops. Each frame must keep its
		// own kind rather than all inheriting the first frame's op, and the op must no longer
		// occupy a column - a user table with a column named `_op` used to collide with it.
		let changes = frames_to_changes(vec![
			frame_with_op(DiffType::Insert, 10),
			frame_with_op(DiffType::Update, 11),
			frame_with_op(DiffType::Remove, 12),
		]);

		assert_eq!(changes.len(), 3);
		assert_eq!(changes[0].kind, ChangeKind::Insert);
		assert_eq!(changes[1].kind, ChangeKind::Update);
		assert_eq!(changes[2].kind, ChangeKind::Remove);

		for (change, expected_id) in changes.iter().zip([10, 11, 12]) {
			assert_eq!(
				change.frame.columns.len(),
				1,
				"the op must ride the frame, so it must not add a column"
			);
			let id = change.frame.columns.iter().find(|c| c.name == "id").expect("id column preserved");
			assert_eq!(id.data.get_value(0), Value::Int4(expected_id));
		}
	}

	#[test]
	fn frame_without_op_defaults_to_insert() {
		// Hydration and any non-subscription frame arrive with no op set; treating them as
		// inserts is what a subscriber needs to seed its initial state.
		let changes = frames_to_changes(vec![Frame::new(vec![FrameColumn {
			name: "id".to_string(),
			data: FrameColumnData::Int4(NumberContainer::from_vec(vec![1])),
		}])]);
		assert_eq!(changes[0].kind, ChangeKind::Insert);
	}
}
