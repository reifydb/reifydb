// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::number::NumberContainer,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	row_number::RowNumber,
	system_columns::SystemColumns,
};

fn no_rows() -> Vec<FrameColumn> {
	vec![FrameColumn {
		name: "x".to_string(),
		data: FrameColumnData::Int4(NumberContainer::new(vec![])),
	}]
}

#[test]
fn a_frame_emptied_by_take_still_renders_the_rownum_column() {
	// Without the bit, dropping the last row also drops #rownum and the empty answer disagrees with the full one.
	let mut system = SystemColumns::new(vec![RowNumber(1), RowNumber(2)], vec![], vec![], vec![], vec![], vec![]);
	system.take(0);
	let frame = Frame {
		system,
		columns: no_rows(),
		op: None,
	};

	assert!(frame.has_row_numbers());
	let rendered = frame.to_string();
	assert!(rendered.contains("#rownum"), "an emptied frame must keep its #rownum header, got:\n{rendered}");
}

#[test]
fn a_frame_without_row_numbers_renders_no_rownum_column() {
	// The bit must not default to on, or aggregates and dictionaries would grow a #rownum they never carry.
	let frame = Frame::new(no_rows());

	assert!(!frame.has_row_numbers());
	let rendered = frame.to_string();
	assert!(!rendered.contains("#rownum"), "a frame without row numbers must not render #rownum, got:\n{rendered}");
}
