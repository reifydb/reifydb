// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, frame::frame::Frame};

use crate::ChangeKind;

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
