// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! State snapshot conversion from live VM state.

use super::entry::{
	CallFrameSnapshot, ColumnSnapshot, DispatchResultSnapshot, FrameSnapshot, OperandSnapshot, OperatorSnapshot,
	RecordSnapshot, ScopeSnapshot, StateSnapshot,
};
use crate::{
	bytecode::OperatorKind,
	vmcore::{
		call_stack::CallFrame,
		interpreter::DispatchResult,
		state::{OperandValue, Record, VmState},
	},
};

/// Create a state snapshot from the current VM state.
pub fn snapshot_state(state: &VmState) -> StateSnapshot {
	StateSnapshot {
		ip: state.ip,
		operand_stack: state.operand_stack.iter().map(snapshot_operand).collect(),
		pipeline_stack: (0..state.pipeline_stack.len()).map(|i| format!("Pipeline#{}", i)).collect(),
		scopes: snapshot_scopes(state),
		call_stack: state.call_stack.iter().map(snapshot_call_frame).collect(),
	}
}

/// Snapshot the scope chain from VM state.
fn snapshot_scopes(state: &VmState) -> Vec<ScopeSnapshot> {
	state.scopes
		.iter()
		.enumerate()
		.map(|(depth, scope)| ScopeSnapshot {
			depth,
			variables: scope.iter().map(|(name, value)| (name.clone(), snapshot_operand(value))).collect(),
		})
		.collect()
}

/// Snapshot an operand value.
pub fn snapshot_operand(value: &OperandValue) -> OperandSnapshot {
	match value {
		OperandValue::Scalar(v) => OperandSnapshot::Scalar(v.clone()),
		OperandValue::Column(col) => OperandSnapshot::Frame(snapshot_column(col)),
		OperandValue::ExprRef(index) => OperandSnapshot::ExprRef(*index),
		OperandValue::ColRef(name) => OperandSnapshot::ColRef(name.clone()),
		OperandValue::ColList(cols) => OperandSnapshot::ColList(cols.clone()),
		OperandValue::Frame(columns) => OperandSnapshot::Frame(snapshot_frame(columns)),
		OperandValue::FunctionRef(index) => OperandSnapshot::FunctionRef(*index),
		OperandValue::PipelineRef(handle) => OperandSnapshot::PipelineRef {
			id: handle.id,
		},
		OperandValue::SortSpecRef(index) => OperandSnapshot::SortSpecRef(*index),
		OperandValue::ExtSpecRef(index) => OperandSnapshot::ExtSpecRef(*index),
		OperandValue::Record(rec) => OperandSnapshot::Record(snapshot_record(rec)),
	}
}

/// Snapshot a frame (Columns).
fn snapshot_frame(columns: &reifydb_core::value::column::Columns) -> FrameSnapshot {
	let row_count = columns.row_count();
	let column_snapshots: Vec<ColumnSnapshot> = columns
		.iter()
		.map(|col| ColumnSnapshot {
			name: col.name().text().to_string(),
			data_type: format!("{:?}", col.data().get_type()),
		})
		.collect();

	// Capture all rows
	let rows: Vec<Vec<reifydb_type::Value>> = (0..row_count).map(|i| columns.row(i)).collect();

	FrameSnapshot {
		row_count,
		columns: column_snapshots,
		rows,
	}
}

/// Snapshot a single column as a frame.
fn snapshot_column(col: &reifydb_core::value::column::Column) -> FrameSnapshot {
	let row_count = col.data().len();
	let column_snapshot = ColumnSnapshot {
		name: col.name().text().to_string(),
		data_type: format!("{:?}", col.data().get_type()),
	};

	// Capture all values as single-element rows
	let rows: Vec<Vec<reifydb_type::Value>> = (0..row_count).map(|i| vec![col.data().get_value(i)]).collect();

	FrameSnapshot {
		row_count,
		columns: vec![column_snapshot],
		rows,
	}
}

/// Snapshot a record.
fn snapshot_record(record: &Record) -> RecordSnapshot {
	RecordSnapshot {
		fields: record.fields.clone(),
	}
}

/// Snapshot a call frame.
pub fn snapshot_call_frame(frame: &CallFrame) -> CallFrameSnapshot {
	CallFrameSnapshot {
		function_index: frame.function_index,
		return_address: frame.return_address,
		operand_base: frame.operand_base,
		pipeline_base: frame.pipeline_base,
		scope_depth: frame.scope_depth,
	}
}

/// Snapshot a dispatch result.
pub fn snapshot_dispatch_result(result: &DispatchResult) -> DispatchResultSnapshot {
	match result {
		DispatchResult::Continue => DispatchResultSnapshot::Continue,
		DispatchResult::Halt => DispatchResultSnapshot::Halt,
		DispatchResult::Yield(_) => DispatchResultSnapshot::Yield,
	}
}

/// Snapshot an operator kind.
pub fn snapshot_operator(kind: OperatorKind) -> OperatorSnapshot {
	match kind {
		OperatorKind::Filter => OperatorSnapshot::Filter,
		OperatorKind::Select => OperatorSnapshot::Select,
		OperatorKind::Extend => OperatorSnapshot::Extend,
		OperatorKind::Take => OperatorSnapshot::Take,
		OperatorKind::Sort => OperatorSnapshot::Sort,
	}
}

/// Get pipeline description for a given index.
pub fn pipeline_description(index: usize) -> String {
	format!("Pipeline#{}", index)
}
