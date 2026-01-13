// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Frame/Record opcodes: FrameLen, FrameRow, GetField.

use reifydb_type::Value;

use crate::error::{Result, VmError};
use crate::runtime::dispatch::DispatchResult;
use crate::runtime::operand::{OperandValue, Record};

use super::HandlerContext;

/// FrameLen - get the number of rows in a frame.
pub fn frame_len(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let frame = ctx.vm.pop_operand()?;
	match frame {
		OperandValue::Frame(columns) => {
			let len = columns.row_count() as i64;
			ctx.vm.push_operand(OperandValue::Scalar(Value::Int8(len)))?;
		}
		_ => return Err(VmError::ExpectedFrame),
	}
	Ok(ctx.advance_and_continue())
}

/// FrameRow - extract a row from a frame as a record.
pub fn frame_row(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let index = ctx.vm.pop_operand()?;
	let frame = ctx.vm.pop_operand()?;

	let row_index = match index {
		OperandValue::Scalar(Value::Int8(n)) => n as usize,
		_ => return Err(VmError::ExpectedInteger),
	};

	match frame {
		OperandValue::Frame(columns) => {
			// Build a Record from the row at the given index
			let mut fields = Vec::new();
			for col in columns.iter() {
				let name = col.name().text().to_string();
				let value = col.data().get_value(row_index);
				fields.push((name, value));
			}
			ctx.vm.push_operand(OperandValue::Record(Record::new(fields)))?;
		}
		_ => return Err(VmError::ExpectedFrame),
	}
	Ok(ctx.advance_and_continue())
}

/// GetField - get a field value from a record.
pub fn get_field(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let name_index = ctx.read_u16()?;
	let record = ctx.vm.pop_operand()?;
	let field_name = ctx.vm.get_constant_string(name_index)?;

	match record {
		OperandValue::Record(rec) => {
			let value = rec.get(&field_name).cloned().unwrap_or(Value::Undefined);
			ctx.vm.push_operand(OperandValue::Scalar(value))?;
		}
		_ => return Err(VmError::ExpectedRecord),
	}
	Ok(ctx.advance_and_continue())
}
