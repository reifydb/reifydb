// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subquery opcodes: ExecSubqueryExists, ExecSubqueryIn, ExecSubqueryScalar.

use reifydb_type::Value;

use crate::error::{Result, VmError};
use crate::runtime::dispatch::DispatchResult;
use crate::runtime::operand::OperandValue;

use super::HandlerContext;

/// ExecSubqueryExists - execute a subquery and check if it returns any rows.
pub fn exec_subquery_exists(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let subquery_index = ctx.read_u16()?;
	let negated = ctx.read_u8()? != 0;

	// Get the subquery definition
	let subquery_def = ctx.vm.program.subqueries.get(subquery_index as usize).ok_or(
		VmError::InvalidSubqueryIndex {
			index: subquery_index,
		},
	)?;
	let subquery_def = subquery_def.clone();

	// Execute the subquery
	let result = ctx.vm.execute_subquery(&subquery_def, ctx.tx.as_deref_mut())?;

	// EXISTS returns true if any rows, NOT EXISTS returns true if no rows
	let row_count = result.row_count();
	let exists = row_count > 0;
	let value = if negated {
		!exists
	} else {
		exists
	};

	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(value)))?;
	Ok(ctx.advance_and_continue())
}

/// ExecSubqueryIn - execute a subquery and check if a value is in the result.
pub fn exec_subquery_in(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let subquery_index = ctx.read_u16()?;
	let negated = ctx.read_u8()? != 0;

	// Pop the value to check
	let check_value = ctx.vm.pop_operand()?;
	let check_scalar = match &check_value {
		OperandValue::Scalar(v) => v.clone(),
		_ => {
			return Err(VmError::TypeMismatchStr {
				expected: "scalar value".to_string(),
				found: "non-scalar".to_string(),
			});
		}
	};

	// Get the subquery definition
	let subquery_def = ctx.vm.program.subqueries.get(subquery_index as usize).ok_or(
		VmError::InvalidSubqueryIndex {
			index: subquery_index,
		},
	)?;
	let subquery_def = subquery_def.clone();

	// Execute the subquery
	let result = ctx.vm.execute_subquery(&subquery_def, ctx.tx.as_deref_mut())?;

	// Check if value is in the first column of the result
	let found = if result.is_empty() || result.columns.is_empty() {
		false
	} else {
		let first_column = &result.columns[0];
		// Iterate through the column data to check for the value
		first_column.data().iter().any(|v| v == check_scalar)
	};

	let value = if negated {
		!found
	} else {
		found
	};
	ctx.vm.push_operand(OperandValue::Scalar(Value::Boolean(value)))?;
	Ok(ctx.advance_and_continue())
}

/// ExecSubqueryScalar - execute a subquery and return a single scalar value.
pub fn exec_subquery_scalar(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let subquery_index = ctx.read_u16()?;

	// Get the subquery definition
	let subquery_def = ctx.vm.program.subqueries.get(subquery_index as usize).ok_or(
		VmError::InvalidSubqueryIndex {
			index: subquery_index,
		},
	)?;
	let subquery_def = subquery_def.clone();

	// Execute the subquery
	let result = ctx.vm.execute_subquery(&subquery_def, ctx.tx.as_deref_mut())?;

	// Scalar subquery must return exactly one row and one column
	if result.row_count() > 1 {
		return Err(VmError::SubqueryMultipleRows {
			expected: 1,
			found: result.row_count(),
		});
	}

	let value = if result.is_empty() || result.columns.is_empty() {
		Value::Undefined
	} else {
		let first_column = &result.columns[0];
		// Get the first value from the column
		first_column.data().iter().next().unwrap_or(Value::Undefined)
	};

	ctx.vm.push_operand(OperandValue::Scalar(value))?;
	Ok(ctx.advance_and_continue())
}
