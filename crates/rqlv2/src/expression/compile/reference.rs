// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Reference compilation (columns, variables, rownum, wildcard).

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use super::helpers::broadcast_value;
use crate::expression::{
	eval::value::EvalValue,
	types::{CompiledExpr, EvalError},
};

pub(super) fn compile_column_ref(name: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, ctx| {
		// Try name-based lookup in columns
		if let Some(col) = columns.iter().find(|c| c.name().text() == name) {
			return Ok(col.clone());
		}

		// Check outer row values for correlated subqueries
		if let Some(outer_values) = &ctx.current_row_values {
			if let Some(value) = outer_values.get(&name) {
				return broadcast_value(value, columns.row_count());
			}
		}

		Err(EvalError::ColumnNotFound {
			name: name.clone(),
		})
	})
}

pub(super) fn compile_variable_ref(id: u32, name: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, ctx| {
		let value = ctx.get_var(id).ok_or(EvalError::VariableNotFound {
			id,
		})?;

		match value {
			EvalValue::Scalar(v) => broadcast_value(v, columns.row_count()),
			EvalValue::Record(_) => Err(EvalError::TypeMismatch {
				expected: "scalar".to_string(),
				found: "record".to_string(),
				context: format!("variable '{}'", name),
			}),
		}
	})
}

pub(super) fn compile_rownum() -> CompiledExpr {
	CompiledExpr::new(|columns, _ctx| {
		let row_count = columns.row_count();
		let values: Vec<i64> = (0..row_count as i64).collect();
		Ok(Column::new(Fragment::internal("_rownum"), ColumnData::int8(values)))
	})
}

pub(super) fn compile_wildcard() -> CompiledExpr {
	// Wildcard should be expanded during planning
	CompiledExpr::new(|_, _| {
		Err(EvalError::UnsupportedOperation {
			operation: "wildcard should be expanded during planning".to_string(),
		})
	})
}

pub(super) fn compile_field_access(_base: CompiledExpr, field: String) -> CompiledExpr {
	// Field access on records is not supported in the columnar expression evaluator.
	// Use the bytecode VM for record field access.
	CompiledExpr::new(move |_, _| {
		Err(EvalError::UnsupportedOperation {
			operation: format!("field access '.{}' requires bytecode VM", field),
		})
	})
}
