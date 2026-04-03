// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::expression::Expression;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{identity::IdentityId, row_number::RowNumber},
};

use crate::{
	Result,
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalSession},
	},
	vm::{services::Services, stack::SymbolTable},
};

/// Decode multiple encoded rows into a single Columns structure using the shape.
pub(crate) fn decode_rows_to_columns(shape: &RowShape, rows: &[(RowNumber, EncodedRow)]) -> Columns {
	let fields = shape.fields();

	let mut columns_vec: Vec<Column> = Vec::with_capacity(fields.len());
	for field in fields.iter() {
		columns_vec.push(Column {
			name: Fragment::internal(&field.name),
			data: ColumnData::with_capacity(field.constraint.get_type(), rows.len()),
		});
	}

	let mut row_numbers = Vec::with_capacity(rows.len());
	for (row_number, encoded) in rows {
		row_numbers.push(*row_number);
		for (i, _) in fields.iter().enumerate() {
			columns_vec[i].data.push_value(shape.get_value(encoded, i));
		}
	}

	Columns {
		row_numbers: CowVec::new(row_numbers),
		created_at: CowVec::new(Vec::new()),
		updated_at: CowVec::new(Vec::new()),
		columns: CowVec::new(columns_vec),
	}
}

/// If every RETURNING expression is a simple `Expression::Column`, extract
/// those columns from `input` by name and return a new `Columns`.
/// Returns `None` if any expression is not a plain column reference or
/// if a referenced column is missing from `input`.
fn try_column_passthrough(exprs: &[Expression], input: &Columns) -> Option<Columns> {
	let mut cols = Vec::with_capacity(exprs.len());
	for expr in exprs {
		let Expression::Column(col_expr) = expr else {
			return None;
		};
		let name = col_expr.0.name.text();
		let col = input.column(name)?;
		cols.push(col.clone());
	}
	if !input.row_numbers.is_empty() {
		Some(Columns::with_row_numbers(cols, input.row_numbers.to_vec()))
	} else {
		Some(Columns::new(cols))
	}
}

/// Evaluate RETURNING expressions against the given columns.
pub(crate) fn evaluate_returning(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	returning_exprs: &[Expression],
	input: Columns,
) -> Result<Columns> {
	if let Some(columns) = try_column_passthrough(returning_exprs, &input) {
		return Ok(columns);
	}

	let compile_ctx = CompileContext {
		functions: &services.functions,
		symbols,
	};

	let compiled: Vec<CompiledExpr> = returning_exprs
		.iter()
		.map(|e| compile_expression(&compile_ctx, e).expect("compile returning expression"))
		.collect();

	let row_count = input.row_count();
	let session = EvalSession {
		params: &Params::None,
		symbols,
		functions: &services.functions,
		runtime_context: &services.runtime_context,
		arena: None,
		identity: IdentityId::root(),
		is_aggregate_context: false,
	};

	let mut new_columns = Vec::with_capacity(compiled.len());
	for compiled_expr in &compiled {
		let exec_ctx = session.eval(input.clone(), row_count);
		let column = compiled_expr.execute(&exec_ctx)?;
		new_columns.push(column);
	}

	if !input.row_numbers.is_empty() {
		Ok(Columns::with_row_numbers(new_columns, input.row_numbers.to_vec()))
	} else {
		Ok(Columns::new(new_columns))
	}
}
