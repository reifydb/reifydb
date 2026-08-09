// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::{bytes::EncodedBytes, shape::RowShape};
use reifydb_core::{
	interface::catalog::{column::Column, dictionary::Dictionary},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{identity::IdentityId, row_number::RowNumber, system_columns::SystemColumns},
};

use crate::{
	Result,
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::{services::Services, stack::SymbolTable, volcano::decode_dictionary_columns},
};

pub(crate) fn decode_rows_to_columns(shape: &RowShape, rows: &[(RowNumber, EncodedBytes)]) -> Columns {
	let fields = shape.fields();

	let mut columns_vec: Vec<ColumnWithName> = Vec::with_capacity(fields.len());
	for field in fields.iter() {
		columns_vec.push(ColumnWithName {
			name: Fragment::internal(&field.name),
			data: ColumnBuffer::with_capacity(field.constraint.get_type(), rows.len()),
		});
	}

	let mut row_numbers = Vec::with_capacity(rows.len());
	let mut created_at = Vec::with_capacity(rows.len());
	let mut updated_at = Vec::with_capacity(rows.len());
	let mut time = Vec::with_capacity(rows.len());
	for (row_number, encoded) in rows {
		row_numbers.push(*row_number);
		created_at.push(shape.created_at(encoded));
		updated_at.push(shape.updated_at(encoded));
		if let Some(t) = shape.time(encoded) {
			time.push(t);
		}
		for (i, _) in fields.iter().enumerate() {
			columns_vec[i].data.push_value(shape.get_value(encoded, i));
		}
	}

	Columns::with_system(columns_vec, SystemColumns::new(row_numbers, Vec::new(), created_at, updated_at, time))
}

pub(crate) fn decode_returning_dictionaries(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	object_columns: &[Column],
	columns: &mut Columns,
) -> Result<()> {
	let mut dictionaries: Vec<Option<Dictionary>> = Vec::with_capacity(columns.len());
	for column in columns.iter() {
		let dict_id =
			object_columns.iter().find(|c| c.name == column.name().text()).and_then(|c| c.dictionary_id);
		match dict_id {
			Some(id) => dictionaries.push(services.catalog.find_dictionary(txn, id)?),
			None => dictionaries.push(None),
		}
	}
	decode_dictionary_columns(columns, &dictionaries, txn)
}

fn try_column_passthrough(exprs: &[Expression], input: &Columns) -> Option<Columns> {
	let mut cols: Vec<ColumnWithName> = Vec::with_capacity(exprs.len());
	for expr in exprs {
		let Expression::Column(col_expr) = expr else {
			return None;
		};
		let name = col_expr.0.name.text();
		let col = input.column(name)?;
		cols.push(ColumnWithName::new(col.name().clone(), col.data().clone()));
	}
	if !input.row_numbers().is_empty() {
		Some(Columns::with_system(
			cols,
			SystemColumns::new(
				input.row_numbers().to_vec(),
				Vec::new(),
				input.created_at().to_vec(),
				input.updated_at().to_vec(),
				input.time().to_vec(),
			),
		))
	} else {
		Some(Columns::new(cols))
	}
}

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
		symbols,
	};

	let compiled: Vec<CompiledExpr> = returning_exprs
		.iter()
		.map(|e| compile_expression(&compile_ctx, e).expect("compile returning expression"))
		.collect();

	let row_count = input.row_count();
	let base = EvalContext {
		params: &Params::None,
		symbols,
		routines: &services.routines,
		runtime_context: &services.runtime_context,
		identity: IdentityId::root(),
		is_aggregate_context: false,
		columns: Columns::empty(),
		row_count: 1,
		target: None,
		take: None,
	};

	let mut new_columns = Vec::with_capacity(compiled.len());
	for compiled_expr in &compiled {
		let exec_ctx = base.with_eval(input.clone(), row_count);
		let column = compiled_expr.execute(&exec_ctx)?;
		new_columns.push(column);
	}

	if !input.row_numbers().is_empty() {
		Ok(Columns::with_system(
			new_columns,
			SystemColumns::new(
				input.row_numbers().to_vec(),
				Vec::new(),
				input.created_at().to_vec(),
				input.updated_at().to_vec(),
				input.time().to_vec(),
			),
		))
	} else {
		Ok(Columns::new(new_columns))
	}
}
