// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::FieldRef;
use reifydb_codec::row::{bytes::EncodedBytes, series::EncodedSeriesRow, shape::RowShape};
use reifydb_core::{
	expression::Expression,
	interface::catalog::{column::Column, dictionary::Dictionary},
	internal_err,
	value::{
		batch::{batch, empty_batch, from_encoded_bytes},
		column::factory,
	},
};
use reifydb_evaluate::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	stack::SymbolTable,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	params::Params,
	value::{
		column_view::ColumnView,
		datetime::DateTime,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{SystemColumn, stamp_system_columns, system_column, user_columns, with_system_column},
		value_type::ValueType,
	},
};

use crate::{
	Result,
	vm::{services::Services, volcano::decode_dictionary_columns},
};

pub(crate) fn decode_rows_to_columns(shape: &RowShape, rows: &[(RowNumber, EncodedBytes)]) -> Result<RecordBatch> {
	let (ids, encoded): (Vec<RowNumber>, Vec<EncodedBytes>) = rows.iter().cloned().unzip();
	from_encoded_bytes(shape, &ids, &encoded)
}

pub(crate) fn with_series_stamps(
	columns: Vec<(FieldRef, ArrayRef)>,
	row_numbers: &[RowNumber],
	encoded: &[EncodedBytes],
) -> Result<RecordBatch> {
	let rows: Vec<&EncodedSeriesRow> = encoded.iter().map(EncodedSeriesRow::view).collect();
	let rn: ArrayRef = Arc::new(UInt64Array::from_iter_values(row_numbers.iter().map(|row_number| row_number.0)));
	let mut stamps: Vec<(SystemColumn, ArrayRef)> = vec![
		(SystemColumn::RowNumbers, rn),
		(
			SystemColumn::CreatedAt,
			factory::datetime(SystemColumn::CreatedAt.name(), rows.iter().map(|row| row.created_at())).1,
		),
		(
			SystemColumn::UpdatedAt,
			factory::datetime(SystemColumn::UpdatedAt.name(), rows.iter().map(|row| row.updated_at())).1,
		),
	];
	let times: Vec<DateTime> = rows.iter().filter_map(|row| row.time()).collect();
	match times.len() {
		0 => {}
		stamped if stamped == rows.len() => {
			stamps.push((SystemColumn::Time, factory::datetime(SystemColumn::Time.name(), times).1));
		}
		stamped => {
			return internal_err!(
				"{} of {} series rows carry a {} stamp",
				stamped,
				rows.len(),
				SystemColumn::Time
			);
		}
	}
	stamp_system_columns(batch(columns)?, stamps)
}

pub(crate) fn with_pre_image(post: RecordBatch, pre: &RecordBatch) -> Result<RecordBatch> {
	let mut merged: Vec<(FieldRef, ArrayRef)> =
		user_columns(&post).map(|(field, array)| (field.clone(), array.clone())).collect();
	for (field, array) in user_columns(pre) {
		merged.push(factory::rename((field.clone(), array.clone()), &format!("pre_{}", field.name())));
	}
	let stamps: Vec<(SystemColumn, ArrayRef)> = SystemColumn::ALL
		.into_iter()
		.filter(|column| *column != SystemColumn::CommitVersion)
		.filter_map(|column| system_column(&post, column).map(|array| (column, array.clone())))
		.collect();
	stamp_system_columns(batch(merged)?, stamps)
}

pub(crate) fn with_absent_pre_image(post: RecordBatch) -> Result<RecordBatch> {
	let row_count = post.num_rows();
	let absent = batch(user_columns(&post)
		.map(|(field, array)| {
			let ty = ColumnView::try_from((array, field.as_ref()))?.get_type();
			Ok(factory::none_typed(field.name(), ty, row_count))
		})
		.collect::<Result<Vec<_>>>()?)?;
	with_pre_image(post, &absent)
}

pub(crate) fn decode_returning_dictionaries(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	object_columns: &[Column],
	columns: RecordBatch,
) -> Result<RecordBatch> {
	let mut dictionaries: Vec<Option<(Dictionary, ValueType)>> = Vec::with_capacity(columns.num_columns());
	for (field, _) in user_columns(&columns) {
		let column = object_columns.iter().find(|c| c.name == *field.name());
		match column.and_then(|c| c.dictionary_id.map(|id| (id, c.constraint.get_type()))) {
			Some((id, declared)) => dictionaries.push(services
				.catalog
				.find_dictionary(txn, id)?
				.map(|dictionary| (dictionary, declared))),
			None => dictionaries.push(None),
		}
	}
	decode_dictionary_columns(columns, &dictionaries, txn)
}

fn try_column_passthrough(exprs: &[Expression], input: &RecordBatch) -> Result<Option<RecordBatch>> {
	let mut cols: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(exprs.len());
	for expr in exprs {
		let Expression::Column(col_expr) = expr else {
			return Ok(None);
		};
		let name = col_expr.0.name.text();
		let Some((field, array)) = user_columns(input).find(|(field, _)| field.name() == name) else {
			return Ok(None);
		};
		cols.push((field.clone(), array.clone()));
	}
	Ok(Some(carry_row_numbers(batch(cols)?, input)?))
}

fn carry_row_numbers(columns: RecordBatch, input: &RecordBatch) -> Result<RecordBatch> {
	match system_column(input, SystemColumn::RowNumbers) {
		Some(array) => with_system_column(columns, SystemColumn::RowNumbers, array.clone()),
		None => Ok(columns),
	}
}

pub(crate) fn evaluate_returning(
	services: &Arc<Services>,
	symbols: &SymbolTable,
	returning_exprs: &[Expression],
	input: RecordBatch,
	identity: IdentityId,
) -> Result<RecordBatch> {
	if let Some(columns) = try_column_passthrough(returning_exprs, &input)? {
		return Ok(columns);
	}

	let compile_ctx = CompileContext {
		symbols,
	};

	let compiled: Vec<CompiledExpr> =
		returning_exprs.iter().map(|e| compile_expression(&compile_ctx, e)).collect::<Result<Vec<_>>>()?;

	let row_count = input.num_rows();
	let base = EvalContext {
		params: &Params::None,
		symbols,
		routines: &services.routines,
		runtime_context: &services.runtime_context,
		identity,
		is_aggregate_context: false,
		batch: empty_batch(),
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

	carry_row_numbers(batch(new_columns)?, &input)
}
