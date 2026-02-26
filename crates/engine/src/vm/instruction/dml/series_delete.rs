// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	error::diagnostic::catalog::series_not_found,
	internal_error,
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::{nodes::DeleteSeriesNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, return_error, util::bitvec::BitVec, value::Value};
use tracing::instrument;

use crate::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::{services::Services, stack::SymbolTable, volcano::scan::series::build_data_column},
};

#[instrument(name = "mutate::series::delete", level = "trace", skip_all)]
pub(crate) fn delete_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteSeriesNode,
	params: Params,
) -> crate::Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let namespace = services.catalog.find_namespace_by_name(txn, namespace_name)?.unwrap();

	let series_name = plan.target.name();
	let Some(series_def) = services.catalog.find_series_by_name(txn, namespace.id, series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	// Get current metadata
	let Some(mut metadata) = services.catalog.find_series_metadata(txn, series_def.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series_def.tag.is_some();
	let mut deleted_count = 0u64;

	if let Some(input_plan) = plan.input {
		// Extract filter conditions from the plan
		let conditions = match *input_plan {
			QueryPlan::Filter(filter) => filter.conditions,
			_ => vec![],
		};

		// Scan all rows directly from storage
		let range = SeriesRowKeyRange::full_scan(series_def.id, None);
		let mut keys = Vec::new();
		let mut timestamps = Vec::new();
		let mut tags = Vec::new();
		let mut data_rows: Vec<Vec<Value>> = Vec::new();

		{
			let mut stream = txn.range(range, 4096)?;
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = SeriesRowKey::decode(&entry.key) {
					keys.push(entry.key);
					timestamps.push(key.timestamp);
					if has_tag {
						tags.push(key.variant_tag.unwrap_or(0));
					}
					let values: Vec<Value> = postcard::from_bytes(&entry.values).map_err(|e| {
						internal_error!("Failed to deserialize series row values: {}", e)
					})?;
					data_rows.push(values);
				}
			}
		}

		if !keys.is_empty() {
			// Build Columns from scanned data
			let mut result_columns = Vec::new();
			result_columns.push(Column {
				name: Fragment::internal("timestamp"),
				data: ColumnData::int8(timestamps),
			});
			if has_tag {
				result_columns.push(Column {
					name: Fragment::internal("tag"),
					data: ColumnData::uint1(tags),
				});
			}
			for (col_idx, col_def) in series_def.columns.iter().enumerate() {
				let col_type = col_def.constraint.get_type();
				let col_values: Vec<Value> = data_rows
					.iter()
					.map(|row| row.get(col_idx).cloned().unwrap_or(Value::none()))
					.collect();
				result_columns.push(build_data_column(&col_def.name, &col_values, col_type)?);
			}
			let columns = Columns::new(result_columns);
			let row_count = columns.row_count();

			// Compile and evaluate filter conditions
			let stack = SymbolTable::new();
			let compile_ctx = CompileContext {
				functions: &services.functions,
				symbol_table: &stack,
			};
			let compiled_exprs: Vec<CompiledExpr> = conditions
				.iter()
				.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
				.collect();

			let mut filter_mask = BitVec::repeat(row_count, true);
			for compiled_expr in &compiled_exprs {
				let exec_ctx = EvalContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: &params,
					symbol_table: &stack,
					is_aggregate_context: false,
					functions: &services.functions,
					clock: &services.clock,
					arena: None,
				};

				let result = compiled_expr.execute(&exec_ctx)?;
				match result.data() {
					ColumnData::Bool(container) => {
						for i in 0..row_count {
							if filter_mask.get(i) {
								let valid = container.is_defined(i);
								let filter_result = container.data().get(i);
								filter_mask.set(i, valid & filter_result);
							}
						}
					}
					ColumnData::Option {
						inner,
						bitvec,
					} => match inner.as_ref() {
						ColumnData::Bool(container) => {
							for i in 0..row_count {
								if filter_mask.get(i) {
									let defined = i < bitvec.len() && bitvec.get(i);
									let valid = defined && container.is_defined(i);
									let value = valid && container.data().get(i);
									filter_mask.set(i, value);
								}
							}
						}
						_ => panic!("filter expression must evaluate to a boolean column"),
					},
					_ => panic!("filter expression must evaluate to a boolean column"),
				}
			}

			// Delete matching rows
			for (i, key) in keys.iter().enumerate() {
				if filter_mask.get(i) {
					txn.remove(key)?;
					deleted_count += 1;
				}
			}
		}
	} else {
		// Delete all rows - scan the full range and delete
		let range = SeriesRowKeyRange::full_scan(series_def.id, None);
		let mut keys_to_delete = Vec::new();

		let mut stream = txn.range(range, 4096)?;
		while let Some(entry) = stream.next() {
			let entry = entry?;
			keys_to_delete.push(entry.key);
		}
		drop(stream);

		for key in &keys_to_delete {
			txn.remove(key)?;
			deleted_count += 1;
		}
	}

	// Update metadata
	metadata.row_count = metadata.row_count.saturating_sub(deleted_count);
	if metadata.row_count == 0 {
		metadata.oldest_timestamp = 0;
		metadata.newest_timestamp = 0;
	}

	services.catalog.update_series_metadata_txn(txn, metadata)?;

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("series", Value::Utf8(series_def.name)),
		("deleted", Value::Uint8(deleted_count)),
	]))
}
