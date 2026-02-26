// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	error::diagnostic::catalog::series_not_found,
	interface::{evaluate::TargetColumn, resolved::{ResolvedColumn, ResolvedPrimitive}},
	internal_error,
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::{
	expression::{Expression, name::column_name_from_expression},
	nodes::UpdateSeriesNode,
	query::QueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::{bitvec::BitVec, cowvec::CowVec},
	value::Value,
};
use tracing::instrument;

use crate::{
	expression::{
		cast::cast_column_data,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::{services::Services, stack::SymbolTable, volcano::scan::series::build_data_column},
};

#[instrument(name = "mutate::series::update", level = "trace", skip_all)]
pub(crate) fn update_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateSeriesNode,
	params: Params,
) -> crate::Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let namespace = services.catalog.find_namespace_by_name(txn, namespace_name)?.unwrap();

	let series_name = plan.target.name();
	let Some(series_def) = services.catalog.find_series_by_name(txn, namespace.id, series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series_def.tag.is_some();

	// Extract filter conditions and patch assignments from the plan
	let (conditions, assignments) = match *plan.input {
		QueryPlan::Patch(patch) => {
			let conditions = if let Some(input) = patch.input {
				match *input {
					QueryPlan::Filter(filter) => filter.conditions,
					_ => vec![],
				}
			} else {
				vec![]
			};
			(conditions, patch.assignments)
		}
		_ => (vec![], vec![]),
	};

	let mut updated_count = 0u64;
	let mut updates_to_apply: Vec<(EncodedKey, Vec<u8>)> = Vec::new();

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

		// Compile filter conditions and patch assignments
		let stack = SymbolTable::new();
		let compile_ctx = CompileContext {
			functions: &services.functions,
			symbol_table: &stack,
		};

		// Evaluate filter to get mask of matching rows
		let mut filter_mask = BitVec::repeat(row_count, true);
		if !conditions.is_empty() {
			let compiled_filters: Vec<CompiledExpr> = conditions
				.iter()
				.map(|e| compile_expression(&compile_ctx, e).expect("compile"))
				.collect();

			for compiled_expr in &compiled_filters {
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
		}

		// Apply patch assignments to matching rows
		let compiled_patches: Vec<CompiledExpr> =
			assignments.iter().map(|e| compile_expression(&compile_ctx, e).expect("compile")).collect();

		let patch_names: Vec<Fragment> = assignments.iter().map(column_name_from_expression).collect();

		// Build the resolved source for target column resolution
		let resolved_source = ResolvedPrimitive::Series(plan.target.clone());

		// Evaluate patch expressions on ALL rows (we'll only use results for matching ones)
		let mut patch_columns = Vec::with_capacity(assignments.len());
		for (expr, compiled_expr) in assignments.iter().zip(compiled_patches.iter()) {
			let mut exec_ctx = EvalContext {
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

			// Set target column for type coercion
			if let Expression::Alias(alias_expr) = expr {
				let alias_name = alias_expr.alias.name();
				if let Some(table_column) =
					resolved_source.columns().iter().find(|col| col.name == alias_name)
				{
					let column_ident = Fragment::internal(&table_column.name);
					let resolved_column = ResolvedColumn::new(
						column_ident,
						resolved_source.clone(),
						table_column.clone(),
					);
					exec_ctx.target = Some(TargetColumn::Resolved(resolved_column));
				}
			}

			let mut column = compiled_expr.execute(&exec_ctx)?;

			if let Some(target_type) = exec_ctx.target.as_ref().map(|t| t.column_type()) {
				if column.data.get_type() != target_type {
					let data = cast_column_data(
						&exec_ctx,
						&column.data,
						target_type,
						&expr.lazy_fragment(),
					)?;
					column = Column {
						name: column.name,
						data,
					};
				}
			}

			patch_columns.push(column);
		}

		// Build patched columns by merging originals with patches
		let mut patched = Vec::new();
		for original_col in columns.into_iter() {
			let original_name = original_col.name().text();
			if let Some(patch_idx) = patch_names.iter().position(|n| n.text() == original_name) {
				patched.push(patch_columns[patch_idx].clone());
			} else {
				patched.push(original_col);
			}
		}
		let patched_columns = Columns::new(patched);

		// Write updated values back to storage for matching rows
		for row_idx in 0..row_count {
			if !filter_mask.get(row_idx) {
				continue;
			}

			// Build new data column values from the patched row
			let mut data_values = Vec::with_capacity(series_def.columns.len());
			for col_def in &series_def.columns {
				let value = if let Some(input_col) =
					patched_columns.iter().find(|c| c.name().text() == col_def.name)
				{
					input_col.data().get_value(row_idx)
				} else {
					Value::none()
				};
				data_values.push(value);
			}

			let encoded_values = postcard::to_allocvec(&data_values)
				.map_err(|e| internal_error!("Failed to serialize series row values: {}", e))?;

			updates_to_apply.push((keys[row_idx].clone(), encoded_values));
		}
	}

	// Apply all collected updates
	for (key, encoded_values) in &updates_to_apply {
		txn.set(key, EncodedValues(CowVec::new(encoded_values.clone())))?;
		updated_count += 1;
	}

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("series", Value::Utf8(series_def.name)),
		("updated", Value::Uint8(updated_count)),
	]))
}
