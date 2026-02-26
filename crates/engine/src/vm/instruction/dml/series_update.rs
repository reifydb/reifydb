// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, key::EncodedKey},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::primitive::PrimitiveId,
		change::{Change, ChangeOrigin, Diff},
		evaluate::TargetColumn,
		resolved::{ResolvedColumn, ResolvedPrimitive},
	},
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
	value::{Value, row_number::RowNumber},
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
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let series_name = plan.target.name();
	let Some(series_def) = services.catalog.find_series_by_name(txn, namespace.id, series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series_def.tag.is_some();

	// Extract filter conditions, patch assignments, and scan bounds from the plan.
	// The physical plan optimizer may push timestamp/tag predicates into the
	// SeriesScan node and remove the Filter node entirely. We need to recover
	// those bounds so the scan is properly limited.
	let (conditions, assignments, scan_start, scan_end, scan_tag) = match *plan.input {
		QueryPlan::Patch(patch) => {
			let (conditions, start, end, tag) = if let Some(input) = patch.input {
				match *input {
					QueryPlan::Filter(filter) => {
						let (start, end, tag) = match *filter.input {
							QueryPlan::SeriesScan(scan) => (
								scan.time_range_start,
								scan.time_range_end,
								scan.variant_tag,
							),
							_ => (None, None, None),
						};
						(filter.conditions, start, end, tag)
					}
					QueryPlan::SeriesScan(scan) => {
						(vec![], scan.time_range_start, scan.time_range_end, scan.variant_tag)
					}
					_ => (vec![], None, None, None),
				}
			} else {
				(vec![], None, None, None)
			};
			(conditions, patch.assignments, start, end, tag)
		}
		_ => (vec![], vec![], None, None, None),
	};

	let mut updated_count = 0u64;
	let mut updates_to_apply: Vec<(EncodedKey, EncodedValues)> = Vec::new();
	let mut updated_row_indices: Vec<usize> = Vec::new();
	let mut pre_columns: Option<Columns> = None;
	let mut post_columns: Option<Columns> = None;
	let mut scanned_timestamps: Vec<i64> = Vec::new();
	let mut scanned_sequences: Vec<u64> = Vec::new();

	// Use bounded scan when time range or tag is available from predicate pushdown
	let range = if scan_start.is_some() || scan_end.is_some() || scan_tag.is_some() {
		SeriesRowKeyRange::scan_range(series_def.id, scan_tag, scan_start, scan_end, None)
	} else {
		SeriesRowKeyRange::full_scan(series_def.id, None)
	};
	let mut keys = Vec::new();
	let mut timestamps = Vec::new();
	let mut tags = Vec::new();
	let mut data_rows: Vec<Vec<Value>> = Vec::new();

	// Get the schema for decoding series values
	let read_schema = super::schema::get_or_create_series_schema(&services.catalog, &series_def, txn)?;

	{
		let mut stream = txn.range(range, 4096)?;
		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(key) = SeriesRowKey::decode(&entry.key) {
				keys.push(entry.key);
				timestamps.push(key.timestamp);
				scanned_sequences.push(key.sequence);
				if has_tag {
					tags.push(key.variant_tag.unwrap_or(0));
				}
				let mut values = Vec::with_capacity(series_def.columns.len());
				for (i, _col) in series_def.columns.iter().enumerate() {
					values.push(read_schema.get_value(&entry.values, i + 1));
				}
				data_rows.push(values);
			}
		}
	}

	if !keys.is_empty() {
		scanned_timestamps = timestamps.clone();

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

		// Save pre-columns for flow change tracking before consuming
		pre_columns = Some(columns.clone());

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

			let schema = super::schema::get_or_create_series_schema(&services.catalog, &series_def, txn)?;
			let mut row = schema.allocate();
			// Get timestamp for this row
			let ts = patched_columns
				.iter()
				.find(|c| c.name().text() == "timestamp")
				.map(|c| c.data().get_value(row_idx))
				.unwrap_or(Value::Int8(0));
			schema.set_value(&mut row, 0, &ts);
			for (i, value) in data_values.iter().enumerate() {
				schema.set_value(&mut row, i + 1, value);
			}

			updates_to_apply.push((keys[row_idx].clone(), row));
			updated_row_indices.push(row_idx);
		}

		post_columns = Some(patched_columns);
	}

	// Apply all collected updates
	for (key, row) in &updates_to_apply {
		txn.set(key, row.clone())?;
		updated_count += 1;
	}

	// Track flow changes for updated rows
	if let (Some(pre_cols), Some(post_cols)) = (&pre_columns, &post_columns) {
		for &row_idx in &updated_row_indices {
			let timestamp = scanned_timestamps[row_idx];
			let row_number = RowNumber::from(scanned_sequences[row_idx]);

			// Build pre columns (original values)
			let mut pre_col_vec = Vec::with_capacity(1 + series_def.columns.len());
			pre_col_vec.push(Column {
				name: Fragment::internal("timestamp"),
				data: ColumnData::int8(vec![timestamp]),
			});
			for col in pre_cols.iter() {
				if col.name().text() != "timestamp" && col.name().text() != "tag" {
					let mut data = ColumnData::with_capacity(col.data().get_type(), 1);
					data.push_value(col.data().get_value(row_idx));
					pre_col_vec.push(Column {
						name: col.name().clone(),
						data,
					});
				}
			}

			// Build post columns (updated values)
			let mut post_col_vec = Vec::with_capacity(1 + series_def.columns.len());
			post_col_vec.push(Column {
				name: Fragment::internal("timestamp"),
				data: ColumnData::int8(vec![timestamp]),
			});
			for col in post_cols.iter() {
				if col.name().text() != "timestamp" && col.name().text() != "tag" {
					let mut data = ColumnData::with_capacity(col.data().get_type(), 1);
					data.push_value(col.data().get_value(row_idx));
					post_col_vec.push(Column {
						name: col.name().clone(),
						data,
					});
				}
			}

			let pre = Columns {
				row_numbers: CowVec::new(vec![row_number]),
				columns: CowVec::new(pre_col_vec),
			};
			let post = Columns {
				row_numbers: CowVec::new(vec![row_number]),
				columns: CowVec::new(post_col_vec),
			};
			txn.track_flow_change(Change {
				origin: ChangeOrigin::Primitive(PrimitiveId::series(series_def.id)),
				version: CommitVersion(0),
				diffs: vec![Diff::Update {
					pre,
					post,
				}],
			});
		}
	}

	// Return summary
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("series", Value::Utf8(series_def.name)),
		("updated", Value::Uint8(updated_count)),
	]))
}
