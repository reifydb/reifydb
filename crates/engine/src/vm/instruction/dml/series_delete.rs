// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, key::EncodedKey},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{policy::PolicyTargetType, primitive::PrimitiveId},
		change::{Change, ChangeOrigin, Diff},
	},
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::{nodes::DeleteSeriesNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{Value, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use crate::{
	expression::{
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
	},
	vm::{
		instruction::dml::schema::get_or_create_series_schema, services::Services, stack::SymbolTable,
		volcano::scan::series::build_data_column,
	},
};

#[instrument(name = "mutate::series::delete", level = "trace", skip_all)]
pub(crate) fn delete_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteSeriesNode,
	params: Params,
	identity: IdentityId,
	symbol_table: &SymbolTable,
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

	// Get current metadata
	let Some(mut metadata) = services.catalog.find_series_metadata(txn, series_def.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series_def.tag.is_some();
	let mut deleted_count = 0u64;

	if let Some(input_plan) = plan.input {
		// Extract filter conditions and scan bounds from the plan.
		// The physical plan optimizer may push timestamp/tag predicates into the
		// SeriesScan node and remove the Filter node entirely. We need to recover
		// those bounds so the scan is properly limited.
		let (conditions, scan_start, scan_end, scan_tag) = match *input_plan {
			QueryPlan::Filter(filter) => {
				let (start, end, tag) = match *filter.input {
					QueryPlan::SeriesScan(scan) => {
						(scan.time_range_start, scan.time_range_end, scan.variant_tag)
					}
					_ => (None, None, None),
				};
				(filter.conditions, start, end, tag)
			}
			QueryPlan::SeriesScan(scan) => {
				(vec![], scan.time_range_start, scan.time_range_end, scan.variant_tag)
			}
			_ => (vec![], None, None, None),
		};

		// Use bounded scan when time range or tag is available from predicate pushdown
		let range = if scan_start.is_some() || scan_end.is_some() || scan_tag.is_some() {
			SeriesRowKeyRange::scan_range(series_def.id, scan_tag, scan_start, scan_end, None)
		} else {
			SeriesRowKeyRange::full_scan(series_def.id, None)
		};
		let mut keys = Vec::new();
		let mut encoded_values = Vec::new();
		let mut timestamps = Vec::new();
		let mut sequences = Vec::new();
		let mut tags = Vec::new();
		let mut data_rows: Vec<Vec<Value>> = Vec::new();

		// Get the schema for decoding series values
		let read_schema = get_or_create_series_schema(&services.catalog, &series_def, txn)?;

		{
			let mut stream = txn.range(range, 4096)?;
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = SeriesRowKey::decode(&entry.key) {
					keys.push(entry.key);
					timestamps.push(key.timestamp);
					sequences.push(key.sequence);
					if has_tag {
						tags.push(key.variant_tag.unwrap_or(0));
					}
					let mut values = Vec::with_capacity(series_def.columns.len());
					for (i, _col) in series_def.columns.iter().enumerate() {
						values.push(read_schema.get_value(&entry.values, i + 1));
					}
					data_rows.push(values);
					encoded_values.push(entry.values);
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
					identity: IdentityId::root(),
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

			// Enforce write policies only on rows that match the filter
			let matching_count = (0..row_count).filter(|&i| filter_mask.get(i)).count();
			if matching_count > 0 {
				let mut filtered_cols = Vec::new();
				for col in columns.iter() {
					let mut data = ColumnData::with_capacity(col.data().get_type(), matching_count);
					for i in 0..row_count {
						if filter_mask.get(i) {
							data.push_value(col.data().get_value(i));
						}
					}
					filtered_cols.push(Column {
						name: col.name().clone(),
						data,
					});
				}
				let filtered = Columns::new(filtered_cols);
				crate::policy::enforce_write_policies(
					services,
					txn,
					identity,
					namespace_name,
					series_name,
					"delete",
					&filtered,
					symbol_table,
					PolicyTargetType::Series,
				)?;
			}

			// Delete matching rows
			for (i, key) in keys.iter().enumerate() {
				if filter_mask.get(i) {
					// Track flow change for deleted row before removing
					let row_number = RowNumber::from(sequences[i]);
					let mut pre_col_vec = Vec::with_capacity(1 + series_def.columns.len());
					// Get timestamp from the columns Columns struct
					if let Some(ts_col) = columns.iter().find(|c| c.name().text() == "timestamp") {
						let mut data = ColumnData::with_capacity(ts_col.data().get_type(), 1);
						data.push_value(ts_col.data().get_value(i));
						pre_col_vec.push(Column {
							name: Fragment::internal("timestamp"),
							data,
						});
					}
					for col in columns.iter() {
						if col.name().text() != "timestamp" && col.name().text() != "tag" {
							let mut data =
								ColumnData::with_capacity(col.data().get_type(), 1);
							data.push_value(col.data().get_value(i));
							pre_col_vec.push(Column {
								name: col.name().clone(),
								data,
							});
						}
					}
					let pre = Columns {
						row_numbers: CowVec::new(vec![row_number]),
						columns: CowVec::new(pre_col_vec),
					};
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::series(series_def.id)),
						version: CommitVersion(0),
						diffs: vec![Diff::Remove {
							pre,
						}],
					});

					txn.unset(key, encoded_values[i].clone())?;
					deleted_count += 1;
				}
			}
		}
	} else {
		// Delete all rows - scan the full range and delete
		let range = SeriesRowKeyRange::full_scan(series_def.id, None);
		let mut entries_to_delete: Vec<(EncodedKey, EncodedValues)> = Vec::new();

		let mut stream = txn.range(range, 4096)?;
		while let Some(entry) = stream.next() {
			let entry = entry?;
			entries_to_delete.push((entry.key, entry.values));
		}
		drop(stream);

		let delete_all_schema = get_or_create_series_schema(&services.catalog, &series_def, txn)?;

		for (key, encoded_values) in entries_to_delete.iter() {
			// Track flow change before removing
			if let Some(decoded_key) = SeriesRowKey::decode(key) {
				let row_number = RowNumber::from(decoded_key.sequence);
				let data_values: Vec<Value> = series_def
					.columns
					.iter()
					.enumerate()
					.map(|(i, _)| delete_all_schema.get_value(encoded_values, i + 1))
					.collect();
				let mut pre_col_vec = Vec::with_capacity(1 + series_def.columns.len());
				pre_col_vec.push(Column {
					name: Fragment::internal("timestamp"),
					data: ColumnData::int8(vec![decoded_key.timestamp]),
				});
				for (col_idx, col_def) in series_def.columns.iter().enumerate() {
					let mut data = ColumnData::with_capacity(col_def.constraint.get_type(), 1);
					data.push_value(data_values.get(col_idx).cloned().unwrap_or(Value::none()));
					pre_col_vec.push(Column {
						name: Fragment::internal(&col_def.name),
						data,
					});
				}
				let pre = Columns {
					row_numbers: CowVec::new(vec![row_number]),
					columns: CowVec::new(pre_col_vec),
				};
				txn.track_flow_change(Change {
					origin: ChangeOrigin::Primitive(PrimitiveId::series(series_def.id)),
					version: CommitVersion(0),
					diffs: vec![Diff::Remove {
						pre,
					}],
				});
			}

			txn.unset(key, encoded_values.clone())?;
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
