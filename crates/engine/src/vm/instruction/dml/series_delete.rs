// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{policy::PolicyTargetType, schema::SchemaId},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSchema, ResolvedSeries},
	},
	key::{
		EncodableKey,
		series_row::{SeriesRowKey, SeriesRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::nodes::DeleteSeriesNode;
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::cowvec::CowVec,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::returning::{decode_rows_to_columns, evaluate_returning};
use crate::{
	Result,
	policy::PolicyEvaluator,
	vm::{
		instruction::dml::schema::get_or_create_series_schema,
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

#[instrument(name = "mutate::series::delete", level = "trace", skip_all)]
pub(crate) fn delete_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteSeriesNode,
	params: Params,
	symbols: &SymbolTable,
) -> Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let series_name = plan.target.name();
	let Some(series) = services.catalog.find_series_by_name(txn, namespace.id(), series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let Some(mut metadata) = services.catalog.find_series_metadata(txn, series.id)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series.tag.is_some();
	let mut deleted_count = 0u64;
	let mut returning_columns: Option<Columns> = None;

	if let Some(input_plan) = plan.input {
		let namespace_ident = Fragment::internal(namespace.name());
		let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
		let series_ident = Fragment::internal(series.name.clone());
		let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series.clone());
		let resolved_source = Some(ResolvedSchema::Series(resolved_series));

		let context = QueryContext {
			services: services.clone(),
			source: resolved_source,
			batch_size: 1024,
			params: params.clone(),
			symbols: symbols.clone(),
			identity: IdentityId::root(),
		};

		let mut input_node = compile(*input_plan, txn, Arc::new(context.clone()));
		input_node.initialize(txn, &context)?;

		let mut mutable_context = context.clone();
		while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
			let row_count = columns.row_count();
			if row_count == 0 {
				continue;
			}

			PolicyEvaluator::new(services, symbols).enforce_write_policies(
				txn,
				namespace_name,
				series_name,
				"delete",
				&columns,
				PolicyTargetType::Series,
			)?;

			let row_numbers = &columns.row_numbers;

			for row_idx in 0..row_count {
				let sequence = u64::from(row_numbers[row_idx]);

				let key_value = columns
					.iter()
					.find(|c| c.name().text() == series.key.column())
					.and_then(|c| series.key_to_u64(c.data().get_value(row_idx)))
					.unwrap_or(0);

				let variant_tag = if has_tag {
					columns.iter()
						.find(|c| c.name().text() == "tag")
						.map(|c| match c.data().get_value(row_idx) {
							Value::Uint1(v) => Some(v),
							_ => None,
						})
						.flatten()
				} else {
					None
				};

				let key = SeriesRowKey {
					series: series.id,
					variant_tag,
					key: key_value,
					sequence,
				};
				let encoded_key = key.encode();

				let Some(old_entry) = txn.get(&encoded_key)? else {
					continue;
				};
				let encoded_row = old_entry.row;

				let row_number = RowNumber::from(sequence);
				let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
				pre_col_vec.push(Column {
					name: Fragment::internal(series.key.column()),
					data: series.key_column_data(vec![key_value]),
				});
				for col in columns.iter() {
					if col.name().text() != series.key.column() && col.name().text() != "tag" {
						let mut data = ColumnData::with_capacity(col.data().get_type(), 1);
						data.push_value(col.data().get_value(row_idx));
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
					origin: ChangeOrigin::Schema(SchemaId::series(series.id)),
					version: CommitVersion(0),
					diffs: vec![Diff::Remove {
						pre,
					}],
				});

				SeriesRowInterceptor::pre_delete(txn, &series)?;
				txn.unset(&encoded_key, encoded_row.clone())?;
				SeriesRowInterceptor::post_delete(txn, &series, &encoded_row)?;
				deleted_count += 1;
			}

			if plan.returning.is_some() {
				returning_columns = Some(match returning_columns {
					Some(existing) => {
						let mut cols = Vec::new();
						for (i, col) in columns.iter().enumerate() {
							if let Some(existing_col) = existing.iter().nth(i) {
								let mut data = ColumnData::with_capacity(
									col.data().get_type(),
									existing_col.data().len() + col.data().len(),
								);
								for j in 0..existing_col.data().len() {
									data.push_value(
										existing_col.data().get_value(j),
									);
								}
								for j in 0..col.data().len() {
									data.push_value(col.data().get_value(j));
								}
								cols.push(Column {
									name: col.name().clone(),
									data,
								});
							}
						}
						Columns::new(cols)
					}
					None => columns,
				});
			}
		}

		if plan.returning.is_some() && returning_columns.is_none() {
			returning_columns = Some(Columns::empty());
		}
	} else {
		// Delete all rows - scan the full range and delete
		let range = SeriesRowKeyRange::full_scan(series.id, None);
		let mut entries_to_delete: Vec<(EncodedKey, EncodedRow)> = Vec::new();

		let mut stream = txn.range(range, 4096)?;
		while let Some(entry) = stream.next() {
			let entry = entry?;
			entries_to_delete.push((entry.key, entry.row));
		}
		drop(stream);

		let delete_all_schema = get_or_create_series_schema(&services.catalog, &series, txn)?;

		for (key, encoded_row) in entries_to_delete.iter() {
			if let Some(decoded_key) = SeriesRowKey::decode(key) {
				let row_number = RowNumber::from(decoded_key.sequence);
				let data_values: Vec<Value> = series
					.data_columns()
					.enumerate()
					.map(|(i, _)| delete_all_schema.get_value(encoded_row, i + 1))
					.collect();
				let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
				pre_col_vec.push(Column {
					name: Fragment::internal(series.key.column()),
					data: series.key_column_data(vec![decoded_key.key]),
				});
				for (col_idx, col_def) in series.data_columns().enumerate() {
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
					origin: ChangeOrigin::Schema(SchemaId::series(series.id)),
					version: CommitVersion(0),
					diffs: vec![Diff::Remove {
						pre,
					}],
				});
			}

			SeriesRowInterceptor::pre_delete(txn, &series)?;
			txn.unset(key, encoded_row.clone())?;
			SeriesRowInterceptor::post_delete(txn, &series, encoded_row)?;
			deleted_count += 1;
		}

		if plan.returning.is_some() {
			let mut returned_rows: Vec<(RowNumber, EncodedRow)> = Vec::new();
			for (key, encoded) in entries_to_delete.iter() {
				if let Some(decoded_key) = SeriesRowKey::decode(key) {
					returned_rows.push((RowNumber::from(decoded_key.sequence), encoded.clone()));
				}
			}
			let columns = decode_rows_to_columns(&delete_all_schema, &returned_rows);
			returning_columns = Some(columns);
		}
	}

	metadata.row_count = metadata.row_count.saturating_sub(deleted_count);
	if metadata.row_count == 0 {
		metadata.oldest_key = 0;
		metadata.newest_key = 0;
	}

	services.catalog.update_series_metadata_txn(txn, metadata)?;

	if let Some(returning_exprs) = &plan.returning {
		let cols = returning_columns.unwrap_or_else(Columns::empty);
		return evaluate_returning(services, symbols, returning_exprs, cols);
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name().to_string())),
		("series", Value::Utf8(series.name)),
		("deleted", Value::Uint8(deleted_count)),
	]))
}
