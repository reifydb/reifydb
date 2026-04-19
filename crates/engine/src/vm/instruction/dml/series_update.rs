// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{
			policy::{DataOp, PolicyTargetType},
			shape::ShapeId,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedSeries, ResolvedShape},
	},
	key::{EncodableKey, series_row::SeriesRowKey},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::nodes::UpdateSeriesNode;
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::returning::evaluate_returning;
use crate::{
	Result,
	policy::PolicyEvaluator,
	vm::{
		instruction::dml::shape::get_or_create_series_shape,
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

#[instrument(name = "mutate::series::update", level = "trace", skip_all)]
pub(crate) fn update_series(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateSeriesNode,
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

	let has_tag = series.tag.is_some();

	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let series_ident = Fragment::internal(series.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series.clone());
	let resolved_source = Some(ResolvedShape::Series(resolved_series));

	let context = QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		symbols: symbols.clone(),
		identity: IdentityId::root(),
	};

	let mut input_node = compile(*plan.input, txn, Arc::new(context.clone()));
	input_node.initialize(txn, &context)?;

	let mut updated_count = 0u64;
	let mut returning_columns: Option<Columns> = None;

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
			DataOp::Update,
			&columns,
			PolicyTargetType::Series,
		)?;

		let row_numbers = &columns.row_numbers;

		let mut updates_to_apply: Vec<(EncodedKey, EncodedRow, usize)> = Vec::new();

		for row_idx in 0..row_count {
			let sequence = u64::from(row_numbers[row_idx]);

			let key_value = columns
				.iter()
				.find(|c| c.name().text() == series.key.column())
				.and_then(|c| series.key_to_u64(c.data().get_value(row_idx)))
				.unwrap_or(0);

			let variant_tag = if has_tag {
				columns.iter().find(|c| c.name().text() == "tag").and_then(|c| {
					match c.data().get_value(row_idx) {
						Value::Uint1(v) => Some(v),
						_ => None,
					}
				})
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

			let shape = get_or_create_series_shape(&services.catalog, &series, txn)?;
			let mut row = shape.allocate();

			let key_col_value = columns
				.iter()
				.find(|c| c.name().text() == series.key.column())
				.map(|c| c.data().get_value(row_idx))
				.unwrap_or(Value::Int8(0));
			shape.set_value(&mut row, 0, &key_col_value);

			for (i, col_def) in series.data_columns().enumerate() {
				let value = columns
					.iter()
					.find(|c| c.name().text() == col_def.name)
					.map(|c| c.data().get_value(row_idx))
					.unwrap_or(Value::none());
				shape.set_value(&mut row, i + 1, &value);
			}

			updates_to_apply.push((encoded_key, row, row_idx));
		}

		for (encoded_key, mut row, row_idx) in updates_to_apply {
			let pre_data = txn.get(&encoded_key)?;
			let pre_values = pre_data.map(|v| v.row);

			let old_created_at = pre_values.as_ref().expect("row must exist for update").created_at_nanos();
			row.set_timestamps(old_created_at, services.runtime_context.clock.now_nanos());

			let key_value = columns
				.iter()
				.find(|c| c.name().text() == series.key.column())
				.and_then(|c| series.key_to_u64(c.data().get_value(row_idx)))
				.unwrap_or(0);

			let row_number = RowNumber::from(u64::from(row_numbers[row_idx]));

			if let Some(ref pre_vals) = pre_values {
				let read_shape = get_or_create_series_shape(&services.catalog, &series, txn)?;
				let mut pre_col_vec = Vec::with_capacity(1 + series.columns.len());
				pre_col_vec.push(Column {
					name: Fragment::internal(series.key.column()),
					data: series.key_column_data(vec![key_value]),
				});
				for (i, col_def) in series.data_columns().enumerate() {
					let val = read_shape.get_value(pre_vals, i + 1);
					let mut data = ColumnData::with_capacity(col_def.constraint.get_type(), 1);
					data.push_value(val);
					pre_col_vec.push(Column {
						name: Fragment::internal(&col_def.name),
						data,
					});
				}

				let mut post_col_vec = Vec::with_capacity(1 + series.columns.len());
				post_col_vec.push(Column {
					name: Fragment::internal(series.key.column()),
					data: series.key_column_data(vec![key_value]),
				});
				for col in columns.iter() {
					if col.name().text() != series.key.column() && col.name().text() != "tag" {
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
					created_at: CowVec::new(vec![DateTime::from_nanos(
						pre_vals.created_at_nanos(),
					)]),
					updated_at: CowVec::new(vec![DateTime::from_nanos(
						pre_vals.updated_at_nanos(),
					)]),
					columns: CowVec::new(pre_col_vec),
				};
				let post = Columns {
					row_numbers: CowVec::new(vec![row_number]),
					created_at: CowVec::new(vec![DateTime::from_nanos(row.created_at_nanos())]),
					updated_at: CowVec::new(vec![DateTime::from_nanos(row.updated_at_nanos())]),
					columns: CowVec::new(post_col_vec),
				};
				txn.track_flow_change(Change {
					origin: ChangeOrigin::Shape(ShapeId::series(series.id)),
					version: CommitVersion(0),
					diffs: vec![Diff::Update {
						pre,
						post,
					}],
					changed_at: DateTime::default(),
				});
			}

			let pre_for_interceptor = pre_values.clone().unwrap_or_else(|| row.clone());
			let row = SeriesRowInterceptor::pre_update(txn, &series, row.clone())?;
			txn.set(&encoded_key, row.clone())?;
			SeriesRowInterceptor::post_update(txn, &series, &row, &pre_for_interceptor)?;
			updated_count += 1;
		}

		if plan.returning.is_some() {
			returning_columns = Some(match returning_columns {
				Some(existing) => {
					let mut cols = Vec::new();
					for (i, col) in columns.iter().enumerate() {
						if let Some(existing_col) = existing.get(i) {
							let mut data = ColumnData::with_capacity(
								col.data().get_type(),
								existing_col.data().len() + col.data().len(),
							);
							for j in 0..existing_col.data().len() {
								data.push_value(existing_col.data().get_value(j));
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

	if let Some(returning_exprs) = &plan.returning {
		let cols = returning_columns.unwrap_or_else(Columns::empty);
		return evaluate_returning(services, symbols, returning_exprs, cols);
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name().to_string())),
		("series", Value::Utf8(series.name)),
		("updated", Value::Uint8(updated_count)),
	]))
}
