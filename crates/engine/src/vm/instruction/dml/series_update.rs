// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{iter, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, key::EncodedKey},
	error::diagnostic::catalog::{namespace_not_found, series_not_found},
	interface::{
		catalog::{policy::PolicyTargetType, primitive::PrimitiveId},
		change::{Change, ChangeOrigin, Diff},
		resolved::{ResolvedNamespace, ResolvedPrimitive, ResolvedSeries},
	},
	key::{EncodableKey, series_row::SeriesRowKey},
	testing::TestingContext,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_rql::nodes::UpdateSeriesNode;
use reifydb_transaction::{interceptor::series::SeriesInterceptor, transaction::Transaction};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	return_error,
	util::cowvec::CowVec,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};
use tracing::instrument;

use super::{
	returning::evaluate_returning,
	series_key::{column_data_from_i64_keys, value_from_i64, value_to_i64},
};
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

#[instrument(name = "mutate::series::update", level = "trace", skip_all)]
pub(crate) fn update_series<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: UpdateSeriesNode,
	params: Params,
	symbol_table_ref: &SymbolTable,
	testing: &mut Option<TestingContext>,
) -> Result<Columns> {
	let namespace_name = plan.target.namespace().name();
	let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
		return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
	};

	let series_name = plan.target.name();
	let Some(series_def) = services.catalog.find_series_by_name(txn, namespace.id(), series_name)? else {
		let fragment = Fragment::internal(plan.target.name());
		return_error!(series_not_found(fragment, namespace_name, series_name));
	};

	let has_tag = series_def.tag.is_some();
	let key_type =
		series_def.columns.iter().find(|c| c.name == series_def.key.column()).map(|c| c.constraint.get_type());

	let namespace_ident = Fragment::internal(namespace.name());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());
	let series_ident = Fragment::internal(series_def.name.clone());
	let resolved_series = ResolvedSeries::new(series_ident, resolved_namespace, series_def.clone());
	let resolved_source = Some(ResolvedPrimitive::Series(resolved_series));

	let context = QueryContext {
		services: services.clone(),
		source: resolved_source,
		batch_size: 1024,
		params: params.clone(),
		stack: SymbolTable::new(),
		identity: IdentityId::root(),
		testing: None,
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

		PolicyEvaluator::new(services, symbol_table_ref).enforce_write_policies(
			txn,
			namespace_name,
			series_name,
			"update",
			&columns,
			PolicyTargetType::Series,
		)?;

		let row_numbers = &columns.row_numbers;

		let mut updates_to_apply: Vec<(EncodedKey, EncodedValues, usize)> = Vec::new();

		for row_idx in 0..row_count {
			let sequence = u64::from(row_numbers[row_idx]);

			let key_value = columns
				.iter()
				.find(|c| c.name().text() == series_def.key.column())
				.and_then(|c| value_to_i64(c.data().get_value(row_idx), &series_def.key))
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
				series: series_def.id,
				variant_tag,
				key: key_value,
				sequence,
			};
			let encoded_key = key.encode();

			let schema = get_or_create_series_schema(&services.catalog, &series_def, txn)?;
			let mut row = schema.allocate();

			let key_col_value = columns
				.iter()
				.find(|c| c.name().text() == series_def.key.column())
				.map(|c| c.data().get_value(row_idx))
				.unwrap_or(Value::Int8(0));
			schema.set_value(&mut row, 0, &key_col_value);

			for (i, col_def) in series_def.data_columns().enumerate() {
				let value = columns
					.iter()
					.find(|c| c.name().text() == col_def.name)
					.map(|c| c.data().get_value(row_idx))
					.unwrap_or(Value::none());
				schema.set_value(&mut row, i + 1, &value);
			}

			updates_to_apply.push((encoded_key, row, row_idx));
		}

		for (encoded_key, row, row_idx) in &updates_to_apply {
			let old_data = txn.get(encoded_key)?;
			let old_values = old_data.map(|v| v.values);

			let key_value = columns
				.iter()
				.find(|c| c.name().text() == series_def.key.column())
				.and_then(|c| value_to_i64(c.data().get_value(*row_idx), &series_def.key))
				.unwrap_or(0);

			let row_number = RowNumber::from(u64::from(row_numbers[*row_idx]));

			if let Some(ref old_vals) = old_values {
				let read_schema = get_or_create_series_schema(&services.catalog, &series_def, txn)?;
				let mut pre_col_vec = Vec::with_capacity(1 + series_def.columns.len());
				pre_col_vec.push(Column {
					name: Fragment::internal(series_def.key.column()),
					data: column_data_from_i64_keys(vec![key_value], &series_def, &series_def.key),
				});
				for (i, col_def) in series_def.data_columns().enumerate() {
					let val = read_schema.get_value(old_vals, i + 1);
					let mut data = ColumnData::with_capacity(col_def.constraint.get_type(), 1);
					data.push_value(val);
					pre_col_vec.push(Column {
						name: Fragment::internal(&col_def.name),
						data,
					});
				}

				let mut post_col_vec = Vec::with_capacity(1 + series_def.columns.len());
				post_col_vec.push(Column {
					name: Fragment::internal(series_def.key.column()),
					data: column_data_from_i64_keys(vec![key_value], &series_def, &series_def.key),
				});
				for col in columns.iter() {
					if col.name().text() != series_def.key.column() && col.name().text() != "tag" {
						let mut data = ColumnData::with_capacity(col.data().get_type(), 1);
						data.push_value(col.data().get_value(*row_idx));
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

				if let Some(log) = testing.as_mut() {
					let old = Columns::single_row(
						iter::once((
							series_def.key.column(),
							value_from_i64(key_value, key_type.as_ref(), &series_def.key),
						))
						.chain(series_def.data_columns().enumerate().map(|(i, col)| {
							(col.name.as_str(), read_schema.get_value(old_vals, i + 1))
						})),
					);
					let new = Columns::single_row(
						iter::once((
							series_def.key.column(),
							value_from_i64(key_value, key_type.as_ref(), &series_def.key),
						))
						.chain(columns
							.iter()
							.filter(|c| {
								c.name().text() != series_def.key.column()
									&& c.name().text() != "tag"
							})
							.map(|c| (c.name().text(), c.data().get_value(*row_idx)))),
					);
					let key = format!("series::{}::{}", namespace.name(), series_def.name);
					log.record_update(key, old, new);
				}
			}

			let row = SeriesInterceptor::pre_update(txn, &series_def, row.clone())?;
			txn.set(encoded_key, row.clone())?;
			SeriesInterceptor::post_update(txn, &series_def, &row, &row)?;
			updated_count += 1;
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
		return evaluate_returning(services, symbol_table_ref, returning_exprs, cols);
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name().to_string())),
		("series", Value::Utf8(series_def.name)),
		("updated", Value::Uint8(updated_count)),
	]))
}
