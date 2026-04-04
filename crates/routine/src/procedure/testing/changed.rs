// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::{catalog::shape::ShapeId, change::Diff},
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::Error,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};

/// Identifies the primitive type category for a `testing::*::changed()` procedure.
pub struct TestingChanged {
	pub shape_type: &'static str,
}

impl TestingChanged {
	pub fn new(shape_type: &'static str) -> Self {
		Self {
			shape_type,
		}
	}
}

impl Procedure for TestingChanged {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let t = match tx {
			Transaction::Test(t) => t,
			_ => {
				return Err(internal_error!("testing::*::changed() requires a test transaction").into());
			}
		};

		let filter_arg = extract_optional_string_param(ctx.params);

		// Materialize view rows from pending source changes so that
		// changed() sees transactional view mutations.
		if self.shape_type == "views" {
			let _ = t.capture_testing_pre_commit();
		}

		// Read individual mutations from the accumulator
		let entries: Vec<_> =
			t.accumulator_entries_from().iter().map(|(id, diff)| (*id, diff.clone())).collect();

		let mut mutations: Vec<MutationEntry> = Vec::new();

		for (shape_id, diff) in &entries {
			let type_matches = matches!(
				(&shape_id, self.shape_type),
				(ShapeId::Table(_), "tables")
					| (ShapeId::View(_), "views") | (ShapeId::RingBuffer(_), "ringbuffers")
					| (ShapeId::Series(_), "series") | (ShapeId::Dictionary(_), "dictionaries")
			);
			if !type_matches {
				continue;
			}

			let catalog: &Catalog = ctx.catalog;
			let name = match resolve_shape_name(
				catalog,
				&mut Transaction::Test(Box::new(t.reborrow())),
				shape_id,
			) {
				Ok(n) => n,
				Err(_) => continue,
			};

			if let Some(filter) = filter_arg.as_deref()
				&& name != filter
			{
				continue;
			}

			mutations.push(MutationEntry {
				target: name,
				diff: diff.clone(),
			});
		}

		mutations.sort_by(|a, b| a.target.cmp(&b.target));
		Ok(build_output_columns(&mutations)?)
	}
}

fn extract_optional_string_param(params: &Params) -> Option<String> {
	match params {
		Params::Positional(args) if !args.is_empty() => match &args[0] {
			Value::Utf8(s) => Some(s.clone()),
			_ => None,
		},
		_ => None,
	}
}

struct MutationEntry {
	target: String,
	diff: Diff,
}

fn resolve_shape_name(catalog: &Catalog, txn: &mut Transaction<'_>, id: &ShapeId) -> Result<String, Error> {
	match id {
		ShapeId::Table(table_id) => {
			let table = catalog
				.find_table(txn, *table_id)?
				.ok_or_else(|| internal_error!("table not found for id {:?}", table_id))?;
			let ns = catalog
				.find_namespace(txn, table.namespace)?
				.ok_or_else(|| internal_error!("namespace not found"))?;
			Ok(format!("{}::{}", ns.name(), table.name))
		}
		ShapeId::View(view_id) => {
			let view = catalog
				.find_view(txn, *view_id)?
				.ok_or_else(|| internal_error!("view not found for id {:?}", view_id))?;
			let ns = catalog
				.find_namespace(txn, view.namespace())?
				.ok_or_else(|| internal_error!("namespace not found"))?;
			Ok(format!("{}::{}", ns.name(), view.name()))
		}
		ShapeId::RingBuffer(rb_id) => {
			let rb = catalog
				.find_ringbuffer(txn, *rb_id)?
				.ok_or_else(|| internal_error!("ringbuffer not found for id {:?}", rb_id))?;
			let ns = catalog
				.find_namespace(txn, rb.namespace)?
				.ok_or_else(|| internal_error!("namespace not found"))?;
			Ok(format!("{}::{}", ns.name(), rb.name))
		}
		ShapeId::Series(series_id) => {
			let series = catalog
				.find_series(txn, *series_id)?
				.ok_or_else(|| internal_error!("series not found for id {:?}", series_id))?;
			let ns = catalog
				.find_namespace(txn, series.namespace)?
				.ok_or_else(|| internal_error!("namespace not found"))?;
			Ok(format!("{}::{}", ns.name(), series.name))
		}
		ShapeId::Dictionary(dict_id) => {
			let dict = catalog
				.find_dictionary(txn, *dict_id)?
				.ok_or_else(|| internal_error!("dictionary not found for id {:?}", dict_id))?;
			let ns = catalog
				.find_namespace(txn, dict.namespace)?
				.ok_or_else(|| internal_error!("namespace not found"))?;
			Ok(format!("{}::{}", ns.name(), dict.name))
		}
		_ => Err(internal_error!("unsupported primitive type {:?}", id)),
	}
}

fn build_output_columns(entries: &[MutationEntry]) -> Result<Columns, Error> {
	if entries.is_empty() {
		return Ok(Columns::empty());
	}

	let mut op_data = ColumnData::utf8_with_capacity(entries.len());
	let mut target_data = ColumnData::utf8_with_capacity(entries.len());

	let mut field_names: Vec<String> = Vec::new();
	for entry in entries {
		match &entry.diff {
			Diff::Insert {
				post,
			}
			| Diff::Remove {
				pre: post,
			} => {
				for col in post.iter() {
					let name = col.name().text().to_string();
					if !field_names.contains(&name) {
						field_names.push(name);
					}
				}
			}
			Diff::Update {
				pre,
				post,
			} => {
				for col in pre.iter() {
					let name = col.name().text().to_string();
					if !field_names.contains(&name) {
						field_names.push(name);
					}
				}
				for col in post.iter() {
					let name = col.name().text().to_string();
					if !field_names.contains(&name) {
						field_names.push(name);
					}
				}
			}
		}
	}

	let mut old_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(entries.len()); field_names.len()];
	let mut new_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(entries.len()); field_names.len()];

	for entry in entries {
		let (op, old_cols, new_cols) = match &entry.diff {
			Diff::Insert {
				post,
			} => ("insert", &Columns::empty(), post),
			Diff::Update {
				pre,
				post,
			} => ("update", pre, post),
			Diff::Remove {
				pre,
			} => ("delete", pre, &Columns::empty()),
		};

		op_data.push(op);
		target_data.push(entry.target.as_str());

		for (i, field_name) in field_names.iter().enumerate() {
			let old_val =
				old_cols.column(field_name).map(|col| col.data().get_value(0)).unwrap_or(Value::none());
			old_columns[i].push(old_val);

			let new_val =
				new_cols.column(field_name).map(|col| col.data().get_value(0)).unwrap_or(Value::none());
			new_columns[i].push(new_val);
		}
	}

	let mut columns = vec![Column::new("op", op_data), Column::new("target", target_data)];

	for (i, name) in field_names.iter().enumerate() {
		let mut old_data = column_for_values(&old_columns[i]);
		for val in &old_columns[i] {
			old_data.push_value(val.clone());
		}
		columns.push(Column::new(format!("old_{}", name), old_data));

		let mut new_data = column_for_values(&new_columns[i]);
		for val in &new_columns[i] {
			new_data.push_value(val.clone());
		}
		columns.push(Column::new(format!("new_{}", name), new_data));
	}

	Ok(Columns::new(columns))
}

fn column_for_values(values: &[Value]) -> ColumnData {
	let first_type = values.iter().find_map(|v| {
		if matches!(v, Value::None { .. }) {
			None
		} else {
			Some(v.get_type())
		}
	});
	match first_type {
		Some(ty) => ColumnData::with_capacity(ty, values.len()),
		None => ColumnData::none_typed(Type::Boolean, 0),
	}
}
