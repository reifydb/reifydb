// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	testing::{MutationRecord, TestingChanged, TestingContext},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{Result, value::Value};

use super::{column_for_values, extract_optional_string_arg, testing_context_from_ioc};
use crate::{GeneratorContext, GeneratorFunction, error::GeneratorFunctionResult};

impl GeneratorFunction for TestingChanged {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> GeneratorFunctionResult<Columns> {
		let testing = testing_context_from_ioc(ctx.ioc)?;
		let guard = testing.lock();
		let filter_arg = extract_optional_string_arg(&ctx.params);
		Ok(build_mutations(&guard, filter_arg.as_deref(), self.primitive_type)?)
	}
}

fn build_mutations(ctx: &TestingContext, filter_name: Option<&str>, primitive_type: &str) -> Result<Columns> {
	let entries: Vec<(&str, &MutationRecord)> = if let Some(s) = filter_name {
		let full_key = format!("{}::{}", primitive_type, s);
		match ctx.mutations.get(&full_key) {
			Some(records) => records.iter().map(|r| (s, r)).collect(),
			None => return Ok(Columns::empty()),
		}
	} else {
		let prefix = format!("{}::", primitive_type);
		let mut all: Vec<(&str, &MutationRecord)> = Vec::new();
		for (key, records) in &ctx.mutations {
			if let Some(target) = key.strip_prefix(&prefix) {
				for rec in records {
					all.push((target, rec));
				}
			}
		}
		all.sort_by_key(|(target, _)| target.to_string());
		all
	};

	if entries.is_empty() {
		return Ok(Columns::empty());
	}

	let mut op_data = ColumnData::utf8_with_capacity(entries.len());
	let mut target_data = ColumnData::utf8_with_capacity(entries.len());

	let mut field_names: Vec<String> = Vec::new();
	for (_, rec) in &entries {
		for col in rec.old.iter() {
			let name = col.name().text().to_string();
			if !field_names.contains(&name) {
				field_names.push(name);
			}
		}
		for col in rec.new.iter() {
			let name = col.name().text().to_string();
			if !field_names.contains(&name) {
				field_names.push(name);
			}
		}
	}

	let mut old_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(entries.len()); field_names.len()];
	let mut new_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(entries.len()); field_names.len()];

	for (target, rec) in &entries {
		op_data.push(rec.op.as_str());
		target_data.push(*target);

		for (i, field_name) in field_names.iter().enumerate() {
			let old_val =
				rec.old.column(field_name).map(|col| col.data().get_value(0)).unwrap_or(Value::none());
			old_columns[i].push(old_val);

			let new_val =
				rec.new.column(field_name).map(|col| col.data().get_value(0)).unwrap_or(Value::none());
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
