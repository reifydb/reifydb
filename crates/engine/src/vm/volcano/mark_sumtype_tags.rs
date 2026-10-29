// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::catalog::column::Column,
	internal_error,
	value::{batch::batch_with, column::headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{
	constraint::Constraint,
	sumtype::SumTypeId,
	value_type::{
		ValueType,
		field::{from_field, to_field},
	},
};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct MarkSumtypeTagsNode {
	input: Box<dyn QueryNode>,
	sumtypes: HashMap<String, SumTypeId>,
}

impl MarkSumtypeTagsNode {
	pub(crate) fn wrap(input: Box<dyn QueryNode>, columns: &[Column]) -> Box<dyn QueryNode> {
		let sumtypes: HashMap<String, SumTypeId> = columns
			.iter()
			.filter_map(|column| match column.constraint.constraint() {
				Some(Constraint::SumType(id)) => Some((column.name.clone(), *id)),
				_ => None,
			})
			.collect();
		if sumtypes.is_empty() {
			return input;
		}
		Box::new(Self {
			input,
			sumtypes,
		})
	}

	fn mark(&self, batch: RecordBatch) -> Result<RecordBatch> {
		let schema = batch.schema();
		let mut columns = Vec::with_capacity(batch.num_columns());
		for (field, array) in schema.fields().iter().zip(batch.columns()) {
			let field = match self.sumtypes.get(field.name()) {
				Some(id) => mark_field(field, *id)?,
				None => field.clone(),
			};
			columns.push((field, array.clone()));
		}
		batch_with(&schema, columns, batch.num_rows())
	}
}

pub(crate) fn mark_field(field: &FieldRef, id: SumTypeId) -> Result<FieldRef> {
	let mut field_type = from_field(field)?;
	if field_type.value_type.as_ref().map(ValueType::inner_type) != Some(&ValueType::Uint1) {
		return Err(internal_error!("sumtype tag column {} is not uint1", field.name()));
	}
	field_type.sumtype = Some(id);
	Ok(Arc::new(to_field(field.name(), &field_type).with_nullable(field.is_nullable())))
}

impl QueryNode for MarkSumtypeTagsNode {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let mut unit = HashMap::new();
		for (name, id) in self.sumtypes.drain() {
			if ctx.services.catalog.get_sumtype(rx, id)?.variants.iter().all(|variant| variant.fields.is_empty()) {
				unit.insert(name, id);
			}
		}
		self.sumtypes = unit;
		self.input.initialize(rx, ctx)
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		match self.input.next(rx, ctx)? {
			Some(batch) => Ok(Some(self.mark(batch)?)),
			None => Ok(None),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
