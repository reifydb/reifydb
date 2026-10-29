// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::catalog::column::Column,
	value::{batch::mark_tag_columns, column::headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{constraint::Constraint, sumtype::SumTypeId};

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
		mark_tag_columns(batch, &self.sumtypes)
	}
}

impl QueryNode for MarkSumtypeTagsNode {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		let mut unit = HashMap::new();
		for (name, id) in self.sumtypes.drain() {
			if ctx.services
				.catalog
				.get_sumtype(rx, id)?
				.variants
				.iter()
				.all(|variant| variant.fields.is_empty())
			{
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
