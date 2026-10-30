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
		Self::wrap_series(input, columns, None)
	}

	pub(crate) fn wrap_series(
		input: Box<dyn QueryNode>,
		columns: &[Column],
		tag: Option<SumTypeId>,
	) -> Box<dyn QueryNode> {
		let mut sumtypes: HashMap<String, SumTypeId> = columns
			.iter()
			.filter_map(|column| match column.constraint.constraint() {
				Some(Constraint::SumType(id)) => Some((column.name.clone(), *id)),
				_ => None,
			})
			.collect();
		sumtypes.extend(tag.map(|id| ("tag".to_string(), id)));
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
