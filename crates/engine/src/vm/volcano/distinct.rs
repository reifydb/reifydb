// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_row::Row;
use arrow_schema::FieldRef;
use reifydb_core::{
	error::diagnostic::operation,
	interface::resolved::ResolvedColumn,
	internal_error,
	value::{
		batch::{concat, heap_size, take_rows},
		column::headers::ColumnHeaders,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{column_view::ColumnView, system_columns::user_columns},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::{
		key_rows::{key_rows, key_types},
		query::{QueryContext, QueryNode, charge_query_memory_bytes},
	},
};

fn ensure_distinct_keyable(name: &Fragment, data: &ColumnView<'_>) -> Result<()> {
	let ty = data.get_type();
	if !ty.is_scalar() {
		return Err(error!(operation::distinct_key_unkeyable(name.clone(), ty)));
	}
	Ok(())
}

pub(crate) struct DistinctNode {
	input: Box<dyn QueryNode>,
	columns: Vec<ResolvedColumn>,
	headers: Option<ColumnHeaders>,
}

impl DistinctNode {
	pub fn new(input: Box<dyn QueryNode>, columns: Vec<ResolvedColumn>) -> Self {
		Self {
			input,
			columns,
			headers: None,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::collect")]
	fn collect_input<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
	) -> Result<Option<RecordBatch>> {
		let mut batches = Vec::new();
		let mut charged = 0usize;
		let mut total = 0usize;

		while let Some(cols) = self.input.next(rx, ctx)? {
			total += heap_size(&cols)?;
			charge_query_memory_bytes(&ctx.memory, &mut charged, total)?;
			batches.push(cols);
		}

		if batches.is_empty() {
			return Ok(None);
		}
		Ok(Some(concat(&batches)?))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::dedupe")]
	fn dedupe(&self, all_columns: &RecordBatch) -> Result<Vec<usize>> {
		let schema = all_columns.schema_ref();
		let mut key_columns: Vec<(&FieldRef, &ArrayRef)> = Vec::new();
		if self.columns.is_empty() {
			for (field, array) in user_columns(all_columns) {
				ensure_distinct_keyable(
					&Fragment::internal(field.name()),
					&ColumnView::try_from((array, field.as_ref()))?,
				)?;
				key_columns.push((field, array));
			}
		} else {
			for column in &self.columns {
				if let Some(index) =
					schema.fields().iter().position(|field| field.name() == column.name())
				{
					let (field, array) = (&schema.fields()[index], all_columns.column(index));
					let view = ColumnView::try_from((array, field.as_ref()))?;
					ensure_distinct_keyable(column.identifier(), &view)?;
					key_columns.push((field, array));
				}
			}
		}

		let row_count = all_columns.num_rows();
		if key_columns.is_empty() {
			return Ok((0..row_count).take(1).collect());
		}

		let (converter, arrays) = key_rows(&key_columns, &key_types(&key_columns)?)?;
		let rows = converter
			.convert_columns(&arrays)
			.map_err(|e| internal_error!("Failed to build distinct keys: {}", e))?;

		let mut seen = HashSet::<Row>::with_capacity(row_count);
		let mut kept_indices = Vec::new();
		for row_idx in 0..row_count {
			if seen.insert(rows.row(row_idx)) {
				kept_indices.push(row_idx);
			}
		}

		Ok(kept_indices)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::extract")]
	fn extract(all_columns: &RecordBatch, kept_indices: &[usize]) -> Result<RecordBatch> {
		take_rows(all_columns, kept_indices)
	}
}

impl QueryNode for DistinctNode {
	#[instrument(level = "trace", skip_all, name = "volcano::distinct::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::distinct::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		if self.headers.is_some() {
			return Ok(None);
		}

		let all_columns = self.collect_input(rx, ctx)?;

		let all_columns = match all_columns {
			Some(cols) => cols,
			None => {
				self.headers = Some(ColumnHeaders::empty());
				return Ok(None);
			}
		};

		let kept_indices = self.dedupe(&all_columns)?;

		let result = if kept_indices.is_empty() {
			all_columns
		} else {
			Self::extract(&all_columns, &kept_indices)?
		};
		self.headers = Some(ColumnHeaders::from_batch(&result));

		Ok(Some(result))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone().or(self.input.headers())
	}
}
