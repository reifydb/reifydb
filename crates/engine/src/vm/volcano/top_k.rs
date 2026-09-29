// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{
	error::diagnostic::query,
	sort::{SortDirection, SortKey},
	value::{
		batch::{concat, head, heap_size, take_rows},
		column::headers::ColumnHeaders,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error, reifydb_assertions,
	value::{
		column_view::ColumnView,
		system_columns::{is_system_field, resolve_column},
	},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode, charge_query_memory_bytes, ensure_sort_key_orderable},
		rank::rank_rows,
	},
};

pub(crate) struct TopKNode {
	input: Box<dyn QueryNode>,
	by: Vec<SortKey>,
	limit: usize,
	initialized: Option<()>,
	exhausted: bool,
}

impl TopKNode {
	pub(crate) fn new(input: Box<dyn QueryNode>, by: Vec<SortKey>, limit: usize) -> Self {
		Self {
			input,
			by,
			limit,
			initialized: None,
			exhausted: false,
		}
	}
}

impl QueryNode for TopKNode {
	#[instrument(level = "trace", skip_all, name = "volcano::top_k::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.initialized.is_some(), "TopKNode::next() called before initialize()");
		}

		if self.limit == 0 {
			if self.exhausted {
				return Ok(None);
			}
			self.exhausted = true;
			return match self.input.next(rx, ctx)? {
				Some(batch) => Ok(Some(head(&batch, 0))),
				None => Ok(None),
			};
		}

		let columns_opt = self.collect_input(rx, ctx)?;

		let columns = match columns_opt {
			Some(f) => f,
			None => return Ok(None),
		};

		let row_count = columns.num_rows();

		let key_cols: Vec<_> =
			self.by.iter().map(|key| Self::resolve_key(&columns, key)).collect::<Result<Vec<_>>>()?;

		let indices = rank_rows(&key_cols, row_count, Some(self.limit))?;
		Ok(Some(Self::permute(&columns, &indices)?))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl TopKNode {
	fn resolve_key<'b>(columns: &'b RecordBatch, key: &SortKey) -> Result<(ColumnView<'b>, SortDirection)> {
		let index = resolve_column(columns, key.column.fragment())
			.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
		let view = ColumnView::try_from((columns.column(index), columns.schema_ref().field(index)))?;
		if !is_system_field(view.field) {
			ensure_sort_key_orderable(key, &view)?;
		}
		Ok((view, key.direction.clone()))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::collect")]
	fn collect_input<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut QueryContext,
	) -> Result<Option<RecordBatch>> {
		let mut batches = Vec::new();
		let mut charged = 0usize;
		let mut total = 0usize;

		while let Some(batch) = self.input.next(rx, ctx)? {
			total += heap_size(&batch)?;
			charge_query_memory_bytes(&ctx.memory, &mut charged, total)?;
			batches.push(batch);
		}

		if batches.is_empty() {
			return Ok(None);
		}
		Ok(Some(concat(&batches)?))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::permute")]
	fn permute(columns: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
		take_rows(columns, indices)
	}
}
