// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{
	error::diagnostic::query,
	sort::SortKey,
	value::{
		batch::{concat, heap_size, take_rows},
		column::headers::ColumnHeaders,
	},
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error,
	error::Error,
	reifydb_assertions,
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

pub(crate) struct SortNode {
	input: Box<dyn QueryNode>,
	by: Vec<SortKey>,
	initialized: Option<()>,
}

impl SortNode {
	pub(crate) fn new(input: Box<dyn QueryNode>, by: Vec<SortKey>) -> Self {
		Self {
			input,
			by,
			initialized: None,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::sort::collect")]
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
}

impl QueryNode for SortNode {
	#[instrument(level = "trace", skip_all, name = "volcano::sort::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::sort::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.initialized.is_some(), "SortNode::next() called before initialize()");
		}

		let columns_opt = self.collect_input(rx, ctx)?;

		let columns = match columns_opt {
			Some(f) => f,
			None => return Ok(None),
		};

		let transform_ctx = TransformContext {
			routines: &ctx.services.routines,
			runtime_context: &ctx.services.runtime_context,
			params: &ctx.params,
		};
		Ok(Some(self.apply(&transform_ctx, columns)?))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl Transform for SortNode {
	fn apply(&self, _ctx: &TransformContext, input: RecordBatch) -> Result<RecordBatch> {
		let key_refs = self
			.by
			.iter()
			.map(|key| {
				let index = resolve_column(&input, key.column.fragment())
					.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
				let view =
					ColumnView::try_from((input.column(index), input.schema_ref().field(index)))?;
				if !is_system_field(view.field) {
					ensure_sort_key_orderable(key, &view)?;
				}
				Ok::<_, Error>((view, key.direction.clone()))
			})
			.collect::<Result<Vec<_>>>()?;

		let indices = rank_rows(&key_refs, input.num_rows(), None)?;
		Self::permute(&input, &indices)
	}
}

impl SortNode {
	#[instrument(level = "trace", skip_all, name = "volcano::sort::permute")]
	fn permute(input: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
		take_rows(input, indices)
	}
}
