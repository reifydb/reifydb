// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::query,
	sort::{SortDirection, SortKey},
	value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error, reifydb_assertions};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode, charge_query_memory, ensure_sort_key_orderable},
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
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.initialized.is_some(), "TopKNode::next() called before initialize()");
		}

		if self.limit == 0 {
			if self.exhausted {
				return Ok(None);
			}
			self.exhausted = true;
			return match self.input.next(rx, ctx)? {
				Some(mut columns) => {
					columns.take(0)?;
					Ok(Some(columns))
				}
				None => Ok(None),
			};
		}

		let columns_opt = self.collect_input(rx, ctx)?;

		let mut columns = match columns_opt {
			Some(f) => f,
			None => return Ok(None),
		};

		let row_count = columns.row_count();

		let key_cols: Vec<_> =
			self.by.iter().map(|key| Self::resolve_key(&columns, key)).collect::<Result<Vec<_>>>()?;

		let indices = rank_rows(&key_cols, row_count, Some(self.limit))?;
		Self::permute(&mut columns, &indices);

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl TopKNode {
	fn resolve_key(columns: &Columns, key: &SortKey) -> Result<(ColumnBuffer, SortDirection)> {
		let name = key.column.fragment();
		if let Some(data) = columns.system_column(name) {
			return Ok((data, key.direction.clone()));
		}
		let col = columns
			.iter()
			.find(|c| c.name() == name)
			.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
		ensure_sort_key_orderable(key, col.data())?;
		Ok((col.data().clone(), key.direction.clone()))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::collect")]
	fn collect_input<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let mut columns_opt: Option<Columns> = None;
		let mut charged = 0usize;

		while let Some(columns) = self.input.next(rx, ctx)? {
			if let Some(existing_columns) = &mut columns_opt {
				existing_columns.system.extend(&columns.system)?;
				for (i, col) in columns.columns.iter().enumerate() {
					existing_columns[i].extend(col.clone())?;
				}
			} else {
				columns_opt = Some(columns);
			}
			if let Some(acc) = &columns_opt {
				charge_query_memory(&ctx.memory, &mut charged, acc)?;
			}
		}

		Ok(columns_opt)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::top_k::permute")]
	fn permute(columns: &mut Columns, indices: &[usize]) {
		columns.system.permute_in_place(indices);

		let cols = &mut columns.columns;
		for col in cols.iter_mut() {
			col.reorder(indices);
		}
	}
}
