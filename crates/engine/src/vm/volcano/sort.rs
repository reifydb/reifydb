// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp::Ordering::Equal;

use reifydb_core::{
	error::diagnostic::query,
	sort::{
		SortDirection::{Asc, Desc},
		SortKey,
	},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error, error::Error, reifydb_assertions};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode, charge_query_memory},
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
}

impl QueryNode for SortNode {
	#[instrument(level = "trace", skip_all, name = "volcano::sort::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::sort::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.initialized.is_some(), "SortNode::next() called before initialize()");
		}

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
	fn apply(&self, _ctx: &TransformContext, mut columns: Columns) -> Result<Columns> {
		let key_refs =
			self.by.iter()
				.map(|key| {
					let name = key.column.fragment();

					if let Some(data) = columns.system_column(name) {
						return Ok::<_, Error>((data, key.direction.clone()));
					}

					let col = columns
						.iter()
						.find(|c| c.name() == name)
						.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
					Ok((col.data().clone(), key.direction.clone()))
				})
				.collect::<Result<Vec<_>>>()?;

		let row_count = columns.row_count();
		let mut indices: Vec<usize> = (0..row_count).collect();

		indices.sort_unstable_by(|&l, &r| {
			for (col, dir) in &key_refs {
				let vl = col.get_value(l);
				let vr = col.get_value(r);
				let ord = vl.partial_cmp(&vr).unwrap_or(Equal);
				let ord = match dir {
					Asc => ord,
					Desc => ord.reverse(),
				};
				if ord != Equal {
					return ord;
				}
			}
			Equal
		});

		columns.system.permute_in_place(&indices);

		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.reorder(&indices);
		}

		Ok(columns)
	}
}
