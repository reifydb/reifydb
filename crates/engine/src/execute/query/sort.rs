// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering::Equal;

use async_trait::async_trait;
use reifydb_core::{
	SortDirection::{Asc, Desc},
	SortKey, error,
	util::CowVec,
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_type::diagnostic::query;
use tracing::instrument;

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode},
};

pub(crate) struct SortNode {
	input: Box<ExecutionPlan>,
	by: Vec<SortKey>,
	initialized: Option<()>,
}

impl<'a> SortNode {
	pub(crate) fn new(input: Box<ExecutionPlan>, by: Vec<SortKey>) -> Self {
		Self {
			input,
			by,
			initialized: None,
		}
	}
}

#[async_trait]
impl QueryNode for SortNode {
	#[instrument(level = "trace", skip_all, name = "query::sort::initialize")]
	async fn initialize<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.input.initialize(rx, ctx).await?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "query::sort::next")]
	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.initialized.is_some(), "SortNode::next() called before initialize()");

		let mut columns_opt: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = self.input.next(rx, ctx).await?
		{
			if let Some(existing_columns) = &mut columns_opt {
				for (i, col) in columns.into_iter().enumerate() {
					existing_columns[i].data_mut().extend(col.data().clone())?;
				}
			} else {
				columns_opt = Some(columns);
			}
		}

		let mut columns = match columns_opt {
			Some(f) => f,
			None => return Ok(None),
		};

		let key_refs =
			self.by.iter()
				.map(|key| {
					let col = columns
						.iter()
						.find(|c| c.name() == key.column.fragment())
						.ok_or_else(|| error!(query::column_not_found(key.column.clone())))?;
					Ok::<_, reifydb_type::Error>((col.data().clone(), key.direction.clone()))
				})
				.collect::<crate::Result<Vec<_>>>()?;

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
				} else {
				}
			}
			Equal
		});

		// Reorder encoded numbers if present
		if !columns.row_numbers.is_empty() {
			let reordered_row_numbers: Vec<_> = indices.iter().map(|&i| columns.row_numbers[i]).collect();
			columns.row_numbers = CowVec::new(reordered_row_numbers);
		}

		// Reorder columns
		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.data_mut().reorder(&indices);
		}

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
