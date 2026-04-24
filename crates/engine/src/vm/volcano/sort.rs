// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering::Equal;

use reifydb_core::{
	error::diagnostic::query,
	sort::{
		SortDirection::{Asc, Desc},
		SortKey,
	},
	value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error,
	error::Error,
	util::cowvec::CowVec,
	value::{
		datetime::{CREATED_AT_COLUMN_NAME, UPDATED_AT_COLUMN_NAME},
		row_number::ROW_NUMBER_COLUMN_NAME,
	},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
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
		debug_assert!(self.initialized.is_some(), "SortNode::next() called before initialize()");

		let mut columns_opt: Option<Columns> = None;

		while let Some(columns) = self.input.next(rx, ctx)? {
			if let Some(existing_columns) = &mut columns_opt {
				existing_columns.row_numbers.make_mut().extend(columns.row_numbers.iter().copied());
				existing_columns.created_at.make_mut().extend(columns.created_at.iter().copied());
				existing_columns.updated_at.make_mut().extend(columns.updated_at.iter().copied());
				for (i, col) in columns.columns.iter().enumerate() {
					existing_columns[i].extend(col.clone())?;
				}
			} else {
				columns_opt = Some(columns);
			}
		}

		let columns = match columns_opt {
			Some(f) => f,
			None => return Ok(None),
		};

		let transform_ctx = TransformContext {
			functions: &ctx.services.functions,
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
		let key_refs = self
			.by
			.iter()
			.map(|key| {
				let name = key.column.fragment();
				let stripped = name.strip_prefix('#').unwrap_or(name);

				if stripped == ROW_NUMBER_COLUMN_NAME && !columns.row_numbers.is_empty() {
					let data: Vec<u64> = columns.row_numbers.iter().map(|r| r.value()).collect();
					return Ok::<_, Error>((ColumnBuffer::uint8(data), key.direction.clone()));
				}
				if stripped == CREATED_AT_COLUMN_NAME && !columns.created_at.is_empty() {
					return Ok((
						ColumnBuffer::datetime(columns.created_at.to_vec()),
						key.direction.clone(),
					));
				}
				if stripped == UPDATED_AT_COLUMN_NAME && !columns.updated_at.is_empty() {
					return Ok((
						ColumnBuffer::datetime(columns.updated_at.to_vec()),
						key.direction.clone(),
					));
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

		if !columns.row_numbers.is_empty() {
			let reordered: Vec<_> = indices.iter().map(|&i| columns.row_numbers[i]).collect();
			columns.row_numbers = CowVec::new(reordered);
		}
		if !columns.created_at.is_empty() {
			let reordered: Vec<_> = indices.iter().map(|&i| columns.created_at[i]).collect();
			columns.created_at = CowVec::new(reordered);
		}
		if !columns.updated_at.is_empty() {
			let reordered: Vec<_> = indices.iter().map(|&i| columns.updated_at[i]).collect();
			columns.updated_at = CowVec::new(reordered);
		}

		let cols = columns.columns.make_mut();
		for col in cols.iter_mut() {
			col.reorder(&indices);
		}

		Ok(columns)
	}
}
