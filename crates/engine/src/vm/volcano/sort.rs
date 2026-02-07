// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp::Ordering::Equal;

use reifydb_core::{
	error::diagnostic::query,
	sort::{
		SortDirection::{Asc, Desc},
		SortKey,
	},
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error, util::cowvec::CowVec};
use tracing::instrument;

use crate::vm::volcano::query::{QueryContext, QueryNode, QueryOperator};

pub(crate) struct SortNode {
	input: Box<QueryOperator>,
	by: Vec<SortKey>,
	initialized: Option<()>,
}

impl<'a> SortNode {
	pub(crate) fn new(input: Box<QueryOperator>, by: Vec<SortKey>) -> Self {
		Self {
			input,
			by,
			initialized: None,
		}
	}
}

impl QueryNode for SortNode {
	#[instrument(level = "trace", skip_all, name = "volcano::sort::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::sort::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.initialized.is_some(), "SortNode::next() called before initialize()");

		let mut columns_opt: Option<Columns> = None;

		while let Some(columns) = self.input.next(rx, ctx)? {
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
					Ok::<_, reifydb_type::error::Error>((col.data().clone(), key.direction.clone()))
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

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}
