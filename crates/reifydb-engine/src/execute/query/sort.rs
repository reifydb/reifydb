// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::Equal;

use reifydb_core::{
	SortDirection::{Asc, Desc},
	SortKey, error,
	interface::{QueryTransaction, Transaction},
	result::error::diagnostic::query,
};

use crate::{
	StandardCommandTransaction,
	columnar::{Columns, layout::ColumnsLayout},
	execute::{Batch, ExecutionContext, ExecutionPlan},
};

pub(crate) struct SortNode {
	input: Box<ExecutionPlan>,
	by: Vec<SortKey>,
}

impl SortNode {
	pub(crate) fn new(input: Box<ExecutionPlan>, by: Vec<SortKey>) -> Self {
		Self {
			input,
			by,
		}
	}
}

impl SortNode {
	pub(crate) fn next<T: Transaction>(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut StandardCommandTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		let mut columns_opt: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = self.input.next(ctx, rx)?
		{
			if let Some(existing_columns) = &mut columns_opt {
				for (i, col) in columns.into_iter().enumerate()
				{
					existing_columns[i]
						.data_mut()
						.extend(col.data().clone())?;
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
					let col =
						columns.iter()
							.find(|c| {
								c.qualified_name()
								== key.column
									.fragment() || c.name()
								== key.column
									.fragment()
							})
							.ok_or_else(|| {
								error!(query::column_not_found(key.column.clone()))
							})?;
					Ok::<_, reifydb_core::Error>((
						col.data().clone(),
						key.direction.clone(),
					))
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

		for col in columns.iter_mut() {
			col.data_mut().reorder(&indices);
		}

		Ok(Some(Batch {
			columns,
		}))
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		self.input.layout()
	}
}
