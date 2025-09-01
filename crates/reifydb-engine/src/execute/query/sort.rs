// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::Equal;

use reifydb_core::{
	SortDirection::{Asc, Desc},
	SortKey, error,
	interface::Transaction,
	value::columnar::{Columns, layout::ColumnsLayout},
};
use reifydb_type::diagnostic::query;

use crate::execute::{Batch, ExecutionContext, ExecutionPlan, QueryNode};

pub(crate) struct SortNode<'a, T: Transaction> {
	input: Box<ExecutionPlan<'a, T>>,
	by: Vec<SortKey>,
	initialized: Option<()>,
}

impl<'a, T: Transaction> SortNode<'a, T> {
	pub(crate) fn new(
		input: Box<ExecutionPlan<'a, T>>,
		by: Vec<SortKey>,
	) -> Self {
		Self {
			input,
			by,
			initialized: None,
		}
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for SortNode<'a, T> {
	fn initialize(
		&mut self,
		rx: &mut crate::StandardTransaction<'a, T>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		self.input.initialize(rx, ctx)?;
		self.initialized = Some(());
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(
			self.initialized.is_some(),
			"SortNode::next() called before initialize()"
		);

		let mut columns_opt: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = self.input.next(rx)?
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
					Ok::<_, reifydb_type::Error>((
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

	fn layout(&self) -> Option<ColumnsLayout> {
		self.input.layout()
	}
}
