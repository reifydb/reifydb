// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	marker::PhantomData,
	ops::Bound::{Excluded, Included},
	sync::Arc,
};

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{
		EncodableKey, EncodableKeyRange, MultiVersionQueryTransaction, RowKey, RowKeyRange, Transaction,
		identifier::ColumnIdentifier,
		resolved::{ResolvedColumn as RColumn, ResolvedSource, ResolvedView},
	},
	log_debug,
	value::{
		column::{
			Column, ColumnData, ColumnResolved, Columns,
			layout::{ColumnLayout, ColumnsLayout},
		},
		row::EncodedRowLayout,
	},
};
use reifydb_type::{Fragment, ROW_NUMBER_COLUMN_NAME};

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct ViewScanNode<'a, T: Transaction> {
	view: ResolvedView<'a>,
	context: Option<Arc<ExecutionContext<'a>>>,
	layout: ColumnsLayout<'a>,
	row_layout: EncodedRowLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<'a, T: Transaction> ViewScanNode<'a, T> {
	pub fn new(view: ResolvedView<'a>, context: Arc<ExecutionContext<'a>>) -> crate::Result<Self> {
		let data = view.columns().iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
		let row_layout = EncodedRowLayout::new(&data);

		let layout = ColumnsLayout {
			columns: view
				.columns()
				.iter()
				.map(|col| ColumnLayout {
					namespace: None,
					source: None,
					name: Fragment::owned_internal(&col.name),
				})
				.collect(),
		};

		Ok(Self {
			view,
			context: Some(context),
			layout,
			row_layout,
			last_key: None,
			exhausted: false,
			_phantom: PhantomData,
		})
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for ViewScanNode<'a, T> {
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a, T>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	fn next(&mut self, rx: &mut crate::StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "ViewScanNode::next() called before initialize()");
		let ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = ctx.batch_size;
		let range = RowKeyRange {
			source: self.view.def().id.into(),
		};

		let range = if let Some(_) = &self.last_key {
			EncodedKeyRange::new(Excluded(self.last_key.clone().unwrap()), Included(range.end().unwrap()))
		} else {
			EncodedKeyRange::new(Included(range.start().unwrap()), Included(range.end().unwrap()))
		};

		log_debug!(
			"ViewScan: Scanning view {:?} with range {:?} to {:?}",
			self.view.def().id,
			range.start,
			range.end
		);

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut rows_collected = 0;
		let mut new_last_key = None;

		let multi_rows: Vec<_> = rx.range(range)?.into_iter().collect();

		log_debug!("ViewScan: Found {} rows for view {:?}", multi_rows.len(), self.view.def().id);

		for multi in multi_rows.into_iter() {
			if let Some(key) = RowKey::decode(&multi.key) {
				batch_rows.push(multi.row);
				row_numbers.push(key.row);
				new_last_key = Some(multi.key);
				rows_collected += 1;

				if rows_collected >= batch_size {
					break;
				}
			}
		}

		if batch_rows.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		self.last_key = new_last_key;

		let mut columns = Columns::from_view(&self.view);
		columns.append_rows(&self.row_layout, batch_rows.into_iter())?;

		// Add the RowNumber column to the columns if requested
		if ctx.preserve_row_numbers {
			// Create a resolved column for row numbers
			let source = ResolvedSource::View(self.view.clone());
			let column_ident = ColumnIdentifier::with_source(
				Fragment::owned_internal(self.view.namespace().name()),
				Fragment::owned_internal(self.view.name()),
				Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
			);
			// Create a dummy ColumnDef for row number
			let col_def = reifydb_core::interface::ColumnDef {
				id: reifydb_core::interface::ColumnId(0),
				name: ROW_NUMBER_COLUMN_NAME.to_string(),
				constraint: reifydb_type::TypeConstraint::unconstrained(reifydb_type::Type::RowNumber),
				index: reifydb_core::interface::catalog::ColumnIndex(0),
				auto_increment: false,
				policies: Vec::new(),
			};
			let resolved_col = RColumn::new(column_ident, source, col_def);
			let row_number_column = Column::Resolved(ColumnResolved::new(
				resolved_col,
				ColumnData::row_number(row_numbers),
			));
			columns.0.push(row_number_column);
		}

		Ok(Some(Batch {
			columns,
		}))
	}

	fn layout(&self) -> Option<ColumnsLayout<'a>> {
		Some(self.layout.clone())
	}
}
