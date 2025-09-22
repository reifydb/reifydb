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
		ColumnDef, ColumnId, EncodableKey, EncodableKeyRange, RowKey, RowKeyRange, Transaction,
		VersionedQueryTransaction,
		catalog::ColumnIndex,
		identifier::ColumnIdentifier,
		resolved::{ResolvedColumn as RColumn, ResolvedSource, ResolvedTable},
	},
	row::EncodedRowLayout,
	value::columnar::{
		Column, ColumnData, ColumnResolved, Columns,
		layout::{ColumnLayout, ColumnsLayout},
	},
};
use reifydb_type::{Fragment, ROW_NUMBER_COLUMN_NAME, Type, TypeConstraint};

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct TableScanNode<'a, T: Transaction> {
	table: ResolvedTable<'a>,
	context: Option<Arc<ExecutionContext<'a>>>,
	layout: ColumnsLayout<'a>,
	row_layout: EncodedRowLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<'a, T: Transaction> TableScanNode<'a, T> {
	pub fn new(table: ResolvedTable<'a>, context: Arc<ExecutionContext<'a>>) -> crate::Result<Self> {
		let data = table.columns().iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
		let row_layout = EncodedRowLayout::new(&data);

		let layout = ColumnsLayout {
			columns: table
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
			table,
			context: Some(context),
			layout,
			row_layout,
			last_key: None,
			exhausted: false,
			_phantom: PhantomData,
		})
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for TableScanNode<'a, T> {
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a, T>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	fn next(&mut self, rx: &mut crate::StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "TableScanNode::next() called before initialize()");
		let ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = ctx.batch_size;
		let range = RowKeyRange {
			source: self.table.def().id.into(),
		};

		let range = if let Some(_) = &self.last_key {
			EncodedKeyRange::new(Excluded(self.last_key.clone().unwrap()), Included(range.end().unwrap()))
		} else {
			EncodedKeyRange::new(Included(range.start().unwrap()), Included(range.end().unwrap()))
		};

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut rows_collected = 0;
		let mut new_last_key = None;

		let versioned_rows: Vec<_> = rx.range(range)?.into_iter().collect();

		for versioned in versioned_rows.into_iter() {
			if let Some(key) = RowKey::decode(&versioned.key) {
				batch_rows.push(versioned.row);
				row_numbers.push(key.row);
				new_last_key = Some(versioned.key);
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

		let mut columns = Columns::from_table(&self.table);
		columns.append_rows(&self.row_layout, batch_rows.into_iter())?;

		// Add the RowNumber column to the columns if requested
		if ctx.preserve_row_numbers {
			// Create a resolved column for row numbers
			let source = ResolvedSource::Table(self.table.clone());

			let column_ident = ColumnIdentifier::with_source(
				Fragment::owned_internal(self.table.namespace().name()),
				Fragment::owned_internal(self.table.name()),
				Fragment::owned_internal(ROW_NUMBER_COLUMN_NAME),
			);

			// Create a dummy ColumnDef for row number
			let col_def = ColumnDef {
				id: ColumnId(0),
				name: ROW_NUMBER_COLUMN_NAME.to_string(),
				constraint: TypeConstraint::unconstrained(Type::RowNumber),
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
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
