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
		resolved::ResolvedTable,
	},
	value::{
		column::{Columns, headers::ColumnHeaders},
		row::EncodedRowLayout,
	},
};
use reifydb_type::Fragment;

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct TableScanNode<'a, T: Transaction> {
	table: ResolvedTable<'a>,
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	row_layout: EncodedRowLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<'a, T: Transaction> TableScanNode<'a, T> {
	pub fn new(table: ResolvedTable<'a>, context: Arc<ExecutionContext<'a>>) -> crate::Result<Self> {
		let data = table.columns().iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
		let row_layout = EncodedRowLayout::new(&data);

		let headers = ColumnHeaders {
			columns: table.columns().iter().map(|col| Fragment::owned_internal(&col.name)).collect(),
		};

		Ok(Self {
			table,
			context: Some(context),
			headers,
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

		let multi_rows: Vec<_> = rx.range(range)?.into_iter().collect();

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

		let mut columns = Columns::from_table(&self.table);
		columns.append_rows(&self.row_layout, batch_rows.into_iter(), row_numbers)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}
