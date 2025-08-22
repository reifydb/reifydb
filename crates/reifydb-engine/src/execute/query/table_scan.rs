// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Bound::{Excluded, Included},
	sync::Arc,
};

use crate::{
	columnar::{
		layout::{ColumnLayout, ColumnsLayout}, Column, ColumnData, Columns,
		SourceQualified,
	},
	execute::{Batch, ExecutionContext},
};
use reifydb_core::interface::QueryTransaction;
use reifydb_core::{
	interface::{
		EncodableKey, EncodableKeyRange, TableDef, TableRowKey,
		TableRowKeyRange,
	}, row::EncodedRowLayout,
	value::row_number::ROW_NUMBER_COLUMN_NAME,
	EncodedKey,
	EncodedKeyRange,
};

pub(crate) struct TableScanNode {
	table: TableDef,
	context: Arc<ExecutionContext>,
	layout: ColumnsLayout,
	row_layout: EncodedRowLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl TableScanNode {
	pub fn new(
		table: TableDef,
		context: Arc<ExecutionContext>,
	) -> crate::Result<Self> {
		let data =
			table.columns.iter().map(|c| c.ty).collect::<Vec<_>>();
		let row_layout = EncodedRowLayout::new(&data);

		let layout = ColumnsLayout {
			columns: table
				.columns
				.iter()
				.map(|col| ColumnLayout {
					schema: None,
					source: None,
					name: col.name.clone(),
				})
				.collect(),
		};

		Ok(Self {
			table,
			context,
			layout,
			row_layout,
			last_key: None,
			exhausted: false,
		})
	}
}

impl TableScanNode {
	pub(crate) fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut impl QueryTransaction,
	) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let batch_size = self.context.batch_size;
		let range = TableRowKeyRange {
			table: self.table.id,
		};

		let range = if let Some(_) = &self.last_key {
			EncodedKeyRange::new(
				Excluded(self.last_key.clone().unwrap()),
				Included(range.end().unwrap()),
			)
		} else {
			EncodedKeyRange::new(
				Included(range.start().unwrap()),
				Included(range.end().unwrap()),
			)
		};

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut rows_collected = 0;
		let mut new_last_key = None;

		let versioned_rows: Vec<_> =
			rx.range(range)?.into_iter().collect();

		for versioned in versioned_rows.into_iter() {
			if let Some(key) = TableRowKey::decode(&versioned.key) {
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

		let mut columns = Columns::from_table_def(&self.table);
		columns.append_rows(&self.row_layout, batch_rows.into_iter())?;

		// Add the RowNumber column to the columns if requested
		if ctx.preserve_row_numbers {
			let row_number_column =
				Column::SourceQualified(SourceQualified {
					source: self.table.name.clone(),
					name: ROW_NUMBER_COLUMN_NAME
						.to_string(),
					data: ColumnData::row_number(
						row_numbers,
					),
				});
			columns.0.push(row_number_column);
		}

		Ok(Some(Batch {
			columns,
		}))
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
