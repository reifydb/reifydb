// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	ops::Bound::{Excluded, Included, Unbounded},
	sync::Arc,
};

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{
		EncodableKey, IndexEntryKey, IndexId, RowKey, SourceId,
		TableDef, Transaction, VersionedQueryTransaction,
	},
	row::EncodedRowLayout,
	value::columnar::{
		Column, ColumnData, Columns, SourceQualified,
		layout::{ColumnLayout, ColumnsLayout},
	},
};
use reifydb_type::{ROW_NUMBER_COLUMN_NAME, RowNumber, Type::Uint8};

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct IndexScanNode<T: Transaction> {
	table: TableDef,
	index_id: IndexId,
	context: Option<Arc<ExecutionContext>>,
	layout: ColumnsLayout,
	row_layout: EncodedRowLayout,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	_phantom: std::marker::PhantomData<T>,
}

impl<T: Transaction> IndexScanNode<T> {
	pub fn new(
		table: TableDef,
		index_id: IndexId,
		context: Arc<ExecutionContext>,
	) -> crate::Result<Self> {
		let data = table
			.columns
			.iter()
			.map(|c| c.constraint.get_type())
			.collect::<Vec<_>>();
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
			index_id,
			context: Some(context),
			layout,
			row_layout,
			last_key: None,
			exhausted: false,
			_phantom: std::marker::PhantomData,
		})
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for IndexScanNode<T> {
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a, T>,
		_ctx: &ExecutionContext,
	) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(
			self.context.is_some(),
			"IndexScanNode::next() called before initialize()"
		);
		let ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = ctx.batch_size;

		// Create range for scanning index entries
		let source_id: SourceId = self.table.id.into();
		let base_range =
			IndexEntryKey::index_range(source_id, self.index_id);

		let range = if let Some(ref last_key) = self.last_key {
			let end = match base_range.end {
				Included(key) => Included(key),
				Excluded(key) => Excluded(key),
				Unbounded => unreachable!(
					"Index range should have bounds"
				),
			};
			EncodedKeyRange::new(Excluded(last_key.clone()), end)
		} else {
			base_range
		};

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut rows_collected = 0;
		let mut new_last_key = None;

		// Scan index entries
		let index_entries: Vec<_> =
			rx.range(range)?.into_iter().collect();

		for entry in index_entries.into_iter() {
			let row_number_layout = EncodedRowLayout::new(&[Uint8]);

			let row_number =
				row_number_layout.get_u64(&entry.row, 0);

			// Fetch the actual row using the row number
			let source: SourceId = self.table.id.into();
			let row_key = RowKey {
				source,
				row: RowNumber(row_number),
			};

			let row_key_encoded = row_key.encode();

			if let Some(row_data) = rx.get(&row_key_encoded)? {
				batch_rows.push(row_data.row);
				row_numbers.push(RowNumber(row_number));
				new_last_key = Some(entry.key);
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

	fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
