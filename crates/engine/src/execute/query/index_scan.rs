// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	EncodedKey,
	interface::{IndexId, TableDef},
	value::{column::headers::ColumnHeaders, encoded::EncodedValuesLayout},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
};

pub(crate) struct IndexScanNode {
	_table: TableDef, // FIXME needs to work with different sources
	_index_id: IndexId,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	_row_layout: EncodedValuesLayout,
	_last_key: Option<EncodedKey>,
	_exhausted: bool,
}

impl IndexScanNode {
	pub fn new(table: TableDef, index_id: IndexId, context: Arc<ExecutionContext>) -> crate::Result<Self> {
		let data = table.columns.iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();
		let row_layout = EncodedValuesLayout::new(&data);

		let headers = ColumnHeaders {
			columns: table.columns.iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			_table: table,
			_index_id: index_id,
			context: Some(context),
			headers,
			_row_layout: row_layout,
			_last_key: None,
			_exhausted: false,
		})
	}
}

impl QueryNode for IndexScanNode {
	fn initialize<'a>(&mut self, _rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	fn next<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "IndexScanNode::next() called before initialize()");
		unimplemented!()
		// let ctx = self.context.as_ref().unwrap();
		//
		// if self.exhausted {
		// 	return Ok(None);
		// }
		//
		// let batch_size = ctx.batch_size;
		//
		// // Create range for scanning index entries
		// let source_id: PrimitiveId = self.table.id.into();
		// let base_range = IndexEntryKey::index_range(source_id, self.index_id);
		//
		// let range = if let Some(ref last_key) = self.last_key {
		// 	let end = match base_range.end {
		// 		Included(key) => Included(key),
		// 		Excluded(key) => Excluded(key),
		// 		Unbounded => unreachable!("Index range should have bounds"),
		// 	};
		// 	EncodedKeyRange::new(Excluded(last_key.clone()), end)
		// } else {
		// 	base_range
		// };
		//
		// let mut batch_rows = Vec::new();
		// let mut row_numbers = Vec::new();
		// let mut rows_collected = 0;
		// let mut new_last_key = None;
		//
		// // Scan index entries
		// let index_entries: Vec<_> = rx.range(range)?.into_iter().collect();
		//
		// for entry in index_entries.into_iter() {
		// 	let row_number_layout = EncodedRowLayout::new(&[Uint8]);
		//
		// 	let row_number = row_number_layout.get_u64(&entry.encoded, 0);
		//
		// 	let source: PrimitiveId = self.table.id.into();
		// 	let row_key = RowKey {
		// 		source,
		// 		encoded: RowNumber(row_number),
		// 	};
		//
		// 	let row_key_encoded = row_key.encode();
		//
		// 	if let Some(row_data) = rx.get(&row_key_encoded)? {
		// 		batch_rows.push(row_data.encoded);
		// 		row_numbers.push(RowNumber(row_number));
		// 		new_last_key = Some(entry.key);
		// 		rows_collected += 1;
		//
		// 		if rows_collected >= batch_size {
		// 			break;
		// 		}
		// 	}
		// }
		//
		// if batch_rows.is_empty() {
		// 	self.exhausted = true;
		// 	return Ok(None);
		// }
		//
		// self.last_key = new_last_key;
		//
		// let mut columns = Columns::from_table_def(&self.table);
		// columns.append_rows(&self.row_layout, batch_rows.into_iter())?;
		//
		// // Add the RowNumber column to the columns if requested
		// if ctx.preserve_row_numbers {
		// 	// TODO: Update IndexScanNode to use ResolvedTable instead of TableDef
		// 	let row_number_column = Column::( {
		// 		source: Fragment::internal(&self.table.name),
		// 		name: Fragment::internal(ROW_NUMBER_COLUMN_NAME),
		// 		data: ColumnData::row_number(row_numbers),
		// 	});
		// 	columns.0.push(row_number_column);
		// }
		//
		// Ok(Some(Batch {
		// 	columns,
		// }))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
