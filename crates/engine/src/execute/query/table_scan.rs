// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	EncodedKey,
	interface::{
		DictionaryDef, EncodableKey, MultiVersionQueryTransaction, RowKey, RowKeyRange, resolved::ResolvedTable,
	},
	value::{
		column::{Column, ColumnData, Columns, headers::ColumnHeaders},
		encoded::EncodedValuesLayout,
	},
};
use reifydb_type::{DictionaryEntryId, Fragment, Type};
use tracing::{instrument, trace};

use crate::{
	execute::{Batch, ExecutionContext, QueryNode},
	transaction::operation::DictionaryOperations,
};

pub(crate) struct TableScanNode<'a> {
	table: ResolvedTable<'a>,
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	row_layout: EncodedValuesLayout,
	/// Storage types for each column (dictionary ID types for dictionary columns)
	storage_types: Vec<Type>,
	/// Dictionary definitions for columns that need decoding (None for non-dictionary columns)
	dictionaries: Vec<Option<DictionaryDef>>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl<'a> TableScanNode<'a> {
	pub fn new<Rx: MultiVersionQueryTransaction + reifydb_core::interface::QueryTransaction>(
		table: ResolvedTable<'a>,
		context: Arc<ExecutionContext<'a>>,
		rx: &mut Rx,
	) -> crate::Result<Self> {
		// Look up dictionaries and build storage types
		let mut storage_types = Vec::with_capacity(table.columns().len());
		let mut dictionaries = Vec::with_capacity(table.columns().len());

		for col in table.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = CatalogStore::find_dictionary(rx, dict_id)? {
					storage_types.push(dict.id_type);
					dictionaries.push(Some(dict));
				} else {
					// Dictionary not found, fall back to constraint type
					storage_types.push(col.constraint.get_type());
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				dictionaries.push(None);
			}
		}

		let row_layout = EncodedValuesLayout::new(&storage_types);

		let headers = ColumnHeaders {
			columns: table.columns().iter().map(|col| Fragment::owned_internal(&col.name)).collect(),
		};

		Ok(Self {
			table,
			context: Some(context),
			headers,
			row_layout,
			storage_types,
			dictionaries,
			last_key: None,
			exhausted: false,
		})
	}
}

impl<'a> QueryNode<'a> for TableScanNode<'a> {
	#[instrument(level = "trace", skip_all, name = "TableScanNode::initialize")]
	fn initialize(
		&mut self,
		_rx: &mut crate::StandardTransaction<'a>,
		_ctx: &ExecutionContext<'a>,
	) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "TableScanNode::next")]
	fn next(
		&mut self,
		rx: &mut crate::StandardTransaction<'a>,
		_ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "TableScanNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;

		let range = RowKeyRange::scan_range(self.table.def().id.into(), self.last_key.as_ref());

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut new_last_key = None;

		let multi_rows: Vec<_> =
			rx.range_batched(range, batch_size)?.into_iter().take(batch_size as usize).collect();

		for multi in multi_rows.into_iter() {
			if let Some(key) = RowKey::decode(&multi.key) {
				batch_rows.push(multi.values);
				row_numbers.push(key.row);
				new_last_key = Some(multi.key);
			}
		}
		if batch_rows.is_empty() {
			self.exhausted = true;
			trace!("table scan exhausted");
			return Ok(None);
		}

		trace!(row_count = batch_rows.len(), "table scan batch loaded");
		self.last_key = new_last_key;

		// Create columns with storage types (dictionary ID types for dictionary columns)
		let storage_columns: Vec<Column> = self
			.table
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| Column {
				name: Fragment::owned_internal(&col.name),
				data: ColumnData::with_capacity(self.storage_types[idx], 0),
			})
			.collect();

		let mut columns = Columns::with_row_numbers(storage_columns, Vec::new());
		columns.append_rows(&self.row_layout, batch_rows.into_iter(), row_numbers.clone())?;

		// Decode dictionary columns
		self.decode_dictionary_columns(&mut columns, rx)?;

		// Restore row numbers (they get cleared during column transformation)
		columns.row_numbers = reifydb_core::util::CowVec::new(row_numbers);

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}

impl<'a> TableScanNode<'a> {
	/// Decode dictionary columns by replacing dictionary IDs with actual values
	fn decode_dictionary_columns(
		&self,
		columns: &mut Columns<'a>,
		rx: &mut crate::StandardTransaction<'a>,
	) -> crate::Result<()> {
		for (col_idx, dict_opt) in self.dictionaries.iter().enumerate() {
			if let Some(dictionary) = dict_opt {
				let col = &columns[col_idx];
				let row_count = col.data().len();

				// Create new column data with the original value type
				let mut new_data = ColumnData::with_capacity(dictionary.value_type, row_count);

				// Decode each value
				for row_idx in 0..row_count {
					let id_value = col.data().get_value(row_idx);
					if let Some(entry_id) = DictionaryEntryId::from_value(&id_value) {
						if let Some(decoded_value) =
							rx.get_from_dictionary(dictionary, entry_id)?
						{
							new_data.push_value(decoded_value);
						} else {
							// ID not found in dictionary, use undefined
							new_data.push_value(reifydb_type::Value::Undefined);
						}
					} else {
						// Not a valid dictionary ID, use undefined
						new_data.push_value(reifydb_type::Value::Undefined);
					}
				}

				// Replace the column data
				let col_name = columns[col_idx].name().clone();
				columns.columns.make_mut()[col_idx] = Column {
					name: col_name,
					data: new_data,
				};
			}
		}
		Ok(())
	}
}
