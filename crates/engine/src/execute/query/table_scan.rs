// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, schema::Schema},
	error::diagnostic,
	interface::{catalog::dictionary::DictionaryDef, resolved::ResolvedTable},
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::{
		batch::lazy::{LazyBatch, LazyColumnMeta},
		column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
	},
};
use reifydb_transaction::transaction::{AsTransaction, Transaction};
use reifydb_type::{error, fragment::Fragment, util::cowvec::CowVec, value::r#type::Type};
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct TableScanNode {
	table: ResolvedTable,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	/// Storage types for each column (dictionary ID types for dictionary columns)
	storage_types: Vec<Type>,
	/// Dictionary definitions for columns that need decoding (None for non-dictionary columns)
	dictionaries: Vec<Option<DictionaryDef>>,
	/// Cached schema loaded from the first batch
	schema: Option<Schema>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
}

impl TableScanNode {
	pub fn new<Rx: AsTransaction>(
		table: ResolvedTable,
		context: Arc<ExecutionContext>,
		rx: &mut Rx,
	) -> crate::Result<Self> {
		// Look up dictionaries and build storage types
		let mut storage_types = Vec::with_capacity(table.columns().len());
		let mut dictionaries = Vec::with_capacity(table.columns().len());

		for col in table.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.executor.catalog.find_dictionary(rx, dict_id)? {
					storage_types.push(Type::DictionaryId);
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

		let headers = ColumnHeaders {
			columns: table.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			table,
			context: Some(context),
			headers,
			storage_types,
			dictionaries,
			schema: None,
			last_key: None,
			exhausted: false,
		})
	}

	fn get_or_load_schema<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		first_row: &reifydb_core::encoded::encoded::EncodedValues,
	) -> crate::Result<Schema> {
		if let Some(schema) = &self.schema {
			return Ok(schema.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("TableScanNode context not set");
		let schema = stored_ctx.executor.catalog.schema.get_or_load(fingerprint, rx)?.ok_or_else(|| {
			error!(diagnostic::internal::internal(format!(
				"Schema with fingerprint {:?} not found for table {}",
				fingerprint,
				self.table.def().name
			)))
		})?;

		self.schema = Some(schema.clone());

		Ok(schema)
	}
}

impl QueryNode for TableScanNode {
	#[instrument(level = "trace", skip_all, name = "query::scan::table::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "query::scan::table::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>> {
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

		// Use streaming API which properly handles version density at storage level
		let mut stream = rx.range(range, batch_size as usize)?;

		// Consume up to batch_size items from the stream
		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					if let Some(key) = RowKey::decode(&multi.key) {
						batch_rows.push(multi.values);
						row_numbers.push(key.row);
						new_last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e.into()),
				None => {
					self.exhausted = true;
					break;
				}
			}
		}

		// Drop the stream to release the borrow on rx before dictionary decoding
		drop(stream);

		if batch_rows.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		self.last_key = new_last_key;

		// Create columns with storage types (dictionary ID types for dictionary columns)
		let storage_columns: Vec<Column> = {
			self.table
				.columns()
				.iter()
				.enumerate()
				.map(|(idx, col)| Column {
					name: Fragment::internal(&col.name),
					data: ColumnData::with_capacity(self.storage_types[idx], 0),
				})
				.collect()
		};

		let mut columns = Columns::with_row_numbers(storage_columns, Vec::new());
		{
			let schema = self.get_or_load_schema(rx, &batch_rows[0])?;
			columns.append_rows(&schema, batch_rows.into_iter(), row_numbers.clone())?;
		}
		// Restore row numbers (they get cleared during column transformation)
		columns.row_numbers = CowVec::new(row_numbers);

		super::decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}

	#[instrument(level = "trace", skip_all, name = "query::scan::table::next_lazy")]
	fn next_lazy<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<LazyBatch>> {
		debug_assert!(self.context.is_some(), "TableScanNode::next_lazy() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;

		let range = RowKeyRange::scan_range(self.table.def().id.into(), self.last_key.as_ref());

		let mut stream = rx.range(range, batch_size as usize)?;

		let mut encoded_rows = Vec::with_capacity(batch_size as usize);
		let mut row_numbers = Vec::with_capacity(batch_size as usize);

		// Consume up to batch_size items from the stream
		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					if let Some(key) = RowKey::decode(&multi.key) {
						encoded_rows.push(multi.values);
						row_numbers.push(key.row);
						self.last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e.into()),
				None => {
					self.exhausted = true;
					break;
				}
			}
		}

		drop(stream);

		if encoded_rows.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		// Build column metas
		let column_metas: Vec<LazyColumnMeta> = self
			.table
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| {
				let output_type = col.constraint.get_type();
				LazyColumnMeta {
					name: Fragment::internal(&col.name),
					storage_type: self.storage_types[idx],
					output_type,
					dictionary: self.dictionaries[idx].clone(),
				}
			})
			.collect();

		let schema = self.get_or_load_schema(rx, &encoded_rows[0])?;
		Ok(Some(LazyBatch::new(encoded_rows, row_numbers, &schema, column_metas)))
	}
}
