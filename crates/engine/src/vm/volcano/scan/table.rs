// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	error::diagnostic,
	interface::{catalog::dictionary::Dictionary, resolved::ResolvedTable},
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::{
		batch::lazy::{LazyBatch, LazyColumnMeta},
		column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error, fragment::Fragment, util::cowvec::CowVec, value::r#type::Type};
use tracing::instrument;

use super::super::decode_dictionary_columns;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct TableScanNode {
	table: ResolvedTable,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	/// Storage types for each column (dictionary ID types for dictionary columns)
	storage_types: Vec<Type>,
	/// Dictionary definitions for columns that need decoding (None for non-dictionary columns)
	dictionaries: Vec<Option<Dictionary>>,
	/// Cached shape loaded from the first batch
	shape: Option<RowShape>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	scan_limit: Option<usize>,
}

impl TableScanNode {
	pub fn new(table: ResolvedTable, context: Arc<QueryContext>, rx: &mut Transaction<'_>) -> Result<Self> {
		// Look up dictionaries and build storage types
		let mut storage_types = Vec::with_capacity(table.columns().len());
		let mut dictionaries = Vec::with_capacity(table.columns().len());

		for col in table.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.services.catalog.find_dictionary(rx, dict_id)? {
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
			shape: None,
			last_key: None,
			exhausted: false,
			scan_limit: None,
		})
	}

	fn get_or_load_shape<'a>(&mut self, rx: &mut Transaction<'a>, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("TableScanNode context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			error!(diagnostic::internal::internal(format!(
				"RowShape with fingerprint {:?} not found for table {}",
				fingerprint,
				self.table.def().name
			)))
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}
}

impl QueryNode for TableScanNode {
	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		// Already has context from constructor
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "TableScanNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = match self.scan_limit {
			Some(limit) => (limit as u64).min(stored_ctx.batch_size),
			None => stored_ctx.batch_size,
		};

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
						batch_rows.push(multi.row);
						row_numbers.push(key.row);
						new_last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e),
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
			if self.last_key.is_none() {
				// Empty table: return empty columns with correct types to preserve shape
				let columns: Vec<ColumnWithName> = self
					.table
					.columns()
					.iter()
					.map(|col| ColumnWithName {
						name: Fragment::internal(&col.name),
						data: ColumnBuffer::none_typed(col.constraint.get_type(), 0),
					})
					.collect();
				return Ok(Some(Columns::new(columns)));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		// Create columns with storage types (dictionary ID types for dictionary columns)
		let storage_columns: Vec<ColumnWithName> = {
			self.table
				.columns()
				.iter()
				.enumerate()
				.map(|(idx, col)| ColumnWithName {
					name: Fragment::internal(&col.name),
					data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
				})
				.collect()
		};

		let mut columns = Columns::with_system_columns(storage_columns, Vec::new(), Vec::new(), Vec::new());
		{
			let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
			columns.append_rows(&shape, batch_rows.into_iter(), row_numbers.clone())?;
		}
		// Restore row numbers (they get cleared during column transformation)
		columns.row_numbers = CowVec::new(row_numbers);

		decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::table::next_lazy")]
	fn next_lazy<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<LazyBatch>> {
		debug_assert!(self.context.is_some(), "TableScanNode::next_lazy() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = match self.scan_limit {
			Some(limit) => (limit as u64).min(stored_ctx.batch_size),
			None => stored_ctx.batch_size,
		};

		let range = RowKeyRange::scan_range(self.table.def().id.into(), self.last_key.as_ref());

		let mut stream = rx.range(range, batch_size as usize)?;

		let mut encoded_rows = Vec::with_capacity(batch_size as usize);
		let mut row_numbers = Vec::with_capacity(batch_size as usize);

		// Consume up to batch_size items from the stream
		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					if let Some(key) = RowKey::decode(&multi.key) {
						encoded_rows.push(multi.row);
						row_numbers.push(key.row);
						self.last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e),
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
					storage_type: self.storage_types[idx].clone(),
					output_type,
					dictionary: self.dictionaries[idx].clone(),
				}
			})
			.collect();

		let shape = self.get_or_load_shape(rx, &encoded_rows[0])?;
		Ok(Some(LazyBatch::new(encoded_rows, row_numbers, &shape, column_metas)))
	}

	fn set_scan_limit(&mut self, limit: usize) {
		self.scan_limit = Some(limit);
	}
}
