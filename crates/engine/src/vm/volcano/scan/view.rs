// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	interface::{catalog::dictionary::Dictionary, resolved::ResolvedView},
	internal_error,
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	util::cowvec::CowVec,
	value::{row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use super::super::decode_dictionary_columns;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct ViewScanNode {
	view: ResolvedView,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	storage_types: Vec<ValueType>,
	dictionaries: Vec<Option<Dictionary>>,
	shape: Option<RowShape>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	sorted: bool,
}

impl ViewScanNode {
	pub fn new(view: ResolvedView, context: Arc<QueryContext>, rx: &mut Transaction<'_>) -> Result<Self> {
		let mut storage_types = Vec::with_capacity(view.columns().len());
		let mut dictionaries = Vec::with_capacity(view.columns().len());

		for col in view.columns() {
			if let Some(dict_id) = col.dictionary_id {
				if let Some(dict) = context.services.catalog.find_dictionary(rx, dict_id)? {
					storage_types.push(ValueType::DictionaryId);
					dictionaries.push(Some(dict));
				} else {
					storage_types.push(col.constraint.get_type());
					dictionaries.push(None);
				}
			} else {
				storage_types.push(col.constraint.get_type());
				dictionaries.push(None);
			}
		}

		let headers = ColumnHeaders {
			columns: view.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};
		let sorted = !view.def().sort().is_empty();

		Ok(Self {
			view,
			context: Some(context),
			headers,
			storage_types,
			dictionaries,
			shape: None,
			last_key: None,
			exhausted: false,
			sorted,
		})
	}

	fn get_or_load_shape<'a>(&mut self, rx: &mut Transaction<'a>, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();

		let stored_ctx = self.context.as_ref().expect("ViewScanNode context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"RowShape with fingerprint {:?} not found for view {}",
				fingerprint,
				self.view.def().name()
			)
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}
}

impl QueryNode for ViewScanNode {
	#[instrument(name = "volcano::scan::view::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(name = "volcano::scan::view::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "ViewScanNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap();

		if self.exhausted {
			return Ok(None);
		}

		let batch_size = stored_ctx.batch_size;
		let range = RowKeyRange::scan_range(self.view.def().underlying_id(), self.last_key.as_ref());

		let mut batch_rows = Vec::new();
		let mut row_numbers = Vec::new();
		let mut new_last_key = None;

		let mut stream = rx.range(range, RangeScope::All, batch_size as usize)?;
		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					let row = if self.sorted {
						let bytes = multi.key.as_slice();
						RowNumber(u64::from_be_bytes(
							bytes[bytes.len() - 8..].try_into().unwrap(),
						))
					} else if let Some(key) = RowKey::decode(&multi.key) {
						key.row
					} else {
						continue;
					};
					batch_rows.push(multi.row);
					row_numbers.push(row);
					new_last_key = Some(multi.key);
				}
				Some(Err(e)) => return Err(e),
				None => {
					self.exhausted = true;
					break;
				}
			}
		}

		drop(stream);
		if batch_rows.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				return Ok(Some(Columns::from_catalog_columns(self.view.columns())));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		let storage_columns: Vec<ColumnWithName> = self
			.view
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
			})
			.collect();

		let mut columns = Columns::with_system_columns(storage_columns, Vec::new(), Vec::new(), Vec::new());
		{
			let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
			columns.append_rows(&shape, batch_rows.into_iter(), row_numbers.clone())?;
		}

		columns.row_numbers = CowVec::new(row_numbers);

		decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
