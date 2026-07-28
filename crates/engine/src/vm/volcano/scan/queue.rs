// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, sync::Arc};

use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::{catalog::dictionary::Dictionary, resolved::ResolvedQueue},
	internal_error,
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::{
		ColumnWithName,
		buffer::ColumnBuffer,
		columns::{Columns, SystemColumns},
		headers::ColumnHeaders,
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	fragment::Fragment,
	value::{row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use super::super::decode_dictionary_columns;
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub struct QueueScan {
	queue: ResolvedQueue,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	storage_types: Vec<ValueType>,
	dictionaries: Vec<Option<Dictionary>>,
	last_key: Option<EncodedKey>,
	exhausted: bool,
	context: Option<Arc<QueryContext>>,
}

impl QueueScan {
	pub fn new(queue: ResolvedQueue, context: Arc<QueryContext>, rx: &mut Transaction<'_>) -> Result<Self> {
		let mut storage_types = Vec::with_capacity(queue.columns().len());
		let mut dictionaries = Vec::with_capacity(queue.columns().len());

		for col in queue.columns() {
			if let Some(dict_id) = col.dictionary_id
				&& let Some(dict) = context.services.catalog.find_dictionary(rx, dict_id)?
			{
				storage_types.push(ValueType::DictionaryId);
				dictionaries.push(Some(dict));
				continue;
			}
			storage_types.push(col.constraint.get_type());
			dictionaries.push(None);
		}

		let headers = ColumnHeaders {
			columns: queue.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			queue,
			headers,
			shape: None,
			storage_types,
			dictionaries,
			last_key: None,
			exhausted: false,
			context: Some(context),
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first_row: &EncodedRow) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = first_row.fingerprint();
		let stored_ctx = self.context.as_ref().expect("QueueScan context not set");
		let shape = stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
			internal_error!(
				"RowShape with fingerprint {:?} not found for queue {}",
				fingerprint,
				self.queue.def().name
			)
		})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	fn enqueue_order_range(&self) -> EncodedKeyRange {
		let full = RowKeyRange::scan_range(self.queue.def().id.into(), None);
		match &self.last_key {
			Some(last_key) => EncodedKeyRange::new(full.start.clone(), Bound::Excluded(last_key.clone())),
			None => full,
		}
	}

	fn empty_declared_columns(&self) -> Columns {
		Columns::new(
			self.queue
				.columns()
				.iter()
				.map(|col| ColumnWithName {
					name: Fragment::internal(&col.name),
					data: ColumnBuffer::none_typed(col.constraint.get_type(), 0),
				})
				.collect(),
		)
	}
}

impl QueryNode for QueueScan {
	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.exhausted {
			return Ok(None);
		}

		let batch_size = self.context.as_ref().expect("QueueScan context not set").batch_size;
		let range = self.enqueue_order_range();

		let mut batch_rows: Vec<EncodedRow> = Vec::new();
		let mut row_numbers: Vec<RowNumber> = Vec::new();
		let mut new_last_key = None;

		let mut stream = rx.range_rev(range, RangeScope::All, batch_size as usize)?;

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

		drop(stream);

		if batch_rows.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				return Ok(Some(self.empty_declared_columns()));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
		let declared = self.queue.columns().len();

		let mut storage_columns: Vec<ColumnWithName> = self
			.queue
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| ColumnWithName {
				name: Fragment::internal(&col.name),
				data: ColumnBuffer::with_capacity(self.storage_types[idx].clone(), 0),
			})
			.collect();

		for index in declared..shape.field_count() {
			let field = shape.get_field(index).ok_or_else(|| {
				internal_error!("queue {} shape lost field {}", self.queue.def().name, index)
			})?;
			storage_columns.push(ColumnWithName {
				name: Fragment::internal(field.name.clone()),
				data: ColumnBuffer::with_capacity(field.constraint.get_type(), 0),
			});
		}

		let mut columns = Columns::with_system(storage_columns, SystemColumns::default());
		columns.append_rows(&shape, batch_rows.into_iter(), row_numbers)?;

		decode_dictionary_columns(&mut columns, &self.dictionaries, rx)?;

		columns.columns.make_mut().truncate(declared);
		columns.names.make_mut().truncate(declared);

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
