// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{bytes::EncodedBytes, queue::EncodedQueueRow, shape::RowShape};
use reifydb_core::{
	common::TimeSource,
	interface::{catalog::dictionary::Dictionary, resolved::ResolvedQueue, store::MultiVersionRow},
	internal_error,
	key::{any::TaggedKey, bound::TaggedKeyBoundRange, row::RowKeyRange},
	value::{
		batch::{append_rows, batch},
		column::{builder::ColumnBuilder, headers::ColumnHeaders},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{row_number::RowNumber, system_columns::SystemColumn, value_type::ValueType};
use tracing::instrument;

use super::{
	super::{decode_dictionary_columns, user_pairs, with_user_columns},
	empty_scan, scan_headers, source_system_columns,
};
use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

type DrainedBatch = (Vec<EncodedBytes>, Vec<RowNumber>, Option<TaggedKey>, bool);

pub struct QueueScan {
	queue: ResolvedQueue,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	storage_types: Vec<ValueType>,
	dictionaries: Vec<Option<(Dictionary, ValueType)>>,
	last_key: Option<TaggedKey>,
	exhausted: bool,
	context: Option<Arc<QueryContext>>,
	system_columns: Vec<SystemColumn>,
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
				dictionaries.push(Some((dict, col.constraint.get_type())));
				continue;
			}
			storage_types.push(col.constraint.get_type());
			dictionaries.push(None);
		}

		let system_columns = source_system_columns(false, queue.def().time != TimeSource::None, false);
		let headers = scan_headers(queue.columns().iter().map(|col| col.name.as_str()), &system_columns);

		Ok(Self {
			queue,
			headers,
			shape: None,
			storage_types,
			dictionaries,
			last_key: None,
			exhausted: false,
			context: Some(context),
			system_columns,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = EncodedQueueRow::view(first).fingerprint();
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

	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::range_open")]
	fn open_range<'rx, 'tx>(
		rx: &'rx mut Transaction<'tx>,
		range: TaggedKeyBoundRange,
		batch_size: u64,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + 'rx>> {
		rx.range_rev(range, RangeScope::All, batch_size as usize)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::drain")]
	fn drain_batch(
		stream: &mut dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>>,
		batch_size: u64,
	) -> Result<DrainedBatch> {
		let mut batch: Vec<EncodedBytes> = Vec::new();
		let mut row_numbers: Vec<RowNumber> = Vec::new();
		let mut new_last_key = None;
		let mut drained = false;

		for _ in 0..batch_size {
			match stream.next() {
				Some(Ok(multi)) => {
					if let TaggedKey::Row(key) = &multi.key {
						row_numbers.push(key.row);
						batch.push(multi.bytes);
						new_last_key = Some(multi.key);
					}
				}
				Some(Err(e)) => return Err(e),
				None => {
					drained = true;
					break;
				}
			}
		}

		Ok((batch, row_numbers, new_last_key, drained))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::column_alloc")]
	fn storage_columns(&self, shape: &RowShape, declared: usize) -> Result<Vec<(FieldRef, ArrayRef)>> {
		let mut storage_columns: Vec<(FieldRef, ArrayRef)> = self
			.queue
			.columns()
			.iter()
			.enumerate()
			.map(|(idx, col)| {
				ColumnBuilder::with_capacity(self.storage_types[idx].clone(), 0).finish(&col.name)
			})
			.collect();

		for index in declared..shape.field_count() {
			let field = shape.get_field(index).ok_or_else(|| {
				internal_error!("queue {} shape lost field {}", self.queue.def().name, index)
			})?;
			storage_columns
				.push(ColumnBuilder::with_capacity(field.constraint.get_type(), 0).finish(&field.name));
		}

		Ok(storage_columns)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::append_rows")]
	fn append_batch(
		shape: &RowShape,
		columns: RecordBatch,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<RecordBatch> {
		append_rows(columns, shape, bytes_vec, row_numbers)
	}

	fn enqueue_order_range(&self) -> TaggedKeyBoundRange {
		RowKeyRange::scan_range(self.queue.def().id.into(), None).resume_before(self.last_key.as_ref())
	}

	fn empty_declared_columns(&self) -> Result<RecordBatch> {
		empty_scan(
			self.queue
				.columns()
				.iter()
				.map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), 0).finish(&col.name))
				.collect(),
			&self.system_columns,
		)
	}
}

impl QueryNode for QueueScan {
	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::queue::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		if self.exhausted {
			return Ok(None);
		}

		let batch_size = self.context.as_ref().expect("QueueScan context not set").batch_size;
		let range = self.enqueue_order_range();

		let (batch_rows, row_numbers, new_last_key, drained) = {
			let mut stream = Self::open_range(rx, range, batch_size)?;
			Self::drain_batch(&mut stream, batch_size)?
		};

		if drained {
			self.exhausted = true;
		}

		if batch_rows.is_empty() {
			self.exhausted = true;
			if self.last_key.is_none() {
				return Ok(Some(self.empty_declared_columns()?));
			}
			return Ok(None);
		}

		self.last_key = new_last_key;

		let shape = self.get_or_load_shape(rx, &batch_rows[0])?;
		let declared = self.queue.columns().len();

		let storage_columns = self.storage_columns(&shape, declared)?;

		let columns = batch(storage_columns)?;
		let columns = Self::append_batch(&shape, columns, batch_rows, row_numbers)?;

		let columns = decode_dictionary_columns(columns, &self.dictionaries, rx)?;

		let mut user = user_pairs(&columns);
		user.truncate(declared);

		Ok(Some(with_user_columns(user, &columns)?))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
