// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter, sync::Arc};

use reifydb_codec::row::{
	bytes::{EncodedBytes, read_fingerprint},
	shape::RowShape,
};
use reifydb_core::{
	interface::{catalog::storage::StorageId, resolved::ResolvedObject},
	internal_err, internal_error,
	key::row::RowKey,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{row_number::RowNumber, value_type::ValueType},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode},
		scan::guard_view_read,
	},
};

fn guard_source_read(source: &ResolvedObject, rx: &mut Transaction<'_>, ctx: &QueryContext) -> Result<()> {
	reifydb_assertions! {
		assert!(
			!matches!(source, ResolvedObject::DeferredView(_) | ResolvedObject::TransactionalView(_)),
			"physical planning must fold view kinds into ResolvedObject::View before row lookup, otherwise guard_view_read silently no-ops here"
		);
	}
	if let ResolvedObject::View(view) = source {
		guard_view_read(view, rx, &ctx.services)?;
	}
	Ok(())
}

pub(crate) struct RowPointLookupNode {
	source: ResolvedObject,
	row_number: u64,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	exhausted: bool,
}

impl RowPointLookupNode {
	pub fn new(source: ResolvedObject, row_number: u64, context: Arc<QueryContext>) -> Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			row_number,
			context: Some(context),
			headers,
			shape: None,
			exhausted: false,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = read_fingerprint(first);

		let stored_ctx = self.context.as_ref().expect("RowPointLookupNode context not set");
		let shape =
			stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
				internal_error!("RowShape with fingerprint {:?} not found", fingerprint)
			})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::lookup::point::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: &mut Columns,
		bytes: EncodedBytes,
	) -> Result<()> {
		let shape = self.get_or_load_shape(rx, &bytes)?;
		columns.append_rows(&shape, iter::once(bytes), vec![RowNumber(self.row_number)])?;
		Ok(())
	}
}

impl QueryNode for RowPointLookupNode {
	#[instrument(name = "volcano::lookup::point::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_source_read(&self.source, rx, ctx)
	}

	#[instrument(name = "volcano::lookup::point::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.exhausted {
			return Ok(None);
		}
		self.exhausted = true;

		let object_id = get_object_id(&self.source)?;
		let encoded_key = RowKey::encoded(object_id, RowNumber(self.row_number));

		if let Some(multi_values) = rx.get(&encoded_key)? {
			let mut columns = columns_from_object(&self.source);
			self.append_batch(rx, &mut columns, multi_values.bytes)?;

			Ok(Some(columns))
		} else {
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

pub(crate) struct RowListLookupNode {
	source: ResolvedObject,
	row_numbers: Vec<u64>,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	current_index: usize,
}

impl RowListLookupNode {
	pub fn new(source: ResolvedObject, row_numbers: Vec<u64>, context: Arc<QueryContext>) -> Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			row_numbers,
			context: Some(context),
			headers,
			shape: None,
			current_index: 0,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = read_fingerprint(first);

		let stored_ctx = self.context.as_ref().expect("RowListLookupNode context not set");
		let shape =
			stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
				internal_error!("RowShape with fingerprint {:?} not found", fingerprint)
			})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::lookup::list::fetch")]
	fn fetch_batch<'a>(
		&self,
		rx: &mut Transaction<'a>,
		object_id: StorageId,
		start: usize,
		end: usize,
	) -> Result<(Vec<EncodedBytes>, Vec<RowNumber>)> {
		let mut batch = Vec::new();
		let mut found_row_numbers = Vec::new();

		for &row_num in &self.row_numbers[start..end] {
			let encoded_key = RowKey::encoded(object_id, RowNumber(row_num));

			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch.push(multi_values.bytes);
				found_row_numbers.push(RowNumber(row_num));
			}
		}

		Ok((batch, found_row_numbers))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::lookup::list::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: &mut Columns,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<()> {
		let shape = self.get_or_load_shape(rx, &bytes_vec[0])?;
		columns.append_rows(&shape, bytes_vec.into_iter(), row_numbers)?;
		Ok(())
	}
}

impl QueryNode for RowListLookupNode {
	#[instrument(name = "volcano::lookup::list::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_source_read(&self.source, rx, ctx)
	}

	#[instrument(name = "volcano::lookup::list::next", level = "trace", skip_all)]
	#[allow(clippy::only_used_in_recursion)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.current_index >= self.row_numbers.len() {
			return Ok(None);
		}

		let object_id = get_object_id(&self.source)?;
		let end_index = (self.current_index + batch_size).min(self.row_numbers.len());

		let (batch, found_row_numbers) = self.fetch_batch(rx, object_id, self.current_index, end_index)?;

		self.current_index = end_index;

		if batch.is_empty() {
			if self.current_index < self.row_numbers.len() {
				return self.next(rx, ctx);
			}
			return Ok(None);
		}

		let mut columns = columns_from_object(&self.source);
		self.append_batch(rx, &mut columns, batch, found_row_numbers)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

pub(crate) struct RowRangeScanNode {
	source: ResolvedObject,
	#[allow(dead_code)]
	start: u64,
	end: u64,
	context: Option<Arc<QueryContext>>,
	headers: ColumnHeaders,
	shape: Option<RowShape>,
	current_row: u64,
	exhausted: bool,
}

impl RowRangeScanNode {
	pub fn new(source: ResolvedObject, start: u64, end: u64, context: Arc<QueryContext>) -> Result<Self> {
		let (headers, _) = build_headers_and_storage_types(&source)?;

		Ok(Self {
			source,
			start,
			end,
			context: Some(context),
			headers,
			shape: None,
			current_row: start,
			exhausted: false,
		})
	}

	fn get_or_load_shape(&mut self, rx: &mut Transaction, first: &EncodedBytes) -> Result<RowShape> {
		if let Some(shape) = &self.shape {
			return Ok(shape.clone());
		}

		let fingerprint = read_fingerprint(first);

		let stored_ctx = self.context.as_ref().expect("RowRangeScanNode context not set");
		let shape =
			stored_ctx.services.catalog.get_or_load_row_shape(fingerprint, rx)?.ok_or_else(|| {
				internal_error!("RowShape with fingerprint {:?} not found", fingerprint)
			})?;

		self.shape = Some(shape.clone());

		Ok(shape)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::range::fetch")]
	fn fetch_batch<'a>(
		&self,
		rx: &mut Transaction<'a>,
		object_id: StorageId,
		start: u64,
		end: u64,
	) -> Result<(Vec<EncodedBytes>, Vec<RowNumber>)> {
		let mut batch = Vec::new();
		let mut found_row_numbers = Vec::new();

		for row_num in start..=end {
			let encoded_key = RowKey::encoded(object_id, RowNumber(row_num));

			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch.push(multi_values.bytes);
				found_row_numbers.push(RowNumber(row_num));
			}
		}

		Ok((batch, found_row_numbers))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::range::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: &mut Columns,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<()> {
		let shape = self.get_or_load_shape(rx, &bytes_vec[0])?;
		columns.append_rows(&shape, bytes_vec.into_iter(), row_numbers)?;
		Ok(())
	}
}

impl QueryNode for RowRangeScanNode {
	#[instrument(name = "volcano::scan::range::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_source_read(&self.source, rx, ctx)
	}

	#[instrument(name = "volcano::scan::range::next", level = "trace", skip_all)]
	#[allow(clippy::only_used_in_recursion)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.exhausted || self.current_row > self.end {
			return Ok(None);
		}

		let object_id = get_object_id(&self.source)?;
		let batch_end = (self.current_row + batch_size as u64 - 1).min(self.end);

		let (batch, found_row_numbers) = self.fetch_batch(rx, object_id, self.current_row, batch_end)?;

		self.current_row = batch_end + 1;
		if self.current_row > self.end {
			self.exhausted = true;
		}

		if batch.is_empty() {
			if !self.exhausted {
				return self.next(rx, ctx);
			}
			return Ok(None);
		}

		let mut columns = columns_from_object(&self.source);
		self.append_batch(rx, &mut columns, batch, found_row_numbers)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

fn build_headers_and_storage_types(source: &ResolvedObject) -> Result<(ColumnHeaders, Vec<ValueType>)> {
	let columns = match source {
		ResolvedObject::Table(table) => table.columns(),
		ResolvedObject::View(view) => view.columns(),
		ResolvedObject::RingBuffer(rb) => rb.columns(),
		_ => {
			unreachable!("Row lookup not supported for this source type");
		}
	};

	let storage_types = columns.iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();

	let headers = ColumnHeaders {
		columns: columns.iter().map(|col| Fragment::internal(&col.name)).collect(),
	};

	Ok((headers, storage_types))
}

fn get_object_id(source: &ResolvedObject) -> Result<StorageId> {
	match source {
		ResolvedObject::Table(table) => Ok(table.def().id.into()),
		ResolvedObject::View(view) => Ok(view.def().storage_id()),
		ResolvedObject::RingBuffer(rb) => Ok(rb.def().id.into()),
		_ => internal_err!("Row lookup not supported for this source type"),
	}
}

fn columns_from_object(source: &ResolvedObject) -> Columns {
	match source {
		ResolvedObject::Table(table) => Columns::from_catalog_columns(table.columns()),
		ResolvedObject::View(view) => Columns::from_catalog_columns(view.columns()),
		ResolvedObject::RingBuffer(rb) => Columns::from_catalog_columns(rb.columns()),
		_ => Columns::empty(),
	}
}
