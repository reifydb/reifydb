// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter, sync::Arc};

use arrow_array::{RecordBatch, UInt64Array};
use reifydb_codec::row::{
	bytes::{EncodedBytes, read_fingerprint},
	shape::RowShape,
};
use reifydb_core::{
	common::TimeSource,
	interface::{catalog::storage::StorageId, resolved::ResolvedObject},
	internal_err, internal_error,
	key::row::RowKey,
	value::{
		batch::{append_rows, empty_batch, empty_for},
		column::headers::ColumnHeaders,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	reifydb_assertions,
	value::{
		row_number::RowNumber,
		system_columns::{SystemColumn, with_system_column},
		value_type::ValueType,
	},
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode},
		scan::{empty_scan, guard_view_read, scan_headers, source_system_columns},
		user_pairs,
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
	system_columns: Vec<SystemColumn>,
	shape: Option<RowShape>,
	exhausted: bool,
}

impl RowPointLookupNode {
	pub fn new(source: ResolvedObject, row_number: u64, context: Arc<QueryContext>) -> Result<Self> {
		let system_columns = lookup_system_columns(&source);
		let (headers, _) = build_headers_and_storage_types(&source, &system_columns)?;

		Ok(Self {
			source,
			row_number,
			context: Some(context),
			headers,
			system_columns,
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
		columns: RecordBatch,
		bytes: EncodedBytes,
	) -> Result<RecordBatch> {
		let shape = self.get_or_load_shape(rx, &bytes)?;
		append_rows(columns, &shape, iter::once(bytes), vec![RowNumber(self.row_number)])
	}
}

impl QueryNode for RowPointLookupNode {
	#[instrument(name = "volcano::lookup::point::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_source_read(&self.source, rx, ctx)
	}

	#[instrument(name = "volcano::lookup::point::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		if self.exhausted {
			return Ok(None);
		}
		self.exhausted = true;

		let object_id = get_object_id(&self.source)?;
		let encoded_key = RowKey::new(object_id, RowNumber(self.row_number));

		if let Some(multi_values) = rx.get(&encoded_key)? {
			let columns = columns_from_object(&self.source)?;
			let columns = self.append_batch(rx, columns, multi_values.bytes)?;

			Ok(Some(with_commit_versions(columns, &self.system_columns, vec![multi_values.version.0])?))
		} else {
			Ok(Some(empty_lookup(&self.source, &self.system_columns)?))
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
	system_columns: Vec<SystemColumn>,
	shape: Option<RowShape>,
	current_index: usize,
	emitted: bool,
}

impl RowListLookupNode {
	pub fn new(source: ResolvedObject, row_numbers: Vec<u64>, context: Arc<QueryContext>) -> Result<Self> {
		let system_columns = lookup_system_columns(&source);
		let (headers, _) = build_headers_and_storage_types(&source, &system_columns)?;

		Ok(Self {
			source,
			row_numbers,
			context: Some(context),
			headers,
			system_columns,
			shape: None,
			current_index: 0,
			emitted: false,
		})
	}

	fn finish(&mut self) -> Result<Option<RecordBatch>> {
		if self.emitted {
			return Ok(None);
		}
		self.emitted = true;
		Ok(Some(empty_lookup(&self.source, &self.system_columns)?))
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
	) -> Result<(Vec<EncodedBytes>, Vec<RowNumber>, Vec<u64>)> {
		let mut batch = Vec::new();
		let mut found_row_numbers = Vec::new();
		let mut commit_versions = Vec::new();

		for &row_num in &self.row_numbers[start..end] {
			let encoded_key = RowKey::new(object_id, RowNumber(row_num));

			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch.push(multi_values.bytes);
				found_row_numbers.push(RowNumber(row_num));
				commit_versions.push(multi_values.version.0);
			}
		}

		Ok((batch, found_row_numbers, commit_versions))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::lookup::list::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: RecordBatch,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<RecordBatch> {
		let shape = self.get_or_load_shape(rx, &bytes_vec[0])?;
		append_rows(columns, &shape, bytes_vec, row_numbers)
	}
}

impl QueryNode for RowListLookupNode {
	#[instrument(name = "volcano::lookup::list::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_source_read(&self.source, rx, ctx)
	}

	#[instrument(name = "volcano::lookup::list::next", level = "trace", skip_all)]
	#[allow(clippy::only_used_in_recursion)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.current_index >= self.row_numbers.len() {
			return self.finish();
		}

		let object_id = get_object_id(&self.source)?;
		let end_index = (self.current_index + batch_size).min(self.row_numbers.len());

		let (batch, found_row_numbers, commit_versions) =
			self.fetch_batch(rx, object_id, self.current_index, end_index)?;

		self.current_index = end_index;

		if batch.is_empty() {
			if self.current_index < self.row_numbers.len() {
				return self.next(rx, ctx);
			}
			return self.finish();
		}

		let columns = columns_from_object(&self.source)?;
		let columns = self.append_batch(rx, columns, batch, found_row_numbers)?;

		self.emitted = true;
		Ok(Some(with_commit_versions(columns, &self.system_columns, commit_versions)?))
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
	system_columns: Vec<SystemColumn>,
	shape: Option<RowShape>,
	current_row: u64,
	exhausted: bool,
	emitted: bool,
}

impl RowRangeScanNode {
	pub fn new(source: ResolvedObject, start: u64, end: u64, context: Arc<QueryContext>) -> Result<Self> {
		let system_columns = lookup_system_columns(&source);
		let (headers, _) = build_headers_and_storage_types(&source, &system_columns)?;

		Ok(Self {
			source,
			start,
			end,
			context: Some(context),
			headers,
			system_columns,
			shape: None,
			current_row: start,
			exhausted: false,
			emitted: false,
		})
	}

	fn finish(&mut self) -> Result<Option<RecordBatch>> {
		if self.emitted {
			return Ok(None);
		}
		self.emitted = true;
		Ok(Some(empty_lookup(&self.source, &self.system_columns)?))
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
	) -> Result<(Vec<EncodedBytes>, Vec<RowNumber>, Vec<u64>)> {
		let mut batch = Vec::new();
		let mut found_row_numbers = Vec::new();
		let mut commit_versions = Vec::new();

		for row_num in start..=end {
			let encoded_key = RowKey::new(object_id, RowNumber(row_num));

			if let Some(multi_values) = rx.get(&encoded_key)? {
				batch.push(multi_values.bytes);
				found_row_numbers.push(RowNumber(row_num));
				commit_versions.push(multi_values.version.0);
			}
		}

		Ok((batch, found_row_numbers, commit_versions))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::scan::range::append_rows")]
	fn append_batch<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		columns: RecordBatch,
		bytes_vec: Vec<EncodedBytes>,
		row_numbers: Vec<RowNumber>,
	) -> Result<RecordBatch> {
		let shape = self.get_or_load_shape(rx, &bytes_vec[0])?;
		append_rows(columns, &shape, bytes_vec, row_numbers)
	}
}

impl QueryNode for RowRangeScanNode {
	#[instrument(name = "volcano::scan::range::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		guard_source_read(&self.source, rx, ctx)
	}

	#[instrument(name = "volcano::scan::range::next", level = "trace", skip_all)]
	#[allow(clippy::only_used_in_recursion)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		let stored_ctx = self.context.as_ref().unwrap();
		let batch_size = stored_ctx.batch_size as usize;

		if self.exhausted || self.current_row > self.end {
			return self.finish();
		}

		let object_id = get_object_id(&self.source)?;
		let batch_end = (self.current_row + batch_size as u64 - 1).min(self.end);

		let (batch, found_row_numbers, commit_versions) =
			self.fetch_batch(rx, object_id, self.current_row, batch_end)?;

		self.current_row = batch_end + 1;
		if self.current_row > self.end {
			self.exhausted = true;
		}

		if batch.is_empty() {
			if !self.exhausted {
				return self.next(rx, ctx);
			}
			return self.finish();
		}

		let columns = columns_from_object(&self.source)?;
		let columns = self.append_batch(rx, columns, batch, found_row_numbers)?;

		self.emitted = true;
		Ok(Some(with_commit_versions(columns, &self.system_columns, commit_versions)?))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}

fn build_headers_and_storage_types(
	source: &ResolvedObject,
	system_columns: &[SystemColumn],
) -> Result<(ColumnHeaders, Vec<ValueType>)> {
	let columns = match source {
		ResolvedObject::Table(table) => table.columns(),
		ResolvedObject::View(view) => view.columns(),
		ResolvedObject::RingBuffer(rb) => rb.columns(),
		_ => {
			unreachable!("Row lookup not supported for this source type");
		}
	};

	let storage_types = columns.iter().map(|c| c.constraint.get_type()).collect::<Vec<_>>();

	let headers = scan_headers(columns.iter().map(|col| col.name.as_str()), system_columns);

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

fn columns_from_object(source: &ResolvedObject) -> Result<RecordBatch> {
	Ok(match source {
		ResolvedObject::Table(table) => empty_for(table.columns())?,
		ResolvedObject::View(view) => empty_for(view.columns())?,
		ResolvedObject::RingBuffer(rb) => empty_for(rb.columns())?,
		_ => empty_batch(),
	})
}

fn lookup_system_columns(source: &ResolvedObject) -> Vec<SystemColumn> {
	match source {
		ResolvedObject::Table(table) => {
			source_system_columns(false, table.def().time != TimeSource::None, true)
		}
		ResolvedObject::View(_) => source_system_columns(false, true, false),
		ResolvedObject::RingBuffer(rb) => {
			source_system_columns(false, rb.def().time != TimeSource::None, false)
		}
		_ => Vec::new(),
	}
}

fn empty_lookup(source: &ResolvedObject, system_columns: &[SystemColumn]) -> Result<RecordBatch> {
	empty_scan(user_pairs(&columns_from_object(source)?), system_columns)
}

fn with_commit_versions(
	columns: RecordBatch,
	system_columns: &[SystemColumn],
	commit_versions: Vec<u64>,
) -> Result<RecordBatch> {
	if !system_columns.contains(&SystemColumn::CommitVersion) {
		return Ok(columns);
	}
	with_system_column(columns, SystemColumn::CommitVersion, Arc::new(UInt64Array::from(commit_versions)))
}
