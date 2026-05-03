// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::marker::PhantomData;

use reifydb_catalog::{
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	error::CoreError,
	interface::catalog::{
		id::IndexId,
		key::PrimaryKey,
		ringbuffer::{RingBuffer, RingBufferMetadata},
		series::{Series, SeriesMetadata},
		table::Table,
	},
	internal_error,
	key::{EncodableKey, index_entry::IndexEntryKey, series_row::SeriesRowKey},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::series_row::SeriesRowInterceptor,
	transaction::{Transaction, command::CommandTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, row_number::RowNumber, r#type::Type},
};

use super::{
	BulkInsertResult, RingBufferInsertResult, SeriesInsertResult, TableInsertResult,
	validation::{
		reorder_rows_unvalidated, reorder_rows_unvalidated_rb, reorder_rows_unvalidated_series,
		validate_and_coerce_rows, validate_and_coerce_rows_rb, validate_and_coerce_rows_series,
	},
};
use crate::{
	Result,
	bulk_insert::primitive::{
		ringbuffer::{PendingRingBufferInsert, RingBufferInsertBuilder},
		series::{PendingSeriesInsert, SeriesInsertBuilder},
		table::{PendingTableInsert, TableInsertBuilder},
	},
	engine::StandardEngine,
	transaction::operation::{
		dictionary::DictionaryOperations, ringbuffer::RingBufferOperations, table::TableOperations,
	},
	vm::instruction::dml::{
		primary_key,
		shape::{get_or_create_ringbuffer_shape, get_or_create_series_shape, get_or_create_table_shape},
	},
};

pub trait ValidationMode: sealed::Sealed + 'static {
	const VALIDATED: bool;

	fn run<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>;
}

pub struct Validated;
impl ValidationMode for Validated {
	const VALIDATED: bool = true;

	fn run<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>,
	{
		run_checked(txn, total_rows, body)
	}
}

pub struct Unchecked;
impl ValidationMode for Unchecked {
	const VALIDATED: bool = false;

	fn run<F, R>(txn: &mut CommandTransaction, _total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>,
	{
		txn.execute_bulk_unchecked(body)
	}
}

fn run_checked<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
where
	F: FnOnce(&mut CommandTransaction) -> Result<R>,
{
	if total_rows > 0 {
		txn.reserve_writes(total_rows.saturating_mul(2))?;
	}
	let r = body(txn)?;
	txn.commit()?;
	Ok(r)
}

pub mod sealed {

	use super::{Unchecked, Validated};
	pub trait Sealed {}
	impl Sealed for Validated {}
	impl Sealed for Unchecked {}
}

pub struct BulkInsertBuilder<'e, V: ValidationMode = Validated> {
	engine: &'e StandardEngine,
	identity: IdentityId,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
	pending_series: Vec<PendingSeriesInsert>,
	_validation: PhantomData<V>,
}

impl<'e> BulkInsertBuilder<'e, Validated> {
	pub(crate) fn new(engine: &'e StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			pending_series: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e> BulkInsertBuilder<'e, Unchecked> {
	pub(crate) fn new_unchecked(engine: &'e StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			pending_series: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e, V: ValidationMode> BulkInsertBuilder<'e, V> {
	pub fn table<'a>(&'a mut self, qualified_name: &str) -> TableInsertBuilder<'a, 'e, V> {
		let (namespace, table) = parse_qualified_name(qualified_name);
		TableInsertBuilder::new(self, namespace, table)
	}

	pub fn ringbuffer<'a>(&'a mut self, qualified_name: &str) -> RingBufferInsertBuilder<'a, 'e, V> {
		let (namespace, ringbuffer) = parse_qualified_name(qualified_name);
		RingBufferInsertBuilder::new(self, namespace, ringbuffer)
	}

	pub fn series<'a>(&'a mut self, qualified_name: &str) -> SeriesInsertBuilder<'a, 'e, V> {
		let (namespace, series) = parse_qualified_name(qualified_name);
		SeriesInsertBuilder::new(self, namespace, series)
	}

	pub(super) fn add_table_insert(&mut self, pending: PendingTableInsert) {
		self.pending_tables.push(pending);
	}

	pub(super) fn add_ringbuffer_insert(&mut self, pending: PendingRingBufferInsert) {
		self.pending_ringbuffers.push(pending);
	}

	pub(super) fn add_series_insert(&mut self, pending: PendingSeriesInsert) {
		self.pending_series.push(pending);
	}

	pub fn execute(self) -> Result<BulkInsertResult> {
		self.engine.reject_if_read_only()?;
		let mut txn = self.engine.begin_command(self.identity)?;
		let catalog = self.engine.catalog();
		let clock = self.engine.clock();
		let total_rows = self.total_pending_rows();
		let pending_tables = self.pending_tables;
		let pending_ringbuffers = self.pending_ringbuffers;
		let pending_series = self.pending_series;

		V::run(&mut txn, total_rows, move |txn| {
			run_all_pending::<V>(catalog, clock, txn, pending_tables, pending_ringbuffers, pending_series)
		})
	}

	#[inline]
	fn total_pending_rows(&self) -> usize {
		self.pending_tables.iter().map(|p| p.rows.len()).sum::<usize>()
			+ self.pending_ringbuffers.iter().map(|p| p.rows.len()).sum::<usize>()
			+ self.pending_series.iter().map(|p| p.rows.len()).sum::<usize>()
	}
}

#[inline]
fn run_all_pending<V: ValidationMode>(
	catalog: Catalog,
	clock: &Clock,
	txn: &mut CommandTransaction,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
	pending_series: Vec<PendingSeriesInsert>,
) -> Result<BulkInsertResult> {
	let mut result = BulkInsertResult::default();
	for pending in pending_tables {
		result.tables.push(execute_table_insert::<V>(&catalog, txn, &pending, clock)?);
	}
	for pending in pending_ringbuffers {
		result.ringbuffers.push(execute_ringbuffer_insert::<V>(&catalog, txn, &pending, clock)?);
	}
	for pending in pending_series {
		result.series.push(execute_series_insert::<V>(&catalog, txn, &pending, clock)?);
	}
	Ok(result)
}

fn execute_table_insert<V: ValidationMode>(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingTableInsert,
	clock: &Clock,
) -> Result<TableInsertResult> {
	let table = resolve_table(catalog, txn, pending)?;
	let shape = get_or_create_table_shape(catalog, &table, &mut Transaction::Command(txn))?;
	let encoded_rows = encode_table_rows::<V>(catalog, txn, pending, &table, &shape, clock)?;
	if encoded_rows.is_empty() {
		return Ok(empty_table_result(pending));
	}
	write_table_rows(catalog, txn, &table, &shape, pending, encoded_rows)
}

#[inline]
fn empty_table_result(pending: &PendingTableInsert) -> TableInsertResult {
	TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: 0,
	}
}

#[inline]
fn write_table_rows(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	table: &Table,
	shape: &RowShape,
	pending: &PendingTableInsert,
	encoded_rows: Vec<EncodedRow>,
) -> Result<TableInsertResult> {
	let total_rows = encoded_rows.len();
	let row_numbers = catalog.next_row_number_batch(txn, table.id, total_rows as u64)?;
	let pk_def = primary_key::get_primary_key(catalog, &mut Transaction::Command(txn), table)?;
	let row_number_shape = pk_def.as_ref().map(|_| RowShape::testing(&[Type::Uint8]));

	for (row, &row_number) in encoded_rows.iter().zip(row_numbers.iter()) {
		txn.insert_table(table, shape, row.clone(), row_number)?;

		if let Some(ref pk_def) = pk_def {
			write_primary_key_index(
				txn,
				table,
				shape,
				pk_def,
				row,
				row_number,
				row_number_shape.as_ref().unwrap(),
			)?;
		}
	}

	Ok(TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: total_rows as u64,
	})
}

fn resolve_table(catalog: &Catalog, txn: &mut CommandTransaction, pending: &PendingTableInsert) -> Result<Table> {
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(txn), &pending.namespace)?
		.ok_or_else(|| CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: pending.namespace.to_string(),
			name: String::new(),
			fragment: Fragment::None,
		})?;

	catalog.find_table_by_name(&mut Transaction::Command(txn), namespace.id(), &pending.table)?.ok_or_else(|| {
		CatalogError::NotFound {
			kind: CatalogObjectKind::Table,
			namespace: pending.namespace.to_string(),
			name: pending.table.to_string(),
			fragment: Fragment::None,
		}
		.into()
	})
}

fn encode_table_rows<V: ValidationMode>(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingTableInsert,
	table: &Table,
	shape: &RowShape,
	clock: &Clock,
) -> Result<Vec<EncodedRow>> {
	let coerced_rows = coerce_table_rows::<V>(&pending.rows, table)?;
	let mut encoded_rows = Vec::with_capacity(coerced_rows.len());
	for values in coerced_rows {
		encoded_rows.push(prepare_table_row::<V>(catalog, txn, table, shape, clock, values)?);
	}
	Ok(encoded_rows)
}

#[inline]
fn coerce_table_rows<V: ValidationMode>(rows: &[Params], table: &Table) -> Result<Vec<Vec<Value>>> {
	if V::VALIDATED {
		validate_and_coerce_rows(rows, table)
	} else {
		reorder_rows_unvalidated(rows, table)
	}
}

#[inline]
fn prepare_table_row<V: ValidationMode>(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	table: &Table,
	shape: &RowShape,
	clock: &Clock,
	mut values: Vec<Value>,
) -> Result<EncodedRow> {
	fill_auto_increment_table(catalog, txn, table, &mut values)?;
	dictionary_encode_table(catalog, txn, table, &mut values)?;
	if V::VALIDATED {
		validate_table_constraints(table, &values)?;
	}
	Ok(encode_row(shape, &values, clock))
}

#[inline]
fn validate_table_constraints(table: &Table, values: &[Value]) -> Result<()> {
	for (idx, col) in table.columns.iter().enumerate() {
		col.constraint.validate(&values[idx])?;
	}
	Ok(())
}

fn fill_auto_increment_table(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	table: &Table,
	values: &mut [Value],
) -> Result<()> {
	for (idx, col) in table.columns.iter().enumerate() {
		if col.auto_increment && matches!(values[idx], Value::None { .. }) {
			values[idx] = catalog.column_sequence_next_value(txn, table.id, col.id)?;
		}
	}
	Ok(())
}

fn dictionary_encode_table(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	table: &Table,
	values: &mut [Value],
) -> Result<()> {
	for (idx, col) in table.columns.iter().enumerate() {
		if let Some(dict_id) = col.dictionary_id {
			let dictionary =
				catalog.find_dictionary(&mut Transaction::Command(txn), dict_id)?.ok_or_else(|| {
					internal_error!("Dictionary {:?} not found for column {}", dict_id, col.name)
				})?;
			let entry_id = txn.insert_into_dictionary(&dictionary, &values[idx])?;
			values[idx] = entry_id.to_value();
		}
	}
	Ok(())
}

fn encode_row(shape: &RowShape, values: &[Value], clock: &Clock) -> EncodedRow {
	let mut row = shape.allocate();
	for (idx, value) in values.iter().enumerate() {
		shape.set_value(&mut row, idx, value);
	}
	let now_nanos = clock.now_nanos();
	row.set_timestamps(now_nanos, now_nanos);
	row
}

fn write_primary_key_index(
	txn: &mut CommandTransaction,
	table: &Table,
	shape: &RowShape,
	pk_def: &PrimaryKey,
	row: &EncodedRow,
	row_number: RowNumber,
	row_number_shape: &RowShape,
) -> Result<()> {
	let index_key = primary_key::encode_primary_key(pk_def, row, table, shape)?;
	let index_entry_key = IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key);

	if txn.contains_key(&index_entry_key.encode())? {
		let key_columns = pk_def.columns.iter().map(|c| c.name.clone()).collect();
		return Err(CoreError::PrimaryKeyViolation {
			fragment: Fragment::None,
			table_name: table.name.clone(),
			key_columns,
		}
		.into());
	}

	let mut row_number_encoded = row_number_shape.allocate();
	row_number_shape.set_u64(&mut row_number_encoded, 0, u64::from(row_number));
	txn.set(&index_entry_key.encode(), row_number_encoded)?;
	Ok(())
}

fn execute_ringbuffer_insert<V: ValidationMode>(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingRingBufferInsert,
	clock: &Clock,
) -> Result<RingBufferInsertResult> {
	let ringbuffer = resolve_ringbuffer(catalog, txn, pending)?;
	let mut metadata = load_ringbuffer_metadata(catalog, txn, pending, &ringbuffer)?;
	let shape = get_or_create_ringbuffer_shape(catalog, &ringbuffer, &mut Transaction::Command(txn))?;
	let coerced_rows = coerce_ringbuffer_rows::<V>(pending, &ringbuffer)?;
	let inserted =
		insert_ringbuffer_rows::<V>(catalog, txn, &ringbuffer, &shape, coerced_rows, &mut metadata, clock)?;
	catalog.update_ringbuffer_metadata(txn, metadata)?;
	Ok(RingBufferInsertResult {
		namespace: pending.namespace.clone(),
		ringbuffer: pending.ringbuffer.clone(),
		inserted,
	})
}

#[inline]
fn resolve_ringbuffer(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingRingBufferInsert,
) -> Result<RingBuffer> {
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(txn), &pending.namespace)?
		.ok_or_else(|| CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: pending.namespace.to_string(),
			name: String::new(),
			fragment: Fragment::None,
		})?;

	catalog.find_ringbuffer_by_name(&mut Transaction::Command(txn), namespace.id(), &pending.ringbuffer)?
		.ok_or_else(|| {
			CatalogError::NotFound {
				kind: CatalogObjectKind::RingBuffer,
				namespace: pending.namespace.to_string(),
				name: pending.ringbuffer.to_string(),
				fragment: Fragment::None,
			}
			.into()
		})
}

#[inline]
fn load_ringbuffer_metadata(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingRingBufferInsert,
	ringbuffer: &RingBuffer,
) -> Result<RingBufferMetadata> {
	catalog.find_ringbuffer_metadata(&mut Transaction::Command(txn), ringbuffer.id)?.ok_or_else(|| {
		CatalogError::NotFound {
			kind: CatalogObjectKind::RingBuffer,
			namespace: pending.namespace.to_string(),
			name: pending.ringbuffer.to_string(),
			fragment: Fragment::None,
		}
		.into()
	})
}

#[inline]
fn coerce_ringbuffer_rows<V: ValidationMode>(
	pending: &PendingRingBufferInsert,
	ringbuffer: &RingBuffer,
) -> Result<Vec<Vec<Value>>> {
	if V::VALIDATED {
		validate_and_coerce_rows_rb(&pending.rows, ringbuffer)
	} else {
		reorder_rows_unvalidated_rb(&pending.rows, ringbuffer)
	}
}

fn insert_ringbuffer_rows<V: ValidationMode>(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	coerced_rows: Vec<Vec<Value>>,
	metadata: &mut RingBufferMetadata,
	clock: &Clock,
) -> Result<u64> {
	let mut inserted_count = 0u64;
	for mut values in coerced_rows {
		dict_encode_ringbuffer_row(catalog, txn, ringbuffer, &mut values)?;

		if V::VALIDATED {
			for (idx, col) in ringbuffer.columns.iter().enumerate() {
				col.constraint.validate(&values[idx])?;
			}
		}

		let mut row = shape.allocate();
		for (idx, value) in values.iter().enumerate() {
			shape.set_value(&mut row, idx, value);
		}
		let now_nanos = clock.now_nanos();
		row.set_timestamps(now_nanos, now_nanos);

		evict_oldest_if_full(txn, ringbuffer, metadata)?;

		let row_number = catalog.next_row_number_for_ringbuffer(txn, ringbuffer.id)?;
		txn.insert_ringbuffer_at(ringbuffer, shape, row_number, row)?;

		if metadata.is_empty() {
			metadata.head = row_number.0;
		}
		metadata.count += 1;
		metadata.tail = row_number.0 + 1;

		inserted_count += 1;
	}
	Ok(inserted_count)
}

#[inline]
fn evict_oldest_if_full(
	txn: &mut CommandTransaction,
	ringbuffer: &RingBuffer,
	metadata: &mut RingBufferMetadata,
) -> Result<()> {
	if metadata.is_full() {
		let oldest_row = RowNumber(metadata.head);
		txn.remove_from_ringbuffer(ringbuffer, oldest_row)?;
		metadata.head += 1;
		metadata.count -= 1;
	}
	Ok(())
}

#[inline]
fn dict_encode_ringbuffer_row(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	ringbuffer: &RingBuffer,
	values: &mut [Value],
) -> Result<()> {
	for (idx, col) in ringbuffer.columns.iter().enumerate() {
		if let Some(dict_id) = col.dictionary_id {
			let dictionary =
				catalog.find_dictionary(&mut Transaction::Command(txn), dict_id)?.ok_or_else(|| {
					internal_error!("Dictionary {:?} not found for column {}", dict_id, col.name)
				})?;
			let entry_id = txn.insert_into_dictionary(&dictionary, &values[idx])?;
			values[idx] = entry_id.to_value();
		}
	}
	Ok(())
}

fn execute_series_insert<V: ValidationMode>(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingSeriesInsert,
	clock: &Clock,
) -> Result<SeriesInsertResult> {
	let series = resolve_series(catalog, txn, pending)?;
	let mut metadata = load_series_metadata(catalog, txn, pending, &series)?;
	let shape = get_or_create_series_shape(catalog, &series, &mut Transaction::Command(txn))?;
	let coerced_rows = coerce_series_rows::<V>(pending, &series)?;
	let inserted = insert_series_rows::<V>(txn, &series, &shape, coerced_rows, &mut metadata, clock)?;
	catalog.update_series_metadata_txn(&mut Transaction::Command(txn), metadata)?;
	Ok(SeriesInsertResult {
		namespace: pending.namespace.clone(),
		series: pending.series.clone(),
		inserted,
	})
}

#[inline]
fn resolve_series(catalog: &Catalog, txn: &mut CommandTransaction, pending: &PendingSeriesInsert) -> Result<Series> {
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(txn), &pending.namespace)?
		.ok_or_else(|| CatalogError::NotFound {
			kind: CatalogObjectKind::Namespace,
			namespace: pending.namespace.to_string(),
			name: String::new(),
			fragment: Fragment::None,
		})?;

	catalog.find_series_by_name(&mut Transaction::Command(txn), namespace.id(), &pending.series)?.ok_or_else(|| {
		CatalogError::NotFound {
			kind: CatalogObjectKind::Series,
			namespace: pending.namespace.to_string(),
			name: pending.series.to_string(),
			fragment: Fragment::None,
		}
		.into()
	})
}

#[inline]
fn load_series_metadata(
	catalog: &Catalog,
	txn: &mut CommandTransaction,
	pending: &PendingSeriesInsert,
	series: &Series,
) -> Result<SeriesMetadata> {
	catalog.find_series_metadata(&mut Transaction::Command(txn), series.id)?.ok_or_else(|| {
		CatalogError::NotFound {
			kind: CatalogObjectKind::Series,
			namespace: pending.namespace.to_string(),
			name: pending.series.to_string(),
			fragment: Fragment::None,
		}
		.into()
	})
}

#[inline]
fn coerce_series_rows<V: ValidationMode>(pending: &PendingSeriesInsert, series: &Series) -> Result<Vec<Vec<Value>>> {
	if V::VALIDATED {
		validate_and_coerce_rows_series(&pending.rows, series)
	} else {
		reorder_rows_unvalidated_series(&pending.rows, series)
	}
}

fn insert_series_rows<V: ValidationMode>(
	txn: &mut CommandTransaction,
	series: &Series,
	shape: &RowShape,
	coerced_rows: Vec<Vec<Value>>,
	metadata: &mut SeriesMetadata,
	clock: &Clock,
) -> Result<u64> {
	let key_col_name = series.key.column();
	let key_col_idx =
		series.columns.iter().position(|c| c.name == key_col_name).ok_or_else(|| {
			internal_error!("series {} key column {} not found", series.name, key_col_name)
		})?;

	let mut inserted_count = 0u64;
	for values in coerced_rows {
		if V::VALIDATED {
			for (idx, col) in series.columns.iter().enumerate() {
				col.constraint.validate(&values[idx])?;
			}
		}

		let key_value = series.key_to_u64(values[key_col_idx].clone()).unwrap_or(0);

		metadata.sequence_counter += 1;
		let sequence = metadata.sequence_counter;
		let row_key = SeriesRowKey {
			series: series.id,
			variant_tag: None,
			key: key_value,
			sequence,
		};
		let encoded_key = row_key.encode();

		let row = encode_series_row(series, shape, key_value, &values, key_col_idx, clock);

		let row = SeriesRowInterceptor::pre_insert(txn, series, row)?;
		txn.set(&encoded_key, row.clone())?;
		SeriesRowInterceptor::post_insert(txn, series, &row)?;

		update_series_metadata_for_insert(metadata, key_value);
		inserted_count += 1;
	}
	Ok(inserted_count)
}

#[inline]
fn encode_series_row(
	series: &Series,
	shape: &RowShape,
	key_value: u64,
	values: &[Value],
	key_col_idx: usize,
	clock: &Clock,
) -> EncodedRow {
	let key_value_encoded = series.key_from_u64(key_value);
	let mut row = shape.allocate();
	shape.set_value(&mut row, 0, &key_value_encoded);
	let mut shape_idx = 1;
	for (col_idx, value) in values.iter().enumerate() {
		if col_idx == key_col_idx {
			continue;
		}
		shape.set_value(&mut row, shape_idx, value);
		shape_idx += 1;
	}
	let now_nanos = clock.now_nanos();
	row.set_timestamps(now_nanos, now_nanos);
	row
}

#[inline]
fn update_series_metadata_for_insert(metadata: &mut SeriesMetadata, key_value: u64) {
	if metadata.row_count == 0 {
		metadata.oldest_key = key_value;
		metadata.newest_key = key_value;
	} else {
		if key_value < metadata.oldest_key {
			metadata.oldest_key = key_value;
		}
		if key_value > metadata.newest_key {
			metadata.newest_key = key_value;
		}
	}
	metadata.row_count += 1;
}

fn parse_qualified_name(qualified_name: &str) -> (String, String) {
	if let Some((ns, name)) = qualified_name.rsplit_once("::") {
		(ns.to_string(), name.to_string())
	} else {
		("default".to_string(), qualified_name.to_string())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_qualified_name_simple() {
		assert_eq!(parse_qualified_name("table"), ("default".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_single_namespace() {
		assert_eq!(parse_qualified_name("ns::table"), ("ns".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_nested_namespace() {
		assert_eq!(parse_qualified_name("a::b::table"), ("a::b".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_deeply_nested_namespace() {
		assert_eq!(parse_qualified_name("a::b::c::table"), ("a::b::c".to_string(), "table".to_string()));
	}

	#[test]
	fn parse_qualified_name_empty_string() {
		assert_eq!(parse_qualified_name(""), ("default".to_string(), "".to_string()));
	}
}
