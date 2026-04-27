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
		table::Table,
	},
	internal_error,
	key::{EncodableKey, index_entry::IndexEntryKey},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_type::{
	fragment::Fragment,
	value::{Value, identity::IdentityId, row_number::RowNumber, r#type::Type},
};

use super::{
	BulkInsertResult, RingBufferInsertResult, TableInsertResult,
	validation::{
		reorder_rows_unvalidated, reorder_rows_unvalidated_rb, validate_and_coerce_rows,
		validate_and_coerce_rows_rb,
	},
};
use crate::{
	Result,
	bulk_insert::primitive::{
		ringbuffer::{PendingRingBufferInsert, RingBufferInsertBuilder},
		table::{PendingTableInsert, TableInsertBuilder},
	},
	engine::StandardEngine,
	transaction::operation::{
		dictionary::DictionaryOperations, ringbuffer::RingBufferOperations, table::TableOperations,
	},
	vm::instruction::dml::{
		primary_key,
		shape::{get_or_create_ringbuffer_shape, get_or_create_table_shape},
	},
};

/// Marker trait for validation mode (sealed)
pub trait ValidationMode: sealed::Sealed + 'static {
	/// Whether this mode performs full type checking and constraint validation.
	const VALIDATED: bool;

	/// Run `body` inside a transaction in this mode and commit. `Unchecked`
	/// routes through `execute_bulk_unchecked`, which disables conflict tracking
	/// and commits via the bypass path; the others reserve the write-set hint
	/// (when `total_rows > 0`) and commit through the standard path.
	fn run<F, R>(txn: &mut CommandTransaction, total_rows: usize, body: F) -> Result<R>
	where
		F: FnOnce(&mut CommandTransaction) -> Result<R>;
}

/// Validated mode - performs full type checking and constraint validation
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

/// Unchecked mode - skips validation AND skips registering the commit in the
/// oracle's per-key conflict-detection index. Used by `bulk_insert_unchecked`.
/// See that method's doc for the safety contract.
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
	// Pre-size the conflict-tracker write set so a known-size bulk insert doesn't
	// rehash its HashSet thousands of times. Each row produces one row write plus
	// up to one primary-index write, so reserve 2x the row total.
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

/// Main builder for bulk insert operations.
///
/// Type parameter `V` tracks the validation mode at compile time.
pub struct BulkInsertBuilder<'e, V: ValidationMode = Validated> {
	engine: &'e StandardEngine,
	identity: IdentityId,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
	_validation: PhantomData<V>,
}

impl<'e> BulkInsertBuilder<'e, Validated> {
	/// Create a new bulk insert builder with full validation enabled.
	pub(crate) fn new(engine: &'e StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e> BulkInsertBuilder<'e, Unchecked> {
	/// Create a new bulk insert builder with validation AND oracle conflict
	/// tracking disabled (unchecked mode).
	pub(crate) fn new_unchecked(engine: &'e StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e, V: ValidationMode> BulkInsertBuilder<'e, V> {
	/// Begin inserting into a table.
	///
	/// The qualified name can be either "namespace::table" or just "table"
	/// (which uses the default namespace).
	pub fn table<'a>(&'a mut self, qualified_name: &str) -> TableInsertBuilder<'a, 'e, V> {
		let (namespace, table) = parse_qualified_name(qualified_name);
		TableInsertBuilder::new(self, namespace, table)
	}

	/// Begin inserting into a ring buffer.
	///
	/// The qualified name can be either "namespace::ringbuffer" or just "ringbuffer"
	/// (which uses the default namespace).
	pub fn ringbuffer<'a>(&'a mut self, qualified_name: &str) -> RingBufferInsertBuilder<'a, 'e, V> {
		let (namespace, ringbuffer) = parse_qualified_name(qualified_name);
		RingBufferInsertBuilder::new(self, namespace, ringbuffer)
	}

	/// Add a pending table insert (called by TableInsertBuilder::done)
	pub(super) fn add_table_insert(&mut self, pending: PendingTableInsert) {
		self.pending_tables.push(pending);
	}

	/// Add a pending ring buffer insert (called by RingBufferInsertBuilder::done)
	pub(super) fn add_ringbuffer_insert(&mut self, pending: PendingRingBufferInsert) {
		self.pending_ringbuffers.push(pending);
	}

	/// Execute all pending inserts in a single transaction.
	///
	/// Returns a summary of what was inserted. On error, the entire
	/// transaction is rolled back (no partial inserts).
	pub fn execute(self) -> Result<BulkInsertResult> {
		self.engine.reject_if_read_only()?;
		let mut txn = self.engine.begin_command(self.identity)?;
		let catalog = self.engine.catalog();
		let clock = self.engine.clock();
		let total_rows = self.total_pending_rows();
		let pending_tables = self.pending_tables;
		let pending_ringbuffers = self.pending_ringbuffers;

		V::run(&mut txn, total_rows, move |txn| {
			run_all_pending::<V>(catalog, clock, txn, pending_tables, pending_ringbuffers)
		})
	}

	#[inline]
	fn total_pending_rows(&self) -> usize {
		self.pending_tables.iter().map(|p| p.rows.len()).sum::<usize>()
			+ self.pending_ringbuffers.iter().map(|p| p.rows.len()).sum::<usize>()
	}
}

#[inline]
fn run_all_pending<V: ValidationMode>(
	catalog: Catalog,
	clock: &Clock,
	txn: &mut CommandTransaction,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
) -> Result<BulkInsertResult> {
	let mut result = BulkInsertResult::default();
	for pending in pending_tables {
		result.tables.push(execute_table_insert::<V>(&catalog, txn, &pending, clock)?);
	}
	for pending in pending_ringbuffers {
		result.ringbuffers.push(execute_ringbuffer_insert::<V>(&catalog, txn, &pending, clock)?);
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
	let coerced_rows = if V::VALIDATED {
		validate_and_coerce_rows(&pending.rows, table)?
	} else {
		reorder_rows_unvalidated(&pending.rows, table)?
	};

	let mut encoded_rows = Vec::with_capacity(coerced_rows.len());

	for mut values in coerced_rows {
		fill_auto_increment_table(catalog, txn, table, &mut values)?;
		dictionary_encode_table(catalog, txn, table, &mut values)?;

		if V::VALIDATED {
			for (idx, col) in table.columns.iter().enumerate() {
				col.constraint.validate(&values[idx])?;
			}
		}

		encoded_rows.push(encode_row(shape, &values, clock));
	}

	Ok(encoded_rows)
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

/// Parse a qualified name like "namespace::table" into (namespace, name).
/// If no namespace is provided, uses "default".
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
