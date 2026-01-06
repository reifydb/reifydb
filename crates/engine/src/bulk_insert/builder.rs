// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::marker::PhantomData;

use reifydb_catalog::{CatalogStore, sequence::RowSequence};
use reifydb_core::{interface::Identity, value::encoded::encode_value};
use reifydb_type::{Fragment, Value};

use super::{
	BulkInsertResult, RingBufferInsertResult, TableInsertResult,
	error::BulkInsertError,
	primitive::{PendingRingBufferInsert, PendingTableInsert, RingBufferInsertBuilder, TableInsertBuilder},
	validation::{
		reorder_rows_trusted, reorder_rows_trusted_rb, validate_and_coerce_rows, validate_and_coerce_rows_rb,
	},
};
use crate::{StandardCommandTransaction, StandardEngine, transaction::operation::RingBufferOperations};

/// Marker trait for validation mode (sealed)
pub trait ValidationMode: sealed::Sealed + 'static {}

/// Validated mode - performs full type checking and constraint validation
pub struct Validated;
impl ValidationMode for Validated {}

/// Trusted mode - skips validation for pre-validated internal data
pub struct Trusted;
impl ValidationMode for Trusted {}

mod sealed {
	pub trait Sealed {}
	impl Sealed for super::Validated {}
	impl Sealed for super::Trusted {}
}

/// Main builder for bulk insert operations.
///
/// Type parameter `V` tracks the validation mode at compile time.
pub struct BulkInsertBuilder<'e, V: ValidationMode = Validated> {
	engine: &'e StandardEngine,
	_identity: &'e Identity,
	pending_tables: Vec<PendingTableInsert>,
	pending_ringbuffers: Vec<PendingRingBufferInsert>,
	_validation: PhantomData<V>,
}

impl<'e> BulkInsertBuilder<'e, Validated> {
	/// Create a new bulk insert builder with full validation enabled.
	pub(crate) fn new(engine: &'e StandardEngine, identity: &'e Identity) -> Self {
		Self {
			engine,
			_identity: identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e> BulkInsertBuilder<'e, Trusted> {
	/// Create a new bulk insert builder with validation disabled (trusted mode).
	pub(crate) fn new_trusted(engine: &'e StandardEngine, identity: &'e Identity) -> Self {
		Self {
			engine,
			_identity: identity,
			pending_tables: Vec::new(),
			pending_ringbuffers: Vec::new(),
			_validation: PhantomData,
		}
	}
}

impl<'e, V: ValidationMode> BulkInsertBuilder<'e, V> {
	/// Begin inserting into a table.
	///
	/// The qualified name can be either "namespace.table" or just "table"
	/// (which uses the default namespace).
	pub fn table<'a>(&'a mut self, qualified_name: &str) -> TableInsertBuilder<'a, 'e, V> {
		let (namespace, table) = parse_qualified_name(qualified_name);
		TableInsertBuilder::new(self, namespace, table)
	}

	/// Begin inserting into a ring buffer.
	///
	/// The qualified name can be either "namespace.ringbuffer" or just "ringbuffer"
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
	pub fn execute(self) -> crate::Result<BulkInsertResult> {
		let mut txn = self.engine.begin_command()?;
		let mut result = BulkInsertResult::default();

		// Process all pending table inserts
		for pending in self.pending_tables {
			let table_result = execute_table_insert::<V>(&mut txn, &pending, std::any::TypeId::of::<V>())?;
			result.tables.push(table_result);
		}

		// Process all pending ring buffer inserts
		for pending in self.pending_ringbuffers {
			let rb_result =
				execute_ringbuffer_insert::<V>(&mut txn, &pending, std::any::TypeId::of::<V>())?;
			result.ringbuffers.push(rb_result);
		}

		// Commit the transaction
		txn.commit()?;

		Ok(result)
	}
}

/// Execute a table insert within a transaction
fn execute_table_insert<V: ValidationMode>(
	txn: &mut StandardCommandTransaction,
	pending: &PendingTableInsert,
	type_id: std::any::TypeId,
) -> crate::Result<TableInsertResult> {
	use reifydb_catalog::sequence::ColumnSequence;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	use crate::{
		execute::mutate::primary_key,
		transaction::operation::{DictionaryOperations, TableOperations},
	};

	// 1. Look up namespace and table from catalog
	let namespace = CatalogStore::find_namespace_by_name(txn, &pending.namespace)?
		.ok_or_else(|| BulkInsertError::namespace_not_found(Fragment::None, &pending.namespace))?;

	let table = CatalogStore::find_table_by_name(txn, namespace.id, &pending.table)?
		.ok_or_else(|| BulkInsertError::table_not_found(Fragment::None, &pending.namespace, &pending.table))?;

	// 2. Build layout for encoding - use dictionary ID type for dictionary-encoded columns
	let mut table_types: Vec<Type> = Vec::with_capacity(table.columns.len());
	for c in &table.columns {
		let ty = if let Some(dict_id) = c.dictionary_id {
			match CatalogStore::find_dictionary(txn, dict_id) {
				Ok(Some(d)) => d.id_type,
				_ => c.constraint.get_type(),
			}
		} else {
			c.constraint.get_type()
		};
		table_types.push(ty);
	}
	let layout = EncodedValuesLayout::new(&table_types);

	// 3. Validate and coerce all rows in batch (fail-fast)
	let is_validated = type_id == std::any::TypeId::of::<Validated>();
	let coerced_rows = if is_validated {
		validate_and_coerce_rows(&pending.rows, &table)?
	} else {
		reorder_rows_trusted(&pending.rows, &table)?
	};

	let mut encoded_rows = Vec::with_capacity(coerced_rows.len());

	for mut values in coerced_rows {
		// Handle auto-increment columns
		for (idx, col) in table.columns.iter().enumerate() {
			if col.auto_increment && matches!(values[idx], Value::Undefined) {
				values[idx] = ColumnSequence::next_value(txn, table.id, col.id)?;
			}
		}

		// Handle dictionary encoding
		for (idx, col) in table.columns.iter().enumerate() {
			if let Some(dict_id) = col.dictionary_id {
				let dictionary = CatalogStore::find_dictionary(txn, dict_id)?.ok_or_else(|| {
					reifydb_type::internal_error!(
						"Dictionary {:?} not found for column {}",
						dict_id,
						col.name
					)
				})?;
				let entry_id = txn.insert_into_dictionary(&dictionary, &values[idx])?;
				values[idx] = entry_id.to_value();
			}
		}

		// Validate constraints (coercion is done in batch, but final constraint check still needed)
		if is_validated {
			for (idx, col) in table.columns.iter().enumerate() {
				col.constraint.validate(&values[idx])?;
			}
		}

		// Encode the row
		let mut row = layout.allocate();
		for (idx, value) in values.iter().enumerate() {
			encode_value(&layout, &mut row, idx, value);
		}
		encoded_rows.push(row);
	}

	// 4. Batch allocate row numbers
	let total_rows = encoded_rows.len();
	if total_rows == 0 {
		return Ok(TableInsertResult {
			namespace: pending.namespace.clone(),
			table: pending.table.clone(),
			inserted: 0,
		});
	}

	let row_numbers = RowSequence::next_row_number_batch(txn, table.id, total_rows as u64)?;

	// 5. Insert all rows with their row numbers
	for (row, &row_number) in encoded_rows.iter().zip(row_numbers.iter()) {
		txn.insert_table(table.clone(), row.clone(), row_number)?;

		// Handle primary key index if table has one
		if let Some(pk_def) = primary_key::get_primary_key(txn, &table)? {
			use reifydb_core::interface::{EncodableKey, IndexEntryKey, IndexId};

			let index_key = primary_key::encode_primary_key(&pk_def, row, &table, &layout)?;
			let index_entry_key =
				IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key.clone());

			// Check for primary key violation
			if txn.contains_key(&index_entry_key.encode())? {
				let key_columns = pk_def.columns.iter().map(|c| c.name.clone()).collect();
				reifydb_core::return_error!(reifydb_type::diagnostic::index::primary_key_violation(
					Fragment::None,
					table.name.clone(),
					key_columns,
				));
			}

			// Store the index entry
			let row_number_layout = EncodedValuesLayout::new(&[Type::Uint8]);
			let mut row_number_encoded = row_number_layout.allocate();
			row_number_layout.set_u64(&mut row_number_encoded, 0, u64::from(row_number));
			txn.set(&index_entry_key.encode(), row_number_encoded)?;
		}
	}

	Ok(TableInsertResult {
		namespace: pending.namespace.clone(),
		table: pending.table.clone(),
		inserted: total_rows as u64,
	})
}

/// Execute a ring buffer insert within a transaction
fn execute_ringbuffer_insert<V: ValidationMode>(
	txn: &mut StandardCommandTransaction,
	pending: &PendingRingBufferInsert,
	type_id: std::any::TypeId,
) -> crate::Result<RingBufferInsertResult> {
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::{RowNumber, Type};

	use crate::transaction::operation::DictionaryOperations;

	// 1. Look up namespace and ring buffer from catalog
	let namespace = CatalogStore::find_namespace_by_name(txn, &pending.namespace)?
		.ok_or_else(|| BulkInsertError::namespace_not_found(Fragment::None, &pending.namespace))?;

	let ringbuffer =
		CatalogStore::find_ringbuffer_by_name(txn, namespace.id, &pending.ringbuffer)?.ok_or_else(|| {
			BulkInsertError::ringbuffer_not_found(Fragment::None, &pending.namespace, &pending.ringbuffer)
		})?;

	// Get current metadata
	let mut metadata = CatalogStore::find_ringbuffer_metadata(txn, ringbuffer.id)?.ok_or_else(|| {
		BulkInsertError::ringbuffer_not_found(Fragment::None, &pending.namespace, &pending.ringbuffer)
	})?;

	// 2. Build layout for encoding
	let mut rb_types: Vec<Type> = Vec::with_capacity(ringbuffer.columns.len());
	for c in &ringbuffer.columns {
		let ty = if let Some(dict_id) = c.dictionary_id {
			match CatalogStore::find_dictionary(txn, dict_id) {
				Ok(Some(d)) => d.id_type,
				_ => c.constraint.get_type(),
			}
		} else {
			c.constraint.get_type()
		};
		rb_types.push(ty);
	}
	let layout = EncodedValuesLayout::new(&rb_types);

	// 3. Validate and coerce all rows in batch (fail-fast)
	let is_validated = type_id == std::any::TypeId::of::<Validated>();
	let coerced_rows = if is_validated {
		validate_and_coerce_rows_rb(&pending.rows, &ringbuffer)?
	} else {
		reorder_rows_trusted_rb(&pending.rows, &ringbuffer)?
	};

	let mut inserted_count = 0u64;

	// 4. Process each coerced row
	for mut values in coerced_rows {
		// Handle dictionary encoding
		for (idx, col) in ringbuffer.columns.iter().enumerate() {
			if let Some(dict_id) = col.dictionary_id {
				let dictionary = CatalogStore::find_dictionary(txn, dict_id)?.ok_or_else(|| {
					reifydb_type::internal_error!(
						"Dictionary {:?} not found for column {}",
						dict_id,
						col.name
					)
				})?;
				let entry_id = txn.insert_into_dictionary(&dictionary, &values[idx])?;
				values[idx] = entry_id.to_value();
			}
		}

		// Validate constraints (coercion is done in batch, but final constraint check still needed)
		if is_validated {
			for (idx, col) in ringbuffer.columns.iter().enumerate() {
				col.constraint.validate(&values[idx])?;
			}
		}

		// Encode the row
		let mut row = layout.allocate();
		for (idx, value) in values.iter().enumerate() {
			encode_value(&layout, &mut row, idx, value);
		}

		// Handle ring buffer overflow - delete oldest entry if full
		if metadata.is_full() {
			let oldest_row = RowNumber(metadata.head);
			txn.remove_from_ringbuffer(ringbuffer.clone(), oldest_row)?;
			metadata.head += 1;
			metadata.count -= 1;
		}

		// Allocate row number
		let row_number = RowSequence::next_row_number_for_ringbuffer(txn, ringbuffer.id)?;

		// Store the row
		txn.insert_ringbuffer_at(ringbuffer.clone(), row_number, row)?;

		// Update metadata
		if metadata.is_empty() {
			metadata.head = row_number.0;
		}
		metadata.count += 1;
		metadata.tail = row_number.0 + 1;

		inserted_count += 1;
	}

	// Save updated metadata
	CatalogStore::update_ringbuffer_metadata(txn, metadata)?;

	Ok(RingBufferInsertResult {
		namespace: pending.namespace.clone(),
		ringbuffer: pending.ringbuffer.clone(),
		inserted: inserted_count,
	})
}

/// Parse a qualified name like "namespace.table" into (namespace, name).
/// If no namespace is provided, uses "default".
fn parse_qualified_name(qualified_name: &str) -> (String, String) {
	if let Some((ns, name)) = qualified_name.split_once('.') {
		(ns.to_string(), name.to_string())
	} else {
		("default".to_string(), qualified_name.to_string())
	}
}
