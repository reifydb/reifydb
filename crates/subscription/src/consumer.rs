// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subscription data consumption logic.
//!
//! Provides static methods for reading, converting, and deleting subscription rows.

use std::collections::HashMap;

use reifydb_catalog::find_subscription;
use reifydb_core::{
	encoded::key::EncodedKey,
	error::diagnostic::internal::internal,
	interface::catalog::id::SubscriptionId,
	key::{
		Key,
		subscription_row::{SubscriptionRowKey, SubscriptionRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, error::Error, fragment::Fragment};
use tracing::debug;

/// Static methods for consuming subscription data.
pub struct SubscriptionConsumer;

impl SubscriptionConsumer {
	/// Read rows from a subscription's storage.
	///
	/// Returns (columns, row_keys) where row_keys are the encoded keys for deletion.
	pub fn read_rows(
		engine: &StandardEngine,
		db_subscription_id: SubscriptionId,
		last_consumed_key: Option<&EncodedKey>,
		batch_size: usize,
	) -> Result<(Columns, Vec<EncodedKey>)> {
		let mut cmd_txn = engine.begin_command()?;

		// Get subscription definition using catalog function
		let _sub_def = match find_subscription(&mut Transaction::Command(&mut cmd_txn), db_subscription_id)? {
			Some(def) => def,
			None => {
				tracing::warn!("Subscription {} not found", db_subscription_id);
				return Ok((Columns::empty(), Vec::new()));
			}
		};

		// Get schema registry for resolving per-row schemas
		let catalog = engine.catalog();
		let schema_registry = &catalog.schema;

		// Create range for scanning rows
		let range = if let Some(last_key) = last_consumed_key {
			SubscriptionRowKeyRange::scan_range(db_subscription_id, Some(last_key))
		} else {
			SubscriptionRowKey::full_scan(db_subscription_id)
		};

		let mut stream = cmd_txn.range(range, batch_size)?;
		let mut entries = Vec::new();
		while let Some(result) = stream.next() {
			entries.push(result?);
		}
		drop(stream); // Explicitly drop to release the borrow on cmd_txn

		// Build dynamic column structure
		let mut column_data: HashMap<String, ColumnData> = HashMap::new();

		let mut row_numbers = Vec::new();
		let mut row_keys = Vec::new();

		// Process collected entries
		for entry in entries {
			// Decode row key
			if let Some(Key::SubscriptionRow(sub_row_key)) = Key::decode(&entry.key) {
				row_numbers.push(sub_row_key.row);
				row_keys.push(entry.key.clone());

				// Extract schema fingerprint from the encoded row
				let fingerprint = entry.values.fingerprint();

				// Resolve schema using SchemaRegistry
				let schema = schema_registry
					.get_or_load(fingerprint, &mut Transaction::Command(&mut cmd_txn))?
					.ok_or_else(|| {
						Error(internal(format!(
							"Schema not found for fingerprint: {:?}",
							fingerprint
						)))
					})?;

				// Decode each field using the resolved schema
				for (idx, field) in schema.fields().iter().enumerate() {
					let value = schema.get_value(&entry.values, idx);

					// Get or create column data for this field
					column_data
						.entry(field.name.clone())
						.or_insert_with(|| {
							ColumnData::with_capacity(field.constraint.get_type(), 0)
						})
						.push_value(value);
				}
			}
		}

		// Convert HashMap to Vec for Columns
		let columns: Vec<Column> = column_data
			.into_iter()
			.map(|(name, data)| Column {
				name: Fragment::internal(&name),
				data,
			})
			.collect();

		Ok((Columns::with_row_numbers(columns, row_numbers), row_keys))
	}

	/// Delete consumed rows from subscription storage.
	pub fn delete_rows(engine: &StandardEngine, row_keys: &[EncodedKey]) -> Result<()> {
		if row_keys.is_empty() {
			return Ok(());
		}

		let mut delete_txn = engine.begin_command()?;

		for key in row_keys {
			delete_txn.remove(key)?;
		}

		delete_txn.commit()?;

		debug!("Deleted {} consumed rows", row_keys.len());
		Ok(())
	}
}
