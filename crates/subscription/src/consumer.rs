// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB


use std::collections::{HashMap, HashSet};

use reifydb_catalog::find_subscription;
use reifydb_core::{
	encoded::key::EncodedKey,
	error::diagnostic::internal::internal,
	interface::catalog::id::SubscriptionId,
	key::{
		Key,
		subscription_row::{SubscriptionRowKey, SubscriptionRowKeyRange},
	},
	value::column::{ColumnWithName, columns::Columns, buffer::ColumnBuffer},
};
use reifydb_type::value::datetime::DateTime;
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, error::Error, fragment::Fragment, value::identity::IdentityId};
use tracing::{debug, warn};

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
		let mut cmd_txn = engine.begin_command(IdentityId::system())?;

		// Get subscription definition using catalog function
		let _sub_def = match find_subscription(&mut Transaction::Command(&mut cmd_txn), db_subscription_id)? {
			Some(def) => def,
			None => {
				warn!("Subscription {} not found", db_subscription_id);
				return Ok((Columns::empty(), Vec::new()));
			}
		};

		// Get shape registry for resolving per-row shapes
		let catalog = engine.catalog();
		let row_shape_registry = &catalog.shape;

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
		let mut column_data: HashMap<String, ColumnBuffer> = HashMap::new();

		let mut row_numbers = Vec::new();
		let mut row_keys = Vec::new();
		let mut row_count: usize = 0;

		// Process collected entries
		for entry in entries {
			// Decode row key
			if let Some(Key::SubscriptionRow(sub_row_key)) = Key::decode(&entry.key) {
				row_numbers.push(sub_row_key.row);
				row_keys.push(entry.key.clone());

				// Extract shape fingerprint from the encoded row
				let fingerprint = entry.row.fingerprint();

				// Resolve shape using RowShapeRegistry
				let shape = row_shape_registry
					.get_or_load(fingerprint, &mut Transaction::Command(&mut cmd_txn))?
					.ok_or_else(|| {
						Error(internal(format!(
							"Shape not found for fingerprint: {:?}",
							fingerprint
						)))
					})?;

				let mut seen_in_this_entry = HashSet::new();

				// Decode each field using the resolved shape
				for (idx, field) in shape.fields().iter().enumerate() {
					let value = shape.get_value(&entry.row, idx);
					seen_in_this_entry.insert(field.name.clone());

					// Get or create column data for this field
					column_data
						.entry(field.name.clone())
						.or_insert_with(|| {
							// New column - backfill with None for all prior rows
							let mut cd = ColumnBuffer::with_capacity(
								field.constraint.get_type(),
								0,
							);
							for _ in 0..row_count {
								cd.push_none();
							}
							cd
						})
						.push_value(value);
				}

				// Pad columns not seen in this entry with None
				for (name, col) in column_data.iter_mut() {
					if !seen_in_this_entry.contains(name) {
						col.push_none();
					}
				}

				row_count += 1;
			}
		}

		// Convert HashMap to Vec for Columns
		let columns: Vec<ColumnWithName> = column_data
			.into_iter()
			.map(|(name, data)| ColumnWithName {
				name: Fragment::internal(&name),
				data,
			})
			.collect();

		let n = row_numbers.len();
		let now = DateTime::default();
		Ok((Columns::with_system_columns(columns, row_numbers, vec![now; n], vec![now; n]), row_keys))
	}

	/// Delete consumed rows from subscription storage.
	pub fn delete_rows(engine: &StandardEngine, row_keys: &[EncodedKey]) -> Result<()> {
		if row_keys.is_empty() {
			return Ok(());
		}

		let mut delete_txn = engine.begin_command(IdentityId::system())?;

		for key in row_keys {
			delete_txn.remove(key)?;
		}

		delete_txn.commit()?;

		debug!("Deleted {} consumed rows", row_keys.len());
		Ok(())
	}
}
