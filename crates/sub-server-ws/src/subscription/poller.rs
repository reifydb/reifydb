// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subscription poller for consuming subscription data and delivering to websocket clients.
//!
//! This module implements the peek-then-delete pattern for consuming subscription rows:
//! 1. Read rows from subscription storage
//! 2. Send to websocket clients via push channels
//! 3. Delete consumed rows after successful delivery
//!
//! The poller tracks consumption state (last consumed row) in memory to enable
//! efficient incremental polling.

use dashmap::DashMap;
use reifydb_core::{
	encoded::key::EncodedKey,
	interface::catalog::id::SubscriptionId,
	key::{
		Key,
		subscription_row::{SubscriptionRowKey, SubscriptionRowKeyRange},
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_sub_server::{
	response::{ResponseColumn, ResponseFrame},
	state::AppState,
};
use reifydb_core::error::diagnostic::internal::internal;
use reifydb_type::{
	error::Error,
	fragment::Fragment,
};
use tokio::sync::mpsc;

use super::{PushMessage, SubscriptionRegistry};

/// Consumption state for a single subscription.
#[derive(Debug, Clone)]
pub struct ConsumptionState {
	/// The database subscription ID being consumed
	pub db_subscription_id: SubscriptionId,
	/// The last row key that was successfully consumed and deleted
	/// Used as a cursor for incremental polling
	pub last_consumed_key: Option<EncodedKey>,
}

/// Subscription poller that consumes subscription data and delivers to websocket clients.
pub struct SubscriptionPoller {
	/// Consumption state per subscription
	/// Maps: subscription_id → ConsumptionState
	states: DashMap<SubscriptionId, ConsumptionState>,
	/// Batch size for reading rows per poll cycle
	batch_size: usize,
}

impl SubscriptionPoller {
	/// Create a new subscription poller.
	///
	/// # Arguments
	///
	/// * `batch_size` - Maximum number of rows to read per subscription per poll cycle
	pub fn new(batch_size: usize) -> Self {
		Self {
			states: DashMap::new(),
			batch_size,
		}
	}

	/// Register a new subscription for polling.
	///
	/// Should be called when a websocket client subscribes.
	pub fn register(&self, subscription_id: SubscriptionId) {
		self.states.insert(
			subscription_id,
			ConsumptionState {
				db_subscription_id: subscription_id,
				last_consumed_key: None,
			},
		);
		tracing::debug!("Registered subscription {} for polling", subscription_id);
	}

	/// Unregister a subscription from polling.
	///
	/// Should be called when a websocket client unsubscribes or disconnects.
	pub fn unregister(&self, subscription_id: &SubscriptionId) {
		self.states.remove(subscription_id);
		tracing::debug!("Unregistered subscription {} from polling", subscription_id);
	}

	/// Poll all active subscriptions and deliver data to clients.
	///
	/// This is the main polling loop that should be called periodically.
	pub async fn poll_all_subscriptions(&self, state: &AppState, registry: &SubscriptionRegistry) {
		// Get all active subscriptions
		let subscription_ids: Vec<_> = self.states.iter().map(|entry| *entry.key()).collect();

		for subscription_id in subscription_ids {
			if let Err(e) = self.poll_single_subscription(subscription_id, state, registry).await {
				tracing::error!("Failed to poll subscription {}: {:?}", subscription_id, e);
			}
		}
	}

	/// Poll a single subscription and deliver data to the client.
	async fn poll_single_subscription(
		&self,
		subscription_id: SubscriptionId,
		state: &AppState,
		registry: &SubscriptionRegistry,
	) -> reifydb_type::Result<()> {
		// Get consumption state
		let consumption_state = match self.states.get(&subscription_id) {
			Some(state) => state.clone(),
			None => {
				// Subscription was removed (client disconnected)
				return Ok(());
			}
		};

		// Get the subscription state from registry to access push channel
		let push_tx = match registry.get_push_channel(&subscription_id) {
			Some(tx) => tx,
			None => {
				// Subscription not in registry (might be cleaning up)
				self.unregister(&subscription_id);
				return Ok(());
			}
		};

		let db_subscription_id = consumption_state.db_subscription_id;
		let last_consumed_key = consumption_state.last_consumed_key.clone();
		let batch_size = self.batch_size;
		let engine = state.engine_clone();
		let pool = state.pool();

		let read_result: reifydb_type::Result<(Columns, Vec<EncodedKey>)> = pool
			.compute(move || {
				Self::read_subscription_rows_sync(
					&engine,
					db_subscription_id,
					last_consumed_key.as_ref(),
					batch_size,
				)
			})
			.await
			.map_err(|e| Error(internal(format!("Compute pool error: {:?}", e))))?;

		let (rows, row_keys) = read_result?;

		if rows.is_empty() {
			// No new data to send
			return Ok(());
		}

		// Convert to ResponseFrame
		let frame = Self::convert_to_frame(rows)?;

		// Send to client via push channel
		let msg = PushMessage::Change {
			subscription_id,
			frame,
		};

		match push_tx.try_send(msg) {
			Ok(_) => {
				// Successfully sent, now delete the rows
				let engine = state.engine_clone();
				let pool = state.pool();
				let keys_to_delete: Vec<EncodedKey> = row_keys.clone();

				let delete_result: reifydb_type::Result<()> = pool
					.compute(move || Self::delete_rows_sync(&engine, &keys_to_delete))
					.await
					.map_err(|e| Error(internal(format!("Compute pool error: {:?}", e))))?;
				delete_result?;

				// Update cursor to last consumed key
				if let Some(last_key) = row_keys.last() {
					if let Some(mut state_ref) = self.states.get_mut(&subscription_id) {
						state_ref.last_consumed_key = Some(last_key.clone());
					}
				}
			}
			Err(mpsc::error::TrySendError::Full(_)) => {
				// Channel full - client is slow, don't delete rows, will retry next poll
				tracing::warn!("Push channel full for subscription {}, will retry", subscription_id);
			}
			Err(mpsc::error::TrySendError::Closed(_)) => {
				// Channel closed - client disconnected
				tracing::debug!(
					"Push channel closed for subscription {}, unregistering",
					subscription_id
				);
				self.unregister(&subscription_id);
			}
		}

		Ok(())
	}

	/// Read rows from a subscription's storage.
	///
	/// Returns (columns, row_keys) where row_keys are the encoded keys for deletion.
	fn read_subscription_rows_sync(
		engine: &reifydb_engine::engine::StandardEngine,
		db_subscription_id: SubscriptionId,
		last_consumed_key: Option<&EncodedKey>,
		batch_size: usize,
	) -> reifydb_type::Result<(Columns, Vec<EncodedKey>)> {
		let mut cmd_txn = engine.begin_command()?;

		// Get subscription definition using catalog function
		let _sub_def = match reifydb_catalog::find_subscription(&mut cmd_txn, db_subscription_id)? {
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
		use std::collections::HashMap;
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
				let schema =
					schema_registry.get_or_load(fingerprint, &mut cmd_txn)?.ok_or_else(|| {
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

	/// Convert Columns to ResponseFrame.
	fn convert_to_frame(columns: Columns) -> reifydb_type::Result<ResponseFrame> {
		let row_count = columns.row_count();
		let row_numbers: Vec<u64> = columns.row_numbers.iter().map(|r| r.0).collect();

		let response_columns: Vec<ResponseColumn> = columns
			.columns
			.iter()
			.map(|col| {
				let data: Vec<String> = (0..row_count)
					.map(|idx| {
						let value = col.data().get_value(idx);
						value.to_string()
					})
					.collect();

				ResponseColumn {
					name: col.name.to_string(),
					r#type: col.data().get_type(),
					data,
				}
			})
			.collect();

		Ok(ResponseFrame {
			row_numbers,
			columns: response_columns,
		})
	}

	/// Delete consumed rows from subscription storage.
	fn delete_rows_sync(
		engine: &reifydb_engine::engine::StandardEngine,
		row_keys: &[EncodedKey],
	) -> reifydb_type::Result<()> {
		if row_keys.is_empty() {
			return Ok(());
		}

		let mut delete_txn = engine.begin_command()?;

		for key in row_keys {
			delete_txn.remove(key)?;
		}

		delete_txn.commit()?;

		tracing::debug!("Deleted {} consumed rows", row_keys.len());
		Ok(())
	}
}
