// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pull-based subscription cursor for embedded/HTTP use.

use reifydb_core::{encoded::key::EncodedKey, interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::actor::system::ActorSystem;

use crate::consumer::SubscriptionConsumer;

/// Pull-based cursor for consuming subscription data.
///
/// Unlike the push-based `SubscriptionPoller`, the cursor allows consumers
/// to pull data on demand, suitable for HTTP polling or embedded usage.
pub struct SubscriptionCursor {
	subscription_id: SubscriptionId,
	batch_size: usize,
	last_consumed_key: Option<EncodedKey>,
	engine: StandardEngine,
	system: ActorSystem,
}

impl SubscriptionCursor {
	/// Create a new subscription cursor.
	pub fn new(
		subscription_id: SubscriptionId,
		batch_size: usize,
		engine: StandardEngine,
		system: ActorSystem,
	) -> Self {
		Self {
			subscription_id,
			batch_size,
			last_consumed_key: None,
			engine,
			system,
		}
	}

	/// Fetch the next batch of subscription data.
	///
	/// Returns `None` if there is no new data available.
	pub fn next(&mut self) -> reifydb_type::Result<Option<Columns>> {
		let sub_id = self.subscription_id;
		let last_key = self.last_consumed_key.clone();
		let batch_size = self.batch_size;
		let engine_clone = self.engine.clone();

		let read_result = self.system.install(move || {
			SubscriptionConsumer::read_rows(&engine_clone, sub_id, last_key.as_ref(), batch_size)
		});

		let (rows, row_keys) = read_result?;

		if rows.is_empty() {
			return Ok(None);
		}

		// Advance cursor BEFORE deletion for at-least-once guarantee
		let prev_cursor = self.last_consumed_key.clone();
		if let Some(last_key) = row_keys.last() {
			self.last_consumed_key = Some(last_key.clone());
		}

		// Delete consumed rows
		let keys_to_delete = row_keys.clone();
		let engine_clone = self.engine.clone();
		let delete_result =
			self.system.install(move || SubscriptionConsumer::delete_rows(&engine_clone, &keys_to_delete));

		match delete_result {
			Ok(()) => {}
			Err(e) => {
				self.last_consumed_key = prev_cursor;
				return Err(e);
			}
		}

		Ok(Some(rows))
	}
}
