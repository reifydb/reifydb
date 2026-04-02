// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::find_subscription;
use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::catalog::id::SubscriptionId,
	key::{Key, subscription_row::SubscriptionRowKey},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error::Error, fragment::Fragment, params::Params, value::Value};

use crate::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};

pub struct InspectSubscription;

impl Default for InspectSubscription {
	fn default() -> Self {
		Self::new()
	}
}

impl InspectSubscription {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for InspectSubscription {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let subscription_id_value = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::Uint8(id) => *id,
				Value::Utf8(s) => s.parse::<u64>().map_err(|_| ProcedureError::ExecutionFailed {
					procedure: Fragment::internal("inspect_subscription"),
					reason: "Invalid subscription_id format".to_string(),
				})?,
				_ => {
					return Err(ProcedureError::ExecutionFailed {
						procedure: Fragment::internal("inspect_subscription"),
						reason: "subscription_id must be of type u64 or utf8".to_string(),
					});
				}
			},
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("inspect_subscription"),
					expected: 1,
					actual: match ctx.params {
						Params::Positional(args) => args.len(),
						_ => 0,
					},
				});
			}
		};

		let subscription_id = SubscriptionId(subscription_id_value);

		// Use catalog function to get subscription definition
		let subscription =
			find_subscription(tx, subscription_id)?.ok_or_else(|| ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("inspect_subscription"),
				reason: format!("Subscription {} not found", subscription_id),
			})?;

		// Scan subscription rows
		let range = SubscriptionRowKey::full_scan(subscription_id);
		let mut stream = tx.range(range, 1024)?;

		// Build columns structure
		let all_columns = subscription.all_columns();
		let mut column_data_builders: Vec<_> = all_columns
			.iter()
			.map(|col| (col.name.clone(), ColumnData::with_capacity(col.ty.clone(), 0)))
			.collect();

		let mut row_numbers = Vec::new();

		// Collect all entries first to avoid borrow checker issues
		let mut entries = Vec::new();
		for result in stream.by_ref() {
			entries.push(result?);
		}
		drop(stream); // Explicitly drop to release the borrow on tx

		let catalog = ctx.catalog;

		// Process collected entries
		for entry in entries {
			if let Some(Key::SubscriptionRow(sub_row_key)) = Key::decode(&entry.key) {
				row_numbers.push(sub_row_key.row);

				let fingerprint = entry.row.fingerprint();
				let shape = catalog.get_or_load_row_shape(fingerprint, tx)?.ok_or_else(|| {
					Error(Box::new(internal(format!(
						"Shape not found for fingerprint: {:?}",
						fingerprint
					))))
				})?;

				for (idx, (_, data)) in column_data_builders.iter_mut().enumerate() {
					let value = shape.get_value(&entry.row, idx);
					data.push_value(value);
				}
			}
		}

		let columns: Vec<Column> = column_data_builders
			.into_iter()
			.map(|(name, data)| Column {
				name: Fragment::internal(&name),
				data,
			})
			.collect();

		Ok(Columns::with_row_numbers(columns, row_numbers))
	}
}
