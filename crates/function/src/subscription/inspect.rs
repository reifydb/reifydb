// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::catalog::id::SubscriptionId,
	key::{Key, subscription_row::SubscriptionRowKey},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{error::Error, fragment::Fragment};

use crate::{GeneratorContext, GeneratorFunction};

pub struct InspectSubscription;

impl InspectSubscription {
	pub fn new() -> Self {
		Self {}
	}
}

impl GeneratorFunction for InspectSubscription {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> crate::error::GeneratorFunctionResult<Columns> {
		let txn = ctx.txn;

		let params = &ctx.params;
		if params.len() != 1 {
			panic!("inspect_subscription requires exactly 1 parameter: subscription_id (u64)");
		}

		let id_column = params.get(0).unwrap();
		let subscription_id_value = match id_column.data() {
			ColumnData::Uint8(container) => {
				container.get(0).copied().expect("subscription_id parameter is empty")
			}
			ColumnData::Utf8 {
				container,
				..
			} => {
				let id_str = container.get(0).expect("subscription_id parameter is empty");
				id_str.parse::<u64>().expect("Invalid subscription_id format")
			}
			_ => panic!("subscription_id must be of type u64 or utf8"),
		};

		let subscription_id = SubscriptionId(subscription_id_value);

		// Use catalog function to get subscription definition
		let subscription_def = reifydb_catalog::find_subscription(txn, subscription_id)?
			.unwrap_or_else(|| panic!("Subscription {} not found", subscription_id));

		// Scan subscription rows
		let range = SubscriptionRowKey::full_scan(subscription_id);
		let mut stream = txn.range(range, 1024)?;

		// Build columns structure
		let all_columns = subscription_def.all_columns();
		let mut column_data_builders: Vec<_> = all_columns
			.iter()
			.map(|col| (col.name.clone(), ColumnData::with_capacity(col.ty, 0)))
			.collect();

		let mut row_numbers = Vec::new();

		// Collect all entries first to avoid borrow checker issues
		let mut entries = Vec::new();
		while let Some(result) = stream.next() {
			entries.push(result?);
		}
		drop(stream); // Explicitly drop to release the borrow on txn

		let catalog = ctx.catalog;
		let schema_registry = &catalog.schema;

		// Process collected entries
		for entry in entries {
			if let Some(Key::SubscriptionRow(sub_row_key)) = Key::decode(&entry.key) {
				row_numbers.push(sub_row_key.row);

				let fingerprint = entry.values.fingerprint();
				let schema = schema_registry.get_or_load(fingerprint, txn)?.ok_or_else(|| {
					Error(internal(format!("Schema not found for fingerprint: {:?}", fingerprint)))
				})?;

				for (idx, (_, data)) in column_data_builders.iter_mut().enumerate() {
					let value = schema.get_value(&entry.values, idx);
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
