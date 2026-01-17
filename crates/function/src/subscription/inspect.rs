// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::named::EncodedValuesNamedLayout,
	interface::catalog::{
		id::SubscriptionId,
		subscription::{SubscriptionColumnDef, SubscriptionDef},
	},
	key::{
		Key, subscription::SubscriptionKey, subscription_column::SubscriptionColumnKey,
		subscription_row::SubscriptionRowKey,
	},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{fragment::Fragment, value::uuid::parse::parse_uuid7};

use crate::{GeneratorContext, GeneratorFunction};

pub struct InspectSubscription;

impl InspectSubscription {
	pub fn new() -> Self {
		Self {}
	}
}

impl GeneratorFunction for InspectSubscription {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> reifydb_type::Result<Columns> {
		let txn = ctx.txn;

		// Extract subscription_id parameter
		let params = &ctx.params;
		if params.len() != 1 {
			panic!("inspect_subscription requires exactly 1 parameter: subscription_id (uuid7)");
		}

		let id_column = params.get(0).unwrap();
		let subscription_id_uuid = match id_column.data() {
			ColumnData::Uuid7(container) => {
				container.get(0).copied().expect("subscription_id parameter is empty")
			}
			ColumnData::Utf8 {
				container,
				..
			} => {
				// Parse UTF-8 string as UUID7
				let uuid_str = container.get(0).expect("subscription_id parameter is empty");
				parse_uuid7(Fragment::internal(uuid_str)).expect("Invalid UUID7 format")
			}
			_ => panic!("subscription_id must be of type uuid7 or utf8"),
		};

		let subscription_id = SubscriptionId(subscription_id_uuid.0);

		let sub_key = SubscriptionKey::encoded(subscription_id);

		let subscription_def = if let Some(entry) = txn.get(&sub_key)? {
			// Scan subscription columns
			let mut stream = txn.range(SubscriptionColumnKey::subscription_range(subscription_id), 256)?;
			let mut columns = Vec::new();

			while let Some(result) = stream.next() {
				let col_entry = result?;
				if let Some(Key::SubscriptionColumn(col_key)) = Key::decode(&col_entry.key) {
					use reifydb_catalog::store::subscription::schema::subscription_column;
					let name = subscription_column::SCHEMA
						.get_utf8(&col_entry.values, subscription_column::NAME)
						.to_string();
					let ty_u8 = subscription_column::SCHEMA
						.get_u8(&col_entry.values, subscription_column::TYPE);
					let ty = reifydb_type::value::r#type::Type::from_u8(ty_u8);

					columns.push(SubscriptionColumnDef {
						id: col_key.column,
						name,
						ty,
					});
				}
			}

			// Sort by column ID (which is the index) - CRITICAL for correct encoding/decoding order
			columns.sort_by_key(|c| c.id.0);

			// Get acknowledged version
			use reifydb_catalog::store::subscription::schema::subscription;
			let acknowledged_version = reifydb_core::common::CommitVersion(
				subscription::SCHEMA.get_u64(&entry.values, subscription::ACKNOWLEDGED_VERSION),
			);

			SubscriptionDef {
				id: subscription_id,
				columns,
				primary_key: None,
				acknowledged_version,
			}
		} else {
			panic!("Subscription {} not found", subscription_id_uuid);
		};

		// Build layout for all columns (user + implicit)
		let layout: EncodedValuesNamedLayout = (&subscription_def).into();

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

		// Scan all rows
		while let Some(result) = stream.next() {
			let entry = result?;

			if let Some(Key::SubscriptionRow(sub_row_key)) = Key::decode(&entry.key) {
				row_numbers.push(sub_row_key.row);

				// Extract values for each column
				for (idx, (_, data)) in column_data_builders.iter_mut().enumerate() {
					let value = layout.get_value_by_idx(&entry.values, idx);
					data.push_value(value);
				}
			}
		}

		// Build final columns
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
