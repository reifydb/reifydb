// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{SubscriptionColumnId, SubscriptionId},
		subscription::{SubscriptionColumnDef, SubscriptionDef},
	},
	key::{subscription::SubscriptionKey, subscription_column::SubscriptionColumnKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::r#type::Type;

use crate::{
	CatalogStore, Result,
	store::{
		sequence::flow::next_flow_id,
		subscription::schema::{subscription, subscription_column},
	},
};

#[derive(Debug, Clone)]
pub struct SubscriptionColumnToCreate {
	pub name: String,
	pub ty: Type,
}

#[derive(Debug, Clone)]
pub struct SubscriptionToCreate {
	pub columns: Vec<SubscriptionColumnToCreate>,
}

impl CatalogStore {
	pub(crate) fn create_subscription(
		txn: &mut AdminTransaction,
		to_create: SubscriptionToCreate,
	) -> Result<SubscriptionDef> {
		// Use the flow sequence to generate subscription ID (FlowId == SubscriptionId for subscription flows)
		let flow_id = next_flow_id(txn)?;
		let subscription_id = SubscriptionId(flow_id.0);
		Self::store_subscription(txn, subscription_id)?;
		Self::insert_columns_for_subscription(txn, subscription_id, &to_create)?;

		Ok(Self::get_subscription(&mut Transaction::Admin(&mut *txn), subscription_id)?)
	}

	fn store_subscription(txn: &mut AdminTransaction, subscription: SubscriptionId) -> Result<()> {
		let mut row = subscription::SCHEMA.allocate();
		subscription::SCHEMA.set_u64(&mut row, subscription::ID, subscription.0);
		subscription::SCHEMA.set_u64(&mut row, subscription::ACKNOWLEDGED_VERSION, 0u64);
		subscription::SCHEMA.set_u64(&mut row, subscription::PRIMARY_KEY, 0u64);

		txn.set(&SubscriptionKey::encoded(subscription), row)?;

		Ok(())
	}

	fn insert_columns_for_subscription(
		txn: &mut AdminTransaction,
		subscription: SubscriptionId,
		to_create: &SubscriptionToCreate,
	) -> Result<()> {
		for (idx, column_to_create) in to_create.columns.iter().enumerate() {
			let column_id = SubscriptionColumnId(idx as u64);

			let mut row = subscription_column::SCHEMA.allocate();
			subscription_column::SCHEMA.set_u64(&mut row, subscription_column::ID, column_id);
			subscription_column::SCHEMA.set_utf8(
				&mut row,
				subscription_column::NAME,
				&column_to_create.name,
			);
			subscription_column::SCHEMA.set_u8(
				&mut row,
				subscription_column::TYPE,
				column_to_create.ty.to_u8(),
			);

			txn.set(&SubscriptionColumnKey::encoded(subscription, column_id), row)?;
		}
		Ok(())
	}

	pub(crate) fn list_subscription_columns(
		txn: &mut Transaction<'_>,
		subscription: SubscriptionId,
	) -> Result<Vec<SubscriptionColumnDef>> {
		let mut stream = txn.range(SubscriptionColumnKey::subscription_range(subscription), 256)?;

		let mut columns = Vec::new();
		while let Some(result) = stream.next() {
			let multi = result?;
			let row = &multi.values;
			let id =
				SubscriptionColumnId(subscription_column::SCHEMA.get_u64(row, subscription_column::ID));
			let name = subscription_column::SCHEMA.get_utf8(row, subscription_column::NAME).to_string();
			let ty_u8 = subscription_column::SCHEMA.get_u8(row, subscription_column::TYPE);
			let ty = Type::from_u8(ty_u8);

			columns.push(SubscriptionColumnDef {
				id,
				name,
				ty,
			});
		}

		// Sort by column ID (which is the index)
		columns.sort_by_key(|c| c.id.0);

		Ok(columns)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SubscriptionColumnId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::value::r#type::Type;

	use crate::{
		CatalogStore,
		store::subscription::create::{SubscriptionColumnToCreate, SubscriptionToCreate},
	};

	#[test]
	fn test_create_subscription() {
		let mut txn = create_test_admin_transaction();

		let to_create = SubscriptionToCreate {
			columns: vec![],
		};

		let result = CatalogStore::create_subscription(&mut txn, to_create).unwrap();
		// UUID v7 IDs are generated, so we just verify the subscription was created
		assert_eq!(result.acknowledged_version.0, 0);
		assert!(result.columns.is_empty());
	}

	#[test]
	fn test_create_subscription_with_columns() {
		let mut txn = create_test_admin_transaction();

		let to_create = SubscriptionToCreate {
			columns: vec![
				SubscriptionColumnToCreate {
					name: "id".to_string(),
					ty: Type::Int8,
				},
				SubscriptionColumnToCreate {
					name: "name".to_string(),
					ty: Type::Utf8,
				},
			],
		};

		let result = CatalogStore::create_subscription(&mut txn, to_create).unwrap();
		assert_eq!(result.columns.len(), 2);

		// Column IDs are indices
		assert_eq!(result.columns[0].id, SubscriptionColumnId(0));
		assert_eq!(result.columns[0].name, "id");
		assert_eq!(result.columns[0].ty, Type::Int8);

		assert_eq!(result.columns[1].id, SubscriptionColumnId(1));
		assert_eq!(result.columns[1].name, "name");
		assert_eq!(result.columns[1].ty, Type::Utf8);
	}

	#[test]
	fn test_create_multiple_subscriptions() {
		let mut txn = create_test_admin_transaction();

		let sub1 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let sub2 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		// Multiple subscriptions allowed with unique UUID v7 IDs
		assert_ne!(sub1.id, sub2.id);
	}
}
