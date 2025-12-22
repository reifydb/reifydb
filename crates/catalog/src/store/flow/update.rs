// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, FlowId, FlowKey, FlowStatus};

use crate::{CatalogStore, store::flow::layout::flow};

impl CatalogStore {
	/// Update the name of a flow
	pub async fn update_flow_name(
		txn: &mut impl CommandTransaction,
		flow_id: FlowId,
		new_name: String,
	) -> crate::Result<()> {
		// Get the existing flow
		let flow = Self::get_flow(txn, flow_id).await?;

		// Update the name field
		let mut row = flow::LAYOUT.allocate();
		flow::LAYOUT.set_u64(&mut row, flow::ID, flow_id.0);
		flow::LAYOUT.set_u64(&mut row, flow::NAMESPACE, flow.namespace.0);
		flow::LAYOUT.set_utf8(&mut row, flow::NAME, &new_name);
		flow::LAYOUT.set_u8(&mut row, flow::STATUS, flow.status as u8);

		txn.set(&FlowKey::encoded(flow_id), row).await?;

		Ok(())
	}

	/// Update the status of a flow
	pub async fn update_flow_status(
		txn: &mut impl CommandTransaction,
		flow_id: FlowId,
		status: FlowStatus,
	) -> crate::Result<()> {
		// Get the existing flow
		let flow = Self::get_flow(txn, flow_id).await?;

		// Update the status field
		let mut row = flow::LAYOUT.allocate();
		flow::LAYOUT.set_u64(&mut row, flow::ID, flow_id.0);
		flow::LAYOUT.set_u64(&mut row, flow::NAMESPACE, flow.namespace.0);
		flow::LAYOUT.set_utf8(&mut row, flow::NAME, &flow.name);
		flow::LAYOUT.set_u8(&mut row, flow::STATUS, status as u8);

		txn.set(&FlowKey::encoded(flow_id), row).await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowStatus;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::test_utils::ensure_test_flow;

	#[tokio::test]
	async fn test_update_flow_name() {
		let mut txn = create_test_command_transaction().await;
		let flow = ensure_test_flow(&mut txn).await;

		// Update the name
		CatalogStore::update_flow_name(&mut txn, flow.id, "new_flow_name".to_string()).await.unwrap();

		// Verify update
		let updated = CatalogStore::get_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(updated.name, "new_flow_name");
		assert_eq!(updated.namespace, flow.namespace);
		assert_eq!(updated.status, flow.status);
	}

	#[tokio::test]
	async fn test_update_flow_status() {
		let mut txn = create_test_command_transaction().await;
		let flow = ensure_test_flow(&mut txn).await;

		// Initial status should be Active
		assert_eq!(flow.status, FlowStatus::Active);

		// Update to Paused
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Paused).await.unwrap();
		let updated = CatalogStore::get_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(updated.status, FlowStatus::Paused);

		// Update to Failed
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Failed).await.unwrap();
		let updated = CatalogStore::get_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(updated.status, FlowStatus::Failed);

		// Update back to Active
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Active).await.unwrap();
		let updated = CatalogStore::get_flow(&mut txn, flow.id).await.unwrap();
		assert_eq!(updated.status, FlowStatus::Active);
	}
}
