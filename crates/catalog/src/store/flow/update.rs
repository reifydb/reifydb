// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowStatus},
	key::flow::FlowKey,
};
use reifydb_transaction::transaction::command::CommandTransaction;

use crate::{CatalogStore, store::flow::schema::flow};

impl CatalogStore {
	/// Update the name of a flow
	pub(crate) fn update_flow_name(
		txn: &mut CommandTransaction,
		flow_id: FlowId,
		new_name: String,
	) -> crate::Result<()> {
		// Get the existing flow
		let flow = Self::get_flow(txn, flow_id)?;

		// Update the name field
		let mut row = flow::SCHEMA.allocate();
		flow::SCHEMA.set_u64(&mut row, flow::ID, flow_id.0);
		flow::SCHEMA.set_u64(&mut row, flow::NAMESPACE, flow.namespace.0);
		flow::SCHEMA.set_utf8(&mut row, flow::NAME, &new_name);
		flow::SCHEMA.set_u8(&mut row, flow::STATUS, flow.status as u8);

		txn.set(&FlowKey::encoded(flow_id), row)?;

		Ok(())
	}

	/// Update the status of a flow
	pub(crate) fn update_flow_status(
		txn: &mut CommandTransaction,
		flow_id: FlowId,
		status: FlowStatus,
	) -> crate::Result<()> {
		// Get the existing flow
		let flow = Self::get_flow(txn, flow_id)?;

		// Update the status field
		let mut row = flow::SCHEMA.allocate();
		flow::SCHEMA.set_u64(&mut row, flow::ID, flow_id.0);
		flow::SCHEMA.set_u64(&mut row, flow::NAMESPACE, flow.namespace.0);
		flow::SCHEMA.set_utf8(&mut row, flow::NAME, &flow.name);
		flow::SCHEMA.set_u8(&mut row, flow::STATUS, status as u8);

		txn.set(&FlowKey::encoded(flow_id), row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowStatus;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::test_utils::ensure_test_flow;

	#[test]
	fn test_update_flow_name() {
		let mut txn = create_test_command_transaction();
		let flow = ensure_test_flow(&mut txn);

		// Update the name
		CatalogStore::update_flow_name(&mut txn, flow.id, "new_flow_name".to_string()).unwrap();

		// Verify update
		let updated = CatalogStore::get_flow(&mut txn, flow.id).unwrap();
		assert_eq!(updated.name, "new_flow_name");
		assert_eq!(updated.namespace, flow.namespace);
		assert_eq!(updated.status, flow.status);
	}

	#[test]
	fn test_update_flow_status() {
		let mut txn = create_test_command_transaction();
		let flow = ensure_test_flow(&mut txn);

		// Initial status should be Active
		assert_eq!(flow.status, FlowStatus::Active);

		// Update to Paused
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Paused).unwrap();
		let updated = CatalogStore::get_flow(&mut txn, flow.id).unwrap();
		assert_eq!(updated.status, FlowStatus::Paused);

		// Update to Failed
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Failed).unwrap();
		let updated = CatalogStore::get_flow(&mut txn, flow.id).unwrap();
		assert_eq!(updated.status, FlowStatus::Failed);

		// Update back to Active
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Active).unwrap();
		let updated = CatalogStore::get_flow(&mut txn, flow.id).unwrap();
		assert_eq!(updated.status, FlowStatus::Active);
	}
}
