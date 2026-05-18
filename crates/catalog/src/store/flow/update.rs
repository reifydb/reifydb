// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{FlowId, FlowStatus},
	key::flow::FlowKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::flow::shape::flow};

impl CatalogStore {
	pub(crate) fn update_flow_name(txn: &mut AdminTransaction, flow_id: FlowId, new_name: String) -> Result<()> {
		let flow = Self::get_flow(&mut Transaction::Admin(&mut *txn), flow_id)?;

		let mut row = flow::SHAPE.allocate();
		flow::SHAPE.set_u64(&mut row, flow::ID, flow_id.0);
		flow::SHAPE.set_u64(&mut row, flow::NAMESPACE, flow.namespace.0);
		flow::SHAPE.set_utf8(&mut row, flow::NAME, &new_name);
		flow::SHAPE.set_u8(&mut row, flow::STATUS, flow.status as u8);

		txn.set(&FlowKey::encoded(flow_id), row)?;

		Ok(())
	}

	pub(crate) fn update_flow_status(
		txn: &mut AdminTransaction,
		flow_id: FlowId,
		status: FlowStatus,
	) -> Result<()> {
		let flow = Self::get_flow(&mut Transaction::Admin(&mut *txn), flow_id)?;

		let mut row = flow::SHAPE.allocate();
		flow::SHAPE.set_u64(&mut row, flow::ID, flow_id.0);
		flow::SHAPE.set_u64(&mut row, flow::NAMESPACE, flow.namespace.0);
		flow::SHAPE.set_utf8(&mut row, flow::NAME, &flow.name);
		flow::SHAPE.set_u8(&mut row, flow::STATUS, status as u8);

		txn.set(&FlowKey::encoded(flow_id), row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowStatus;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::test_utils::ensure_test_flow;

	#[test]
	fn test_update_flow_name() {
		let mut txn = create_test_admin_transaction();
		let flow = ensure_test_flow(&mut txn);

		// Update the name
		CatalogStore::update_flow_name(&mut txn, flow.id, "new_flow_name".to_string()).unwrap();

		// Verify update
		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(updated.name, "new_flow_name");
		assert_eq!(updated.namespace, flow.namespace);
		assert_eq!(updated.status, flow.status);
	}

	#[test]
	fn test_update_flow_status() {
		let mut txn = create_test_admin_transaction();
		let flow = ensure_test_flow(&mut txn);

		// Initial status should be Active
		assert_eq!(flow.status, FlowStatus::Active);

		// Update to Paused
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Paused).unwrap();
		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(updated.status, FlowStatus::Paused);

		// Update to Failed
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Failed).unwrap();
		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(updated.status, FlowStatus::Failed);

		// Update back to Active
		CatalogStore::update_flow_status(&mut txn, flow.id, FlowStatus::Active).unwrap();
		let updated = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(updated.status, FlowStatus::Active);
	}
}
