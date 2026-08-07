// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		change::CatalogTrackOperatorChangeOperations,
		flow::{FlowId, OperatorId},
	},
	key::operator::{OperatorByFlowKey, OperatorKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_operator(txn: &mut AdminTransaction, operator_id: OperatorId) -> Result<()> {
		let Some(node_def) = CatalogStore::find_operator(&mut Transaction::Admin(&mut *txn), operator_id)?
		else {
			return Ok(());
		};

		Self::unlink_node(txn, operator_id, node_def.flow)?;
		txn.track_operator_deleted(node_def)?;
		Ok(())
	}

	#[inline]
	fn unlink_node(txn: &mut AdminTransaction, operator_id: OperatorId, flow: FlowId) -> Result<()> {
		txn.remove(&OperatorKey::encoded(operator_id))?;
		txn.remove(&OperatorByFlowKey::encoded(flow, operator_id))?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_operator, ensure_test_flow},
	};

	#[test]
	fn test_drop_operator() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator = create_operator(&mut txn, flow.id, 1, &[0x01]);

		assert!(CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), operator.id).unwrap().is_some());

		CatalogStore::drop_operator(&mut txn, operator.id).unwrap();

		assert!(CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), operator.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_node_removes_from_index() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator = create_operator(&mut txn, flow.id, 1, &[0x01]);

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(operators.len(), 1);

		CatalogStore::drop_operator(&mut txn, operator.id).unwrap();

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert!(operators.is_empty());
	}

	#[test]
	fn test_drop_nonexistent_node() {
		// Dropping a operator that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		CatalogStore::drop_operator(&mut txn, OperatorId(999)).unwrap();
	}

	#[test]
	fn test_drop_one_node_keeps_others() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_operator(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_operator(&mut txn, flow.id, 4, &[0x02]);

		CatalogStore::drop_operator(&mut txn, node1.id).unwrap();

		assert!(CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), node1.id).unwrap().is_none());
		assert!(CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), node2.id).unwrap().is_some());

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(operators.len(), 1);
		assert_eq!(operators[0].id, node2.id);
	}
}
