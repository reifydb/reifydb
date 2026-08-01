// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use operator::SHAPE;
use reifydb_core::{
	interface::catalog::flow::{FlowId, Operator, OperatorId},
	key::operator::OperatorKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::operator::shape::operator};

impl CatalogStore {
	pub(crate) fn find_operator(rx: &mut Transaction<'_>, operator_id: OperatorId) -> Result<Option<Operator>> {
		let Some(multi) = rx.get(&OperatorKey::encoded(operator_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = OperatorId(SHAPE.get::<u64>(&row, operator::ID));
		let flow = FlowId(SHAPE.get::<u64>(&row, operator::FLOW));
		let node_type = SHAPE.get::<u8>(&row, operator::TYPE);
		let data = SHAPE.get_blob(&row, operator::DATA).clone();

		Ok(Some(Operator {
			id,
			flow,
			node_type,
			data,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_operator, ensure_test_flow},
	};

	#[test]
	fn test_find_operator_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator = create_operator(&mut txn, flow.id, 1, &[0x01, 0x02, 0x03]);

		let result = CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), operator.id).unwrap();
		assert!(result.is_some());
		let found = result.unwrap();
		assert_eq!(found.id, operator.id);
		assert_eq!(found.flow, flow.id);
		assert_eq!(found.node_type, 1);
	}

	#[test]
	fn test_find_operator_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), OperatorId(999)).unwrap();
		assert!(result.is_none());
	}
}
