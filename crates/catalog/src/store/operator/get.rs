// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{Operator, OperatorId},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_operator(rx: &mut Transaction<'_>, node_id: OperatorId) -> Result<Operator> {
		CatalogStore::find_operator(rx, node_id)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Flow node with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				node_id
			)))
		})
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
	fn test_get_operator_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_operator(&mut txn, flow.id, 1, &[0x01, 0x02, 0x03]);

		let result = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), node.id).unwrap();
		assert_eq!(result.id, node.id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.node_type, 1);
		assert_eq!(result.data.as_bytes(), &[0x01, 0x02, 0x03]);
	}

	#[test]
	fn test_get_operator_not_found() {
		let mut txn = create_test_admin_transaction();

		let err = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), OperatorId(999)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("OperatorId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
