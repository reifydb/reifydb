// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	flow::operator::{FlowNode, OperatorDef},
	interface::catalog::flow::OperatorId,
	internal,
	key::operator::key::OperatorKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result, store::operator::shape::operator};

impl CatalogStore {
	pub(crate) fn find_operator(rx: &mut Transaction<'_>, operator_id: OperatorId) -> Result<Option<FlowNode>> {
		let Some(multi) = rx.get(&OperatorKey::new(operator_id))? else {
			return Ok(None);
		};

		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
		let id = OperatorId(operator::get_id(&bytes));
		let ty: OperatorDef = from_bytes(operator::get_data(&bytes).as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize OperatorDef: {}", e))))?;

		Ok(Some(FlowNode::new(id, ty)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{flow::operator::OperatorDef, interface::catalog::flow::OperatorId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
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

		let operator = create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});

		let result = CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), operator.id).unwrap();
		assert!(result.is_some());
		let found = result.unwrap();
		assert_eq!(found.id, operator.id);
		assert!(CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id)
			.unwrap()
			.iter()
			.any(|n| n.id == found.id));
		assert_eq!(found.ty, OperatorDef::SourceInlineData {});
	}

	#[test]
	fn test_find_operator_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), OperatorId(999)).unwrap();
		assert!(result.is_none());
	}
}
