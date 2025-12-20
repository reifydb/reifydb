// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{FlowNodeDef, FlowNodeId, QueryTransaction},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_flow_node(txn: &mut impl QueryTransaction, node_id: FlowNodeId) -> crate::Result<FlowNodeDef> {
		CatalogStore::find_flow_node(txn, node_id)?.ok_or_else(|| {
			Error(internal!(
				"Flow node with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				node_id
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowNodeId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_get_flow_node_ok() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_flow_node(&mut txn, flow.id, 1, &[0x01, 0x02, 0x03]);

		let result = CatalogStore::get_flow_node(&mut txn, node.id).unwrap();
		assert_eq!(result.id, node.id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.node_type, 1);
		assert_eq!(result.data.as_ref(), &[0x01, 0x02, 0x03]);
	}

	#[test]
	fn test_get_flow_node_not_found() {
		let mut txn = create_test_command_transaction();

		let err = CatalogStore::get_flow_node(&mut txn, FlowNodeId(999)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("FlowNodeId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
