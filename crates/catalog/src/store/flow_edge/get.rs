// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::flow::{FlowEdgeDef, FlowEdgeId};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::{error::Error, internal};

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_flow_edge(
		txn: &mut impl IntoStandardTransaction,
		edge_id: FlowEdgeId,
	) -> crate::Result<FlowEdgeDef> {
		CatalogStore::find_flow_edge(txn, edge_id)?.ok_or_else(|| {
			Error(internal!(
				"Flow edge with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				edge_id
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowEdgeId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow_edge, create_flow_node, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_get_flow_edge_ok() {
		let mut txn = create_test_command_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_flow_node(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_flow_node(&mut txn, flow.id, 4, &[0x02]);
		let edge = create_flow_edge(&mut txn, flow.id, node1.id, node2.id);

		let result = CatalogStore::get_flow_edge(&mut txn, edge.id).unwrap();
		assert_eq!(result.id, edge.id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.source, node1.id);
		assert_eq!(result.target, node2.id);
	}

	#[test]
	fn test_get_flow_edge_not_found() {
		let mut txn = create_test_command_transaction();

		let err = CatalogStore::get_flow_edge(&mut txn, FlowEdgeId(999)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("FlowEdgeId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
