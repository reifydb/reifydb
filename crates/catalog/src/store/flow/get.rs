// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{FlowDef, FlowId},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn get_flow(rx: &mut Transaction<'_>, flow: FlowId) -> crate::Result<FlowDef> {
		CatalogStore::find_flow(rx, flow)?.ok_or_else(|| {
			Error(internal!(
				"Flow with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				flow
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_namespace},
	};

	#[test]
	fn test_get_flow_ok() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let _namespace_two = create_namespace(&mut txn, "namespace_two");

		create_flow(&mut txn, "namespace_one", "flow_one");
		create_flow(&mut txn, "namespace_two", "flow_two");

		let result = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), FlowId(1)).unwrap();
		assert_eq!(result.id, FlowId(1));
		assert_eq!(result.name, "flow_one");
		assert_eq!(result.namespace, namespace_one.id);
	}

	#[test]
	fn test_get_flow_not_found() {
		let mut txn = create_test_admin_transaction();

		let err = CatalogStore::get_flow(&mut Transaction::Admin(&mut txn), FlowId(42)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("FlowId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
