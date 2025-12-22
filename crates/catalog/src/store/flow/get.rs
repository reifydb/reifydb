// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	diagnostic::catalog::flow_not_found,
	interface::{FlowDef, FlowId, NamespaceId, QueryTransaction},
};
use reifydb_type::{Fragment, internal};

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_flow(rx: &mut impl QueryTransaction, flow: FlowId) -> crate::Result<FlowDef> {
		CatalogStore::find_flow(rx, flow).await?.ok_or_else(|| {
			Error(internal!(
				"Flow with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				flow
			))
		})
	}

	pub async fn get_flow_by_name(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<FlowDef> {
		let name_ref = name.as_ref();

		// Look up namespace name for error message
		let namespace_name = Self::find_namespace(rx, namespace)
			.await?
			.map(|s| s.name)
			.unwrap_or_else(|| format!("namespace_{}", namespace));

		CatalogStore::find_flow_by_name(rx, namespace, name_ref)
			.await?
			.ok_or_else(|| Error(flow_not_found(Fragment::None, &namespace_name, name_ref)))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_namespace},
	};

	#[tokio::test]
	fn test_get_flow_ok() {
		let mut txn = create_test_command_transaction().await;
		let namespace_one = create_namespace(&mut txn, "namespace_one").await;
		let _namespace_two = create_namespace(&mut txn, "namespace_two").await;

		create_flow(&mut txn, "namespace_one", "flow_one");
		create_flow(&mut txn, "namespace_two", "flow_two");

		let result = CatalogStore::get_flow(&mut txn, FlowId(1)).unwrap();
		assert_eq!(result.id, FlowId(1));
		assert_eq!(result.name, "flow_one");
		assert_eq!(result.namespace, namespace_one.id);
	}

	#[tokio::test]
	fn test_get_flow_not_found() {
		let mut txn = create_test_command_transaction().await;

		let err = CatalogStore::get_flow(&mut txn, FlowId(42)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("FlowId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[tokio::test]
	fn test_get_flow_by_name_ok() {
		let mut txn = create_test_command_transaction().await;
		let _namespace_one = create_namespace(&mut txn, "namespace_one").await;
		let namespace_two = create_namespace(&mut txn, "namespace_two").await;

		create_flow(&mut txn, "namespace_one", "flow_one");
		create_flow(&mut txn, "namespace_two", "flow_two");

		let result = CatalogStore::get_flow_by_name(&mut txn, namespace_two.id, "flow_two").unwrap();
		assert_eq!(result.name, "flow_two");
		assert_eq!(result.namespace, namespace_two.id);
	}

	#[tokio::test]
	fn test_get_flow_by_name_not_found() {
		let mut txn = create_test_command_transaction().await;
		let namespace = create_namespace(&mut txn, "test_namespace").await;

		create_flow(&mut txn, "test_namespace", "flow_one");

		let err = CatalogStore::get_flow_by_name(&mut txn, namespace.id, "flow_two").unwrap_err();
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_031");
		assert!(diagnostic.message.contains("flow_two"));
		assert!(diagnostic.message.contains("not found"));
	}

	#[tokio::test]
	fn test_get_flow_by_name_different_namespace() {
		let mut txn = create_test_command_transaction().await;
		let _namespace_one = create_namespace(&mut txn, "namespace_one").await;
		let namespace_two = create_namespace(&mut txn, "namespace_two").await;

		create_flow(&mut txn, "namespace_one", "my_flow");

		// Flow exists in namespace_one but we're looking in namespace_two
		let err = CatalogStore::get_flow_by_name(&mut txn, namespace_two.id, "my_flow").unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_031");
	}
}
