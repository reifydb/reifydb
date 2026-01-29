// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ViewId, view::ViewDef},
	internal,
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::error::Error;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn get_view(rx: &mut impl AsTransaction, view: ViewId) -> crate::Result<ViewDef> {
		CatalogStore::find_view(rx, view)?.ok_or_else(|| {
			Error(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				view
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_view, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::get_view(&mut txn, ViewId(1026)).unwrap();

		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let err = CatalogStore::get_view(&mut txn, ViewId(42)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ViewId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
