// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::namespace_already_exists,
	interface::catalog::{id::NamespaceId, namespace::NamespaceDef},
	key::namespace::NamespaceKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	CatalogStore,
	store::{
		namespace::schema::namespace::{ID, NAME, PARENT_ID, SCHEMA},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct NamespaceToCreate {
	pub namespace_fragment: Option<Fragment>,
	pub name: String,
	pub parent_id: NamespaceId,
}

impl CatalogStore {
	pub(crate) fn create_namespace(
		txn: &mut AdminTransaction,
		to_create: NamespaceToCreate,
	) -> crate::Result<NamespaceDef> {
		if let Some(namespace) = Self::find_namespace_by_name(txn, &to_create.name)? {
			return_error!(namespace_already_exists(
				to_create.namespace_fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name
			));
		}

		let namespace_id = SystemSequence::next_namespace_id(txn)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, namespace_id);
		SCHEMA.set_utf8(&mut row, NAME, &to_create.name);
		SCHEMA.set_u64(&mut row, PARENT_ID, to_create.parent_id.0);

		txn.set(&NamespaceKey::encoded(namespace_id), row)?;

		Ok(Self::get_namespace(txn, namespace_id)?)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::{CatalogStore, store::namespace::create::NamespaceToCreate};

	#[test]
	fn test_create_namespace() {
		let mut txn = create_test_admin_transaction();

		let to_create = NamespaceToCreate {
			namespace_fragment: None,
			name: "test_namespace".to_string(),
			parent_id: NamespaceId(0),
		};

		// First creation should succeed
		let result = CatalogStore::create_namespace(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, NamespaceId(1025));
		assert_eq!(result.name, "test_namespace");

		// Creating the same namespace again with `if_not_exists =
		// false` should return error
		let err = CatalogStore::create_namespace(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
