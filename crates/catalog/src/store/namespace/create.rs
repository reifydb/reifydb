// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, EncodableKey, NamespaceDef, NamespaceKey};
use reifydb_type::{OwnedFragment, diagnostic::catalog::namespace_already_exists, return_error};

use crate::{
	CatalogStore,
	store::{
		namespace::layout::namespace::{ID, LAYOUT, NAME},
		sequence::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct NamespaceToCreate {
	pub namespace_fragment: Option<OwnedFragment>,
	pub name: String,
}

impl CatalogStore {
	pub fn create_namespace(
		txn: &mut impl CommandTransaction,
		to_create: NamespaceToCreate,
	) -> crate::Result<NamespaceDef> {
		if let Some(namespace) = Self::find_namespace_by_name(txn, &to_create.name)? {
			return_error!(namespace_already_exists(to_create.namespace_fragment, &namespace.name));
		}

		let namespace_id = SystemSequence::next_namespace_id(txn)?;

		let mut row = LAYOUT.allocate();
		LAYOUT.set_u64(&mut row, ID, namespace_id);
		LAYOUT.set_utf8(&mut row, NAME, &to_create.name);

		txn.set(
			&NamespaceKey {
				namespace: namespace_id,
			}
			.encode(),
			row,
		)?;

		Ok(Self::get_namespace(txn, namespace_id)?)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::NamespaceId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::namespace::create::NamespaceToCreate};

	#[test]
	fn test_create_namespace() {
		let mut txn = create_test_command_transaction();

		let to_create = NamespaceToCreate {
			namespace_fragment: None,
			name: "test_namespace".to_string(),
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
