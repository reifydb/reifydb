// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, namespace::Namespace},
	key::namespace::NamespaceKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		namespace::shape::namespace::{GRPC, ID, LOCAL_NAME, NAME, PARENT_ID, SHAPE, TOKEN},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct NamespaceToCreate {
	pub namespace_fragment: Option<Fragment>,
	pub name: String,
	pub local_name: String,
	pub parent_id: NamespaceId,
	pub grpc: Option<String>,
	pub token: Option<String>,
}

impl CatalogStore {
	pub(crate) fn create_namespace(txn: &mut AdminTransaction, to_create: NamespaceToCreate) -> Result<Namespace> {
		if let Some(namespace) =
			Self::find_namespace_by_name(&mut Transaction::Admin(&mut *txn), &to_create.name)?
		{
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Namespace,
				namespace: namespace.name().to_string(),
				name: namespace.name().to_string(),
				fragment: to_create.namespace_fragment.unwrap_or_else(|| Fragment::None),
			}
			.into());
		}

		let namespace_id = SystemSequence::next_namespace_id(txn)?;

		let mut row = SHAPE.allocate();
		SHAPE.set_u64(&mut row, ID, namespace_id);
		SHAPE.set_utf8(&mut row, NAME, &to_create.name);
		SHAPE.set_u64(&mut row, PARENT_ID, to_create.parent_id.0);
		if let Some(ref grpc) = to_create.grpc {
			SHAPE.set_utf8(&mut row, GRPC, grpc);
		}
		if let Some(ref token) = to_create.token {
			SHAPE.set_utf8(&mut row, TOKEN, token);
		}
		SHAPE.set_utf8(&mut row, LOCAL_NAME, &to_create.local_name);

		txn.set(&NamespaceKey::encoded(namespace_id), row)?;

		Ok(Self::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use crate::{CatalogStore, store::namespace::create::NamespaceToCreate};

	#[test]
	fn test_create_namespace() {
		let mut txn = create_test_admin_transaction();

		let to_create = NamespaceToCreate {
			namespace_fragment: None,
			name: "test_namespace".to_string(),
			local_name: "test_namespace".to_string(),
			parent_id: NamespaceId(0),
			grpc: None,
			token: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_namespace(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id(), NamespaceId(1025));
		assert_eq!(result.name(), "test_namespace");

		// Creating the same namespace again with `if_not_exists =
		// false` should return error
		let err = CatalogStore::create_namespace(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
