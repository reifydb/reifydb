// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::NamespaceId, key::namespace::NamespaceKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore, Result,
	store::namespace::schema::namespace::{GRPC, ID, NAME, PARENT_ID, SCHEMA},
};

impl CatalogStore {
	pub(crate) fn update_namespace_grpc(
		txn: &mut AdminTransaction,
		namespace_id: NamespaceId,
		grpc: Option<String>,
	) -> Result<()> {
		let existing = Self::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, existing.id().0);
		SCHEMA.set_utf8(&mut row, NAME, existing.name());
		SCHEMA.set_u64(&mut row, PARENT_ID, existing.parent_id().0);
		if let Some(ref grpc) = grpc {
			SCHEMA.set_utf8(&mut row, GRPC, grpc);
		}

		txn.set(&NamespaceKey::encoded(namespace_id), row)?;
		Ok(())
	}
}
