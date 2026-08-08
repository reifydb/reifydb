// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::NamespaceId, key::namespace::NamespaceKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::namespace::shape::namespace};

impl CatalogStore {
	pub(crate) fn update_namespace_grpc(
		txn: &mut AdminTransaction,
		namespace_id: NamespaceId,
		grpc: Option<String>,
	) -> Result<()> {
		let existing = Self::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;

		let mut row = namespace::allocate();
		namespace::set_id(&mut row, existing.id().0);
		namespace::set_name(&mut row, existing.name());
		namespace::set_parent_id(&mut row, existing.parent_id().0);
		if let Some(ref grpc) = grpc {
			namespace::set_grpc(&mut row, grpc);
		}
		namespace::set_local_name(&mut row, existing.local_name());

		txn.set(&NamespaceKey::encoded(namespace_id), row.freeze())?;
		Ok(())
	}
}
