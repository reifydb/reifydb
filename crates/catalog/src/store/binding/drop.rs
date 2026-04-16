// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::BindingId,
	key::{binding::BindingKey, namespace_binding::NamespaceBindingKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_binding(txn: &mut AdminTransaction, binding_id: BindingId) -> Result<()> {
		let binding = CatalogStore::find_binding(&mut Transaction::Admin(&mut *txn), binding_id)?;

		if let Some(binding) = binding {
			txn.remove(&NamespaceBindingKey::encoded(binding.namespace, binding_id))?;
			txn.remove(&BindingKey::encoded(binding_id))?;
		}

		Ok(())
	}
}
