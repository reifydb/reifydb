// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::{id::NamespaceId, vtable::VTable};
use reifydb_transaction::transaction::Transaction;

use crate::catalog::Catalog;

impl Catalog {
	pub fn find_vtable_user_by_name(
		&self,
		_txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Option<Arc<VTable>> {
		self.cache.find_vtable_user_by_name(namespace, name)
	}

	pub fn list_user_vtables(&self) -> Vec<Arc<VTable>> {
		self.cache.list_vtable_user_all()
	}
}
