// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::{NamespaceId, VTableDef};
use reifydb_transaction::IntoStandardTransaction;

use crate::Catalog;

impl Catalog {
	/// Find a user-defined virtual table by name.
	/// VTables are not transactionally modified, so this just delegates to the materialized catalog.
	pub fn find_vtable_user_by_name<T: IntoStandardTransaction>(
		&self,
		_txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> Option<Arc<VTableDef>> {
		self.materialized.find_vtable_user_by_name(namespace, name)
	}

	/// List all user-defined virtual tables.
	/// VTables are not transactionally modified, so this just delegates to the materialized catalog.
	pub fn list_user_vtables(&self) -> Vec<Arc<VTableDef>> {
		self.materialized.list_vtable_user_all()
	}
}
