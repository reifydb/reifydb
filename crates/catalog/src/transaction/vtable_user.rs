// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{NamespaceId, VTableDef};

use super::MaterializedCatalogTransaction;

/// Query operations for user-defined virtual tables.
pub trait CatalogTableVirtualUserQueryOperations {
	/// Find a user-defined virtual table by namespace and name.
	fn find_vtable_user_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Arc<VTableDef>>;
}

impl<T: MaterializedCatalogTransaction> CatalogTableVirtualUserQueryOperations for T {
	fn find_vtable_user_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Arc<VTableDef>> {
		self.catalog().find_vtable_user_by_name(namespace, name)
	}
}
