// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	TableId,
	identifier::{NamespaceIdentifier, TableIdentifier},
	resolved::{ResolvedNamespace, ResolvedTable},
};
use reifydb_type::Fragment;

use crate::transaction::{CatalogNamespaceQueryOperations, CatalogTableQueryOperations};

/// Resolve a table ID to a fully resolved table with namespace and identifiers
pub fn resolve_table<'a, T>(txn: &mut T, table_id: TableId) -> crate::Result<ResolvedTable<'a>>
where
	T: CatalogTableQueryOperations + CatalogNamespaceQueryOperations,
{
	let table_def = txn.get_table(table_id)?;
	let namespace_def = txn.get_namespace(table_def.namespace)?;
	let namespace_ident = NamespaceIdentifier::new(Fragment::owned_internal(namespace_def.name.clone()));
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace_def);

	let table_ident = TableIdentifier::new(
		Fragment::owned_internal(resolved_namespace.name().to_string()),
		Fragment::owned_internal(table_def.name.clone()),
	);

	Ok(ResolvedTable::new(table_ident, resolved_namespace, table_def))
}
