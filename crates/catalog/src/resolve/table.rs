// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{TableId, resolved::ResolvedTable};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogTableQueryOperations},
};

/// Resolve a table ID to a fully resolved table with namespace and identifiers
pub fn resolve_table<'a, T>(txn: &mut T, table_id: TableId) -> crate::Result<ResolvedTable<'a>>
where
	T: CatalogTableQueryOperations + CatalogNamespaceQueryOperations,
{
	let table_def = txn.get_table(table_id)?;
	let resolved_namespace = resolve_namespace(txn, table_def.namespace)?;
	let table_ident = Fragment::owned_internal(table_def.name.clone());

	Ok(ResolvedTable::new(table_ident, resolved_namespace, table_def))
}
