// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{TableId, resolved::ResolvedTable};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogTableQueryOperations},
};

/// Resolve a table ID to a fully resolved table with namespace and identifiers
pub async fn resolve_table<T>(txn: &mut T, table_id: TableId) -> crate::Result<ResolvedTable>
where
	T: CatalogTableQueryOperations + CatalogNamespaceQueryOperations,
{
	let table_def = txn.get_table(table_id).await?;
	let resolved_namespace = resolve_namespace(txn, table_def.namespace).await?;
	let table_ident = Fragment::internal(table_def.name.clone());

	Ok(ResolvedTable::new(table_ident, resolved_namespace, table_def))
}
