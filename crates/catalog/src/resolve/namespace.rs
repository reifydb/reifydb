// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{NamespaceId, resolved::ResolvedNamespace};
use reifydb_type::Fragment;

use crate::transaction::CatalogNamespaceQueryOperations;

/// Resolve a namespace ID to a fully resolved namespace with identifier
pub async fn resolve_namespace<'a, T>(txn: &mut T, namespace_id: NamespaceId) -> crate::Result<ResolvedNamespace<'a>>
where
	T: CatalogNamespaceQueryOperations,
{
	let def = txn.get_namespace(namespace_id).await?;
	let ident = Fragment::owned_internal(def.name.clone());
	Ok(ResolvedNamespace::new(ident, def))
}
