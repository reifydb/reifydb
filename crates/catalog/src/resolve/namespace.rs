// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{NamespaceId, resolved::ResolvedNamespace};
use reifydb_type::Fragment;

use crate::transaction::CatalogNamespaceQueryOperations;

/// Resolve a namespace ID to a fully resolved namespace with identifier
pub async fn resolve_namespace<T>(txn: &mut T, namespace_id: NamespaceId) -> crate::Result<ResolvedNamespace>
where
	T: CatalogNamespaceQueryOperations,
{
	let def = txn.get_namespace(namespace_id).await?;
	let ident = Fragment::internal(def.name.clone());
	Ok(ResolvedNamespace::new(ident, def))
}
