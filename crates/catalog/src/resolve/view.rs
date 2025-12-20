// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ViewId, resolved::ResolvedView};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogViewQueryOperations},
};

/// Resolve a view ID to a fully resolved view with namespace and identifiers
pub async fn resolve_view<'a, T>(txn: &mut T, view_id: ViewId) -> crate::Result<ResolvedView<'a>>
where
	T: CatalogViewQueryOperations + CatalogNamespaceQueryOperations,
{
	let view_def = txn.get_view(view_id).await?;
	let resolved_namespace = resolve_namespace(txn, view_def.namespace).await?;
	let view_ident = Fragment::owned_internal(view_def.name.clone());

	Ok(ResolvedView::new(view_ident, resolved_namespace, view_def))
}
