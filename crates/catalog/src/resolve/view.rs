// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{ViewId, resolved::ResolvedView};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogViewQueryOperations},
};

/// Resolve a view ID to a fully resolved view with namespace and identifiers
pub async fn resolve_view<T>(txn: &mut T, view_id: ViewId) -> crate::Result<ResolvedView>
where
	T: CatalogViewQueryOperations + CatalogNamespaceQueryOperations,
{
	let view_def = txn.get_view(view_id).await?;
	let resolved_namespace = resolve_namespace(txn, view_def.namespace).await?;
	let view_ident = Fragment::internal(view_def.name.clone());

	Ok(ResolvedView::new(view_ident, resolved_namespace, view_def))
}
