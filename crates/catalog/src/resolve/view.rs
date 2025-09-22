// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ViewId,
	identifier::{NamespaceIdentifier, ViewIdentifier},
	resolved::{ResolvedNamespace, ResolvedView},
};
use reifydb_type::Fragment;

use crate::transaction::{CatalogNamespaceQueryOperations, CatalogViewQueryOperations};

/// Resolve a view ID to a fully resolved view with namespace and identifiers
pub fn resolve_view<'a, T>(txn: &mut T, view_id: ViewId) -> crate::Result<ResolvedView<'a>>
where
	T: CatalogViewQueryOperations + CatalogNamespaceQueryOperations,
{
	let view_def = txn.get_view(view_id)?;
	let namespace_def = txn.get_namespace(view_def.namespace)?;
	let namespace_ident = NamespaceIdentifier::new(Fragment::owned_internal(namespace_def.name.clone()));
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace_def);

	let view_ident = ViewIdentifier::new(
		Fragment::owned_internal(resolved_namespace.name().to_string()),
		Fragment::owned_internal(view_def.name.clone()),
	);

	Ok(ResolvedView::new(view_ident, resolved_namespace, view_def))
}
