// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{FlowId, resolved::ResolvedFlow};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogFlowQueryOperations, CatalogNamespaceQueryOperations},
};

/// Resolve a flow ID to a fully resolved flow with namespace and identifiers
pub async fn resolve_flow<'a, T>(txn: &mut T, flow_id: FlowId) -> crate::Result<ResolvedFlow<'a>>
where
	T: CatalogFlowQueryOperations + CatalogNamespaceQueryOperations,
{
	let flow_def = txn.get_flow(flow_id).await?;
	let resolved_namespace = resolve_namespace(txn, flow_def.namespace).await?;
	let flow_ident = Fragment::owned_internal(flow_def.name.clone());

	Ok(ResolvedFlow::new(flow_ident, resolved_namespace, flow_def))
}
