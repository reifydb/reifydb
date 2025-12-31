// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{FlowId, resolved::ResolvedFlow};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogFlowQueryOperations, CatalogNamespaceQueryOperations},
};

/// Resolve a flow ID to a fully resolved flow with namespace and identifiers
pub async fn resolve_flow<T>(txn: &mut T, flow_id: FlowId) -> crate::Result<ResolvedFlow>
where
	T: CatalogFlowQueryOperations + CatalogNamespaceQueryOperations,
{
	let flow_def = txn.get_flow(flow_id).await?;
	let resolved_namespace = resolve_namespace(txn, flow_def.namespace).await?;
	let flow_ident = Fragment::internal(flow_def.name.clone());

	Ok(ResolvedFlow::new(flow_ident, resolved_namespace, flow_def))
}
