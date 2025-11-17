// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{FlowEdgeDef, FlowEdgeId, QueryTransaction},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_flow_edge(txn: &mut impl QueryTransaction, edge_id: FlowEdgeId) -> crate::Result<FlowEdgeDef> {
		CatalogStore::find_flow_edge(txn, edge_id)?.ok_or_else(|| {
			Error(internal!(
				"Flow edge with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				edge_id
			))
		})
	}
}
