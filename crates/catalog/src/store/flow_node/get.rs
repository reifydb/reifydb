// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{FlowNodeDef, FlowNodeId, QueryTransaction},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_flow_node(txn: &mut impl QueryTransaction, node_id: FlowNodeId) -> crate::Result<FlowNodeDef> {
		CatalogStore::find_flow_node(txn, node_id)?.ok_or_else(|| {
			Error(internal!(
				"Flow node with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				node_id
			))
		})
	}
}
