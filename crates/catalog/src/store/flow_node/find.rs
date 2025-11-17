// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use flow_node::LAYOUT;
use reifydb_core::interface::{EncodableKey, FlowId, FlowNodeDef, FlowNodeId, QueryTransaction};

use crate::{CatalogStore, store::flow_node::layout::flow_node};

impl CatalogStore {
	pub fn find_flow_node(
		txn: &mut impl QueryTransaction,
		node_id: FlowNodeId,
	) -> crate::Result<Option<FlowNodeDef>> {
		let Some(multi) = txn.get(&reifydb_core::key::FlowNodeKey {
			node: node_id,
		}
		.encode())?
		else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowNodeId(LAYOUT.get_u64(&row, flow_node::ID));
		let flow = FlowId(LAYOUT.get_u64(&row, flow_node::FLOW));
		let node_type = LAYOUT.get_u8(&row, flow_node::TYPE);
		let data = LAYOUT.get_blob(&row, flow_node::DATA).clone();

		Ok(Some(FlowNodeDef {
			id,
			flow,
			node_type,
			data,
		}))
	}
}
