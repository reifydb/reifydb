// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::{FlowId, FlowNode, FlowNodeId},
		store::MultiVersionRow,
	},
	key::flow_node::FlowNodeKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	Result,
	store::flow_node::shape::flow_node::{self, DATA, FLOW, ID, TYPE},
};

pub(crate) fn load_flow_nodes(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = FlowNodeKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let node = convert_flow_node(multi);
		catalog.set_flow_node(node.id, version, Some(node));
	}

	Ok(())
}

fn convert_flow_node(multi: MultiVersionRow) -> FlowNode {
	let row = multi.row;
	let id = FlowNodeId(flow_node::SHAPE.get_u64(&row, ID));
	let flow = FlowId(flow_node::SHAPE.get_u64(&row, FLOW));
	let node_type = flow_node::SHAPE.get_u8(&row, TYPE);
	let data = flow_node::SHAPE.get_blob(&row, DATA).clone();

	FlowNode {
		id,
		flow,
		node_type,
		data,
	}
}
