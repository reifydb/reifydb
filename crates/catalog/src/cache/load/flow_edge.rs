// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::{FlowEdge, FlowEdgeId, FlowId, OperatorId},
		store::MultiVersionRow,
	},
	key::flow_edge::FlowEdgeKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	Result,
	store::flow_edge::shape::flow_edge::{self, FLOW, ID, SOURCE, TARGET},
};

pub(crate) fn load_flow_edges(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = FlowEdgeKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let edge = convert_flow_edge(multi);
		catalog.set_flow_edge(edge.id, version, Some(edge));
	}

	Ok(())
}

fn convert_flow_edge(multi: MultiVersionRow) -> FlowEdge {
	let bytes = multi.bytes;
	let id = FlowEdgeId(flow_edge::SHAPE.get::<u64>(&bytes, ID));
	let flow = FlowId(flow_edge::SHAPE.get::<u64>(&bytes, FLOW));
	let source = OperatorId(flow_edge::SHAPE.get::<u64>(&bytes, SOURCE));
	let target = OperatorId(flow_edge::SHAPE.get::<u64>(&bytes, TARGET));

	FlowEdge {
		id,
		flow,
		source,
		target,
	}
}
