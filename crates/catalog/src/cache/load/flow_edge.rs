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
use crate::{Result, store::flow_edge::shape::flow_edge};

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
	let id = FlowEdgeId(flow_edge::get_id(&bytes));
	let flow = FlowId(flow_edge::get_flow(&bytes));
	let source = OperatorId(flow_edge::get_source(&bytes));
	let target = OperatorId(flow_edge::get_target(&bytes));

	FlowEdge {
		id,
		flow,
		source,
		target,
	}
}
