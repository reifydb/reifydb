// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	FlowDef, FlowId, FlowKey, FlowStatus, MultiVersionQueryTransaction, MultiVersionValues, NamespaceId,
};

use crate::{
	MaterializedCatalog,
	store::flow::layout::{
		flow,
		flow::{ID, NAME, NAMESPACE, QUERY, STATUS},
	},
};

pub(crate) fn load_flows(
	qt: &mut impl MultiVersionQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = FlowKey::full_scan();
	let flows = qt.range(range)?;

	for multi in flows {
		let version = multi.version;
		let flow_def = convert_flow(multi);
		catalog.set_flow(flow_def.id, version, Some(flow_def));
	}

	Ok(())
}

fn convert_flow(multi: MultiVersionValues) -> FlowDef {
	let row = multi.values;
	let id = FlowId(flow::LAYOUT.get_u64(&row, ID));
	let namespace = NamespaceId(flow::LAYOUT.get_u64(&row, NAMESPACE));
	let name = flow::LAYOUT.get_utf8(&row, NAME).to_string();
	let query = flow::LAYOUT.get_blob(&row, QUERY);
	let status = FlowStatus::from_u8(flow::LAYOUT.get_u8(&row, STATUS));

	FlowDef {
		id,
		namespace,
		name,
		columns: vec![],
		query,
		dependencies: vec![],
		status,
	}
}
