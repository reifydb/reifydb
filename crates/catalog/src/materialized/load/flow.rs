// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::{FlowDef, FlowId, FlowStatus},
			id::NamespaceId,
		},
		store::MultiVersionValues,
	},
	key::flow::FlowKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::store::flow::schema::{
	flow,
	flow::{ID, NAME, NAMESPACE, STATUS},
};

pub(crate) fn load_flows(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = FlowKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let flow_def = convert_flow(multi);
		catalog.set_flow(flow_def.id, version, Some(flow_def));
	}

	Ok(())
}

fn convert_flow(multi: MultiVersionValues) -> FlowDef {
	let row = multi.values;
	let id = FlowId(flow::SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(flow::SCHEMA.get_u64(&row, NAMESPACE));
	let name = flow::SCHEMA.get_utf8(&row, NAME).to_string();
	let status = FlowStatus::from_u8(flow::SCHEMA.get_u8(&row, STATUS));

	FlowDef {
		id,
		namespace,
		name,
		status,
	}
}
