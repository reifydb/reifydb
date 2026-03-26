// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::{FlowDef, FlowId, FlowStatus},
			id::NamespaceId,
		},
		store::MultiVersionRow,
	},
	key::flow::FlowKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::duration::Duration;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::flow::schema::{
		flow,
		flow::{ID, NAME, NAMESPACE, STATUS, TICK_NANOS},
	},
};

pub(crate) fn load_flows(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
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

fn convert_flow(multi: MultiVersionRow) -> FlowDef {
	let row = multi.row;
	let id = FlowId(flow::SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(flow::SCHEMA.get_u64(&row, NAMESPACE));
	let name = flow::SCHEMA.get_utf8(&row, NAME).to_string();
	let status = FlowStatus::from_u8(flow::SCHEMA.get_u8(&row, STATUS));
	let tick_nanos = flow::SCHEMA.get_u64(&row, TICK_NANOS);
	let tick = if tick_nanos > 0 {
		Some(Duration::from_nanoseconds(tick_nanos as i64))
	} else {
		None
	};

	FlowDef {
		id,
		namespace,
		name,
		status,
		tick,
	}
}
