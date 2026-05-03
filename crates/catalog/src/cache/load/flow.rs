// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::{Flow, FlowId, FlowStatus},
			id::NamespaceId,
		},
		store::MultiVersionRow,
	},
	key::flow::FlowKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::duration::Duration;

use super::CatalogCache;
use crate::{
	Result,
	store::flow::shape::{
		flow,
		flow::{ID, NAME, NAMESPACE, STATUS, TICK_NANOS},
	},
};

pub(crate) fn load_flows(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = FlowKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let flow = convert_flow(multi);
		catalog.set_flow(flow.id, version, Some(flow));
	}

	Ok(())
}

fn convert_flow(multi: MultiVersionRow) -> Flow {
	let row = multi.row;
	let id = FlowId(flow::SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(flow::SHAPE.get_u64(&row, NAMESPACE));
	let name = flow::SHAPE.get_utf8(&row, NAME).to_string();
	let status = FlowStatus::from_u8(flow::SHAPE.get_u8(&row, STATUS));
	let tick_nanos = flow::SHAPE.get_u64(&row, TICK_NANOS);
	let tick = if tick_nanos > 0 {
		Some(Duration::from_nanoseconds(tick_nanos as i64).unwrap())
	} else {
		None
	};

	Flow {
		id,
		namespace,
		name,
		status,
		tick,
	}
}
