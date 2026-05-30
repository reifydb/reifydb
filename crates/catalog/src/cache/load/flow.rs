// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	Result,
	store::flow::shape::{
		flow,
		flow::{ID, NAME, NAMESPACE, STATUS},
	},
};

pub(crate) fn load_flows(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = FlowKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

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

	Flow {
		id,
		namespace,
		name,
		status,
	}
}
