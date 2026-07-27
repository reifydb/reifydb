// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::flow::Flow,
		store::MultiVersionRow,
	},
	key::flow::FlowKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	Result,
	store::flow::decode_flow,
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
	decode_flow(&multi.row)
}
