// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::flow::{Flow, FlowEntry},
		store::MultiVersionRow,
	},
	key::flow::FlowKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{CatalogStore, Result, store::flow::decode_flow};

pub(crate) fn load_flows(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = FlowKey::full_scan();
	let mut loaded = Vec::new();
	for entry in rx.range(range, RangeScope::All, 1024)? {
		let multi = entry?;
		let version = multi.version;
		loaded.push((version, convert_flow(multi)));
	}

	for (version, flow) in loaded {
		let dag = CatalogStore::load_flow_dag(rx, flow.id)?;
		let id = flow.id;
		catalog.set_flow(
			id,
			version,
			Some(FlowEntry {
				flow,
				dag,
			}),
		);
	}

	Ok(())
}

fn convert_flow<K>(multi: MultiVersionRow<K>) -> Flow {
	decode_flow(EncodedCatalogRow::view(&multi.bytes))
}
