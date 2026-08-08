// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowStatus,
			id::{NamespaceId, SinkId},
			sink::Sink,
		},
		store::MultiVersionRow,
	},
	key::sink::SinkKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use super::CatalogCache;
use crate::{Result, store::sink::shape::sink};

pub(crate) fn load_sinks(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SinkKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let sink = convert_sink(multi);
		catalog.set_sink(sink.id, version, Some(sink));
	}

	Ok(())
}

fn convert_sink(multi: MultiVersionRow) -> Sink {
	let bytes = multi.bytes;
	let id = SinkId(sink::get_id(&bytes));
	let namespace = NamespaceId(sink::get_namespace(&bytes));
	let name = sink::get_name(&bytes).to_string();
	let source_namespace = NamespaceId(sink::get_source_namespace(&bytes));
	let source_name = sink::get_source_name(&bytes).to_string();
	let connector = sink::get_connector(&bytes).to_string();
	let config_json = sink::get_config(&bytes);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let status = FlowStatus::from_u8(sink::get_status(&bytes));

	Sink {
		id,
		namespace,
		name,
		source_namespace,
		source_name,
		connector,
		config,
		status,
	}
}
