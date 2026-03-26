// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowStatus,
			id::{NamespaceId, SinkId},
			sink::SinkDef,
		},
		store::MultiVersionRow,
	},
	key::sink::SinkKey,
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::sink::schema::{
		sink,
		sink::{CONFIG, CONNECTOR, ID, NAME, NAMESPACE, SOURCE_NAME, SOURCE_NAMESPACE, STATUS},
	},
};

pub(crate) fn load_sinks(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = SinkKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let sink_def = convert_sink(multi);
		catalog.set_sink(sink_def.id, version, Some(sink_def));
	}

	Ok(())
}

fn convert_sink(multi: MultiVersionRow) -> SinkDef {
	let row = multi.row;
	let id = SinkId(sink::SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(sink::SCHEMA.get_u64(&row, NAMESPACE));
	let name = sink::SCHEMA.get_utf8(&row, NAME).to_string();
	let source_namespace = NamespaceId(sink::SCHEMA.get_u64(&row, SOURCE_NAMESPACE));
	let source_name = sink::SCHEMA.get_utf8(&row, SOURCE_NAME).to_string();
	let connector = sink::SCHEMA.get_utf8(&row, CONNECTOR).to_string();
	let config_json = sink::SCHEMA.get_utf8(&row, CONFIG);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let status = FlowStatus::from_u8(sink::SCHEMA.get_u8(&row, STATUS));

	SinkDef {
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
