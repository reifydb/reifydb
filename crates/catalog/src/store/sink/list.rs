// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SinkId},
		sink::Sink,
	},
	key::sink::SinkKey,
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use crate::{CatalogStore, Result, store::sink::schema::sink};

impl CatalogStore {
	pub(crate) fn list_sinks_all(rx: &mut Transaction<'_>) -> Result<Vec<Sink>> {
		let mut result = Vec::new();

		let mut stream = rx.range(SinkKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			let row = &entry.row;

			let id = SinkId(sink::SCHEMA.get_u64(row, sink::ID));
			let namespace = NamespaceId(sink::SCHEMA.get_u64(row, sink::NAMESPACE));
			let name = sink::SCHEMA.get_utf8(row, sink::NAME).to_string();
			let source_namespace = NamespaceId(sink::SCHEMA.get_u64(row, sink::SOURCE_NAMESPACE));
			let source_name = sink::SCHEMA.get_utf8(row, sink::SOURCE_NAME).to_string();
			let connector = sink::SCHEMA.get_utf8(row, sink::CONNECTOR).to_string();
			let config_json = sink::SCHEMA.get_utf8(row, sink::CONFIG);
			let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
			let status_u8 = sink::SCHEMA.get_u8(row, sink::STATUS);
			let status = FlowStatus::from_u8(status_u8);

			result.push(Sink {
				id,
				name,
				namespace,
				source_namespace,
				source_name,
				connector,
				config,
				status,
			});
		}

		Ok(result)
	}
}
