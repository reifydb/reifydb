// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SinkId},
		sink::Sink,
	},
	key::sink::SinkKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use crate::{CatalogStore, Result, store::sink::shape::sink};

impl CatalogStore {
	pub(crate) fn list_sinks_all(rx: &mut Transaction<'_>) -> Result<Vec<Sink>> {
		let mut result = Vec::new();

		let stream = rx.range(SinkKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			let bytes = EncodedCatalogRow::view(&entry.bytes);

			let id = SinkId(sink::get_id(bytes));
			let namespace = NamespaceId(sink::get_namespace(bytes));
			let name = sink::get_name(bytes).to_string();
			let source_namespace = NamespaceId(sink::get_source_namespace(bytes));
			let source_name = sink::get_source_name(bytes).to_string();
			let connector = sink::get_connector(bytes).to_string();
			let config_json = sink::get_config(bytes);
			let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
			let status_u8 = sink::get_status(bytes);
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
