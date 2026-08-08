// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SourceId},
		source::Source,
	},
	key::source::SourceKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use crate::{CatalogStore, Result, store::source::shape::source};

impl CatalogStore {
	pub(crate) fn list_sources_all(rx: &mut Transaction<'_>) -> Result<Vec<Source>> {
		let mut result = Vec::new();

		let stream = rx.range(SourceKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			let bytes = &entry.bytes;

			let id = SourceId(source::get_id(bytes));
			let namespace = NamespaceId(source::get_namespace(bytes));
			let name = source::get_name(bytes).to_string();
			let connector = source::get_connector(bytes).to_string();
			let config_json = source::get_config(bytes);
			let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
			let target_namespace = NamespaceId(source::get_target_namespace(bytes));
			let target_name = source::get_target_name(bytes).to_string();
			let status_u8 = source::get_status(bytes);
			let status = FlowStatus::from_u8(status_u8);

			result.push(Source {
				id,
				name,
				namespace,
				connector,
				config,
				target_namespace,
				target_name,
				status,
			});
		}

		Ok(result)
	}
}
