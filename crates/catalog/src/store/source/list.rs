// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SourceId},
		source::Source,
	},
	key::source::SourceKey,
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use crate::{CatalogStore, Result, store::source::shape::source};

impl CatalogStore {
	pub(crate) fn list_sources_all(rx: &mut Transaction<'_>) -> Result<Vec<Source>> {
		let mut result = Vec::new();

		let stream = rx.range(SourceKey::full_scan(), 1024)?;

		for entry in stream {
			let entry = entry?;
			let row = &entry.row;

			let id = SourceId(source::SHAPE.get_u64(row, source::ID));
			let namespace = NamespaceId(source::SHAPE.get_u64(row, source::NAMESPACE));
			let name = source::SHAPE.get_utf8(row, source::NAME).to_string();
			let connector = source::SHAPE.get_utf8(row, source::CONNECTOR).to_string();
			let config_json = source::SHAPE.get_utf8(row, source::CONFIG);
			let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
			let target_namespace = NamespaceId(source::SHAPE.get_u64(row, source::TARGET_NAMESPACE));
			let target_name = source::SHAPE.get_utf8(row, source::TARGET_NAME).to_string();
			let status_u8 = source::SHAPE.get_u8(row, source::STATUS);
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
