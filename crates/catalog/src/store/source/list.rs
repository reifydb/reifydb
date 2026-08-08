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

			let id = SourceId(source::SHAPE.get::<u64>(bytes, source::ID));
			let namespace = NamespaceId(source::SHAPE.get::<u64>(bytes, source::NAMESPACE));
			let name = source::SHAPE.get_utf8(bytes, source::NAME).to_string();
			let connector = source::SHAPE.get_utf8(bytes, source::CONNECTOR).to_string();
			let config_json = source::SHAPE.get_utf8(bytes, source::CONFIG);
			let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
			let target_namespace = NamespaceId(source::SHAPE.get::<u64>(bytes, source::TARGET_NAMESPACE));
			let target_name = source::SHAPE.get_utf8(bytes, source::TARGET_NAME).to_string();
			let status_u8 = source::SHAPE.get::<u8>(bytes, source::STATUS);
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
