// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowStatus,
			id::{NamespaceId, SourceId},
			source::Source,
		},
		store::MultiVersionRow,
	},
	key::{EncodableKey, source::SourceKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use super::CatalogCache;
use crate::{
	Result,
	store::source::shape::{
		source,
		source::{CONFIG, CONNECTOR, ID, NAME, NAMESPACE, STATUS, TARGET_NAME, TARGET_NAMESPACE},
	},
};

pub(crate) fn load_sources(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SourceKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;

		if SourceKey::decode(&multi.key).is_none() {
			continue;
		}
		let version = multi.version;
		let source = convert_source(multi);
		catalog.set_source(source.id, version, Some(source));
	}

	Ok(())
}

fn convert_source(multi: MultiVersionRow) -> Source {
	let bytes = multi.bytes;
	let id = SourceId(source::SHAPE.get::<u64>(&bytes, ID));
	let namespace = NamespaceId(source::SHAPE.get::<u64>(&bytes, NAMESPACE));
	let name = source::SHAPE.get_utf8(&bytes, NAME).to_string();
	let connector = source::SHAPE.get_utf8(&bytes, CONNECTOR).to_string();
	let config_json = source::SHAPE.get_utf8(&bytes, CONFIG);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let target_namespace = NamespaceId(source::SHAPE.get::<u64>(&bytes, TARGET_NAMESPACE));
	let target_name = source::SHAPE.get_utf8(&bytes, TARGET_NAME).to_string();
	let status = FlowStatus::from_u8(source::SHAPE.get::<u8>(&bytes, STATUS));

	Source {
		id,
		namespace,
		name,
		connector,
		config,
		target_namespace,
		target_name,
		status,
	}
}
