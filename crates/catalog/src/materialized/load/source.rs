// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowStatus,
			id::{NamespaceId, SourceId},
			source::Source,
		},
		store::MultiVersionRow,
	},
	key::source::SourceKey,
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::source::shape::{
		source,
		source::{CONFIG, CONNECTOR, ID, NAME, NAMESPACE, STATUS, TARGET_NAME, TARGET_NAMESPACE},
	},
};

pub(crate) fn load_sources(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = SourceKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let source = convert_source(multi);
		catalog.set_source(source.id, version, Some(source));
	}

	Ok(())
}

fn convert_source(multi: MultiVersionRow) -> Source {
	let row = multi.row;
	let id = SourceId(source::SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(source::SHAPE.get_u64(&row, NAMESPACE));
	let name = source::SHAPE.get_utf8(&row, NAME).to_string();
	let connector = source::SHAPE.get_utf8(&row, CONNECTOR).to_string();
	let config_json = source::SHAPE.get_utf8(&row, CONFIG);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let target_namespace = NamespaceId(source::SHAPE.get_u64(&row, TARGET_NAMESPACE));
	let target_name = source::SHAPE.get_utf8(&row, TARGET_NAME).to_string();
	let status = FlowStatus::from_u8(source::SHAPE.get_u8(&row, STATUS));

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
