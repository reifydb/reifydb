// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowStatus,
			id::{NamespaceId, SourceId},
			source::Source,
		},
		store::MultiVersionRow,
	},
	key::{catalog::SourceKey, typed::key::Key},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use super::CatalogCache;
use crate::{Result, store::source::shape::source};

pub(crate) fn load_sources(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SourceKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;

		if SourceKey::decode(&multi.key).is_none() {
			continue;
		}
		let version = multi.version;
		let source = convert_source(multi)?;
		catalog.set_source(source.id, version, Some(source));
	}

	Ok(())
}

fn convert_source(multi: MultiVersionRow) -> Result<Source> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = SourceId(source::get_id(&bytes));
	let namespace = NamespaceId(source::get_namespace(&bytes));
	let name = source::get_name(&bytes).to_string();
	let connector = source::get_connector(&bytes).to_string();
	let config_json = source::get_config(&bytes);
	let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
	let target_namespace = NamespaceId(source::get_target_namespace(&bytes));
	let target_name = source::get_target_name(&bytes).to_string();
	let status = FlowStatus::from_u8(source::get_status(&bytes));

	Ok(Source {
		id,
		namespace,
		name,
		connector,
		config,
		target_namespace,
		target_name,
		status,
	})
}
