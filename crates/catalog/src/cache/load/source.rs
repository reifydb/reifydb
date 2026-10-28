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
	key::{any::TaggedKey, catalog::SourceKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use super::CatalogCache;
use crate::{Result, error::CatalogError, store::source::shape::source};

pub(crate) fn load_sources(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SourceKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;

		if !matches!(&multi.key, TaggedKey::Source(_)) {
			continue;
		}
		let version = multi.version;
		let source = convert_source(multi)?;
		catalog.set_source(source.id, version, Some(source));
	}

	Ok(())
}

fn convert_source<K>(multi: MultiVersionRow<K>) -> Result<Source> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = SourceId(source::get_id(&bytes));
	let namespace = NamespaceId(source::get_namespace(&bytes));
	let name = source::get_name(&bytes).to_string();
	let connector = source::get_connector(&bytes).to_string();
	let config_json = source::get_config(&bytes);
	let config: Vec<(String, String)> = from_str(config_json).map_err(|e| CatalogError::SourceConfigCorrupt {
		name: name.clone(),
		reason: e.to_string(),
	})?;
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

#[cfg(test)]
mod tests {
	use reifydb_core::common::CommitVersion;

	use super::*;

	#[test]
	fn test_convert_rejects_corrupt_config_json() {
		// Without this, a corrupt source row loads into the cache with an empty config and the connector runs
		// on defaults.
		let mut row = source::allocate();
		source::set_id(&mut row, 7);
		source::set_namespace(&mut row, 1);
		source::set_name(&mut row, "broken");
		source::set_connector(&mut row, "test");
		source::set_config(&mut row, "not json");
		source::set_target_namespace(&mut row, 1);
		source::set_target_name(&mut row, "target");
		source::set_status(&mut row, 0);

		let multi = MultiVersionRow {
			key: (),
			bytes: row.freeze().into(),
			version: CommitVersion(1),
		};

		let err = convert_source(multi).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_128");
	}
}
