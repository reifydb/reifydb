// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowStatus,
			id::{NamespaceId, SinkId},
			sink::Sink,
		},
		store::MultiVersionRow,
	},
	key::catalog::SinkKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use serde_json::from_str;

use super::CatalogCache;
use crate::{Result, error::CatalogError, store::sink::shape::sink};

pub(crate) fn load_sinks(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SinkKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let sink = convert_sink(multi)?;
		catalog.set_sink(sink.id, version, Some(sink));
	}

	Ok(())
}

fn convert_sink<K>(multi: MultiVersionRow<K>) -> Result<Sink> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = SinkId(sink::get_id(&bytes));
	let namespace = NamespaceId(sink::get_namespace(&bytes));
	let name = sink::get_name(&bytes).to_string();
	let source_namespace = NamespaceId(sink::get_source_namespace(&bytes));
	let source_name = sink::get_source_name(&bytes).to_string();
	let connector = sink::get_connector(&bytes).to_string();
	let config_json = sink::get_config(&bytes);
	let config: Vec<(String, String)> = from_str(config_json).map_err(|e| CatalogError::SinkConfigCorrupt {
		name: name.clone(),
		reason: e.to_string(),
	})?;
	let status = FlowStatus::from_u8(sink::get_status(&bytes));

	Ok(Sink {
		id,
		namespace,
		name,
		source_namespace,
		source_name,
		connector,
		config,
		status,
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::CommitVersion;

	use super::*;

	#[test]
	fn test_convert_rejects_corrupt_config_json() {
		// Without this, a corrupt sink row loads into the cache with an empty config and the connector runs on
		// defaults.
		let mut row = sink::allocate();
		sink::set_id(&mut row, 7);
		sink::set_namespace(&mut row, 1);
		sink::set_name(&mut row, "broken");
		sink::set_connector(&mut row, "test");
		sink::set_config(&mut row, "not json");
		sink::set_source_namespace(&mut row, 1);
		sink::set_source_name(&mut row, "origin");
		sink::set_status(&mut row, 0);

		let multi = MultiVersionRow {
			key: (),
			bytes: row.freeze().into(),
			version: CommitVersion(1),
		};

		let err = convert_sink(multi).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_129");
	}
}
