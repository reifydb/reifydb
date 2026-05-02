// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	row_ttl::{RowTtlKey, RowTtlKeyRange},
};
use reifydb_transaction::transaction::Transaction;
use tracing::warn;

use super::MaterializedCatalog;
use crate::{Result, store::ttl::decode_ttl_config};

pub(crate) fn load_row_ttls(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = RowTtlKeyRange::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let Some(key) = RowTtlKey::decode(&multi.key) else {
			warn!("Failed to decode RowTtlKey from catalog entry, skipping");
			continue;
		};
		let Some(config) = decode_ttl_config(&multi.row) else {
			warn!(?key.shape, "Failed to decode TTL config from catalog entry, skipping");
			continue;
		};
		catalog.set_row_ttl(key.shape, version, Some(config));
	}

	Ok(())
}
