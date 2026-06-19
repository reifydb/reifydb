// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	row_settings::{RowSettingsKey, RowSettingsKeyRange},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use tracing::warn;

use super::CatalogCache;
use crate::{Result, store::row_settings::decode_row_settings};

pub(crate) fn load_row_settings(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = RowSettingsKeyRange::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let Some(key) = RowSettingsKey::decode(&multi.key) else {
			warn!("Failed to decode RowSettingsKey from catalog entry, skipping");
			continue;
		};
		let Some(config) = decode_row_settings(&multi.row) else {
			warn!(?key.shape, "Failed to decode TTL config from catalog entry, skipping");
			continue;
		};
		catalog.set_row_settings(key.shape, version, Some(config));
	}

	Ok(())
}
