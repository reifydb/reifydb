// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	operator_settings::{OperatorSettingsKey, OperatorSettingsKeyRange},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use tracing::warn;

use super::CatalogCache;
use crate::{Result, store::operator_settings::decode_operator_settings};

pub(crate) fn load_operator_settings(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = OperatorSettingsKeyRange::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let Some(key) = OperatorSettingsKey::decode(&multi.key) else {
			warn!("Failed to decode OperatorSettingsKey from catalog entry, skipping");
			continue;
		};
		let Some(config) = decode_operator_settings(&multi.row) else {
			warn!(?key.operator, "Failed to decode operator settings from catalog entry, skipping");
			continue;
		};
		catalog.set_operator_settings(key.operator, version, Some(config));
	}

	Ok(())
}
