// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::key::{any::TaggedKey, operator_retention::OperatorRetentionKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use tracing::warn;

use super::CatalogCache;
use crate::{Result, store::operator_retention::decode_operator_retention};

pub(crate) fn load_operator_retention(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = OperatorRetentionKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let TaggedKey::OperatorRetention(key) = &multi.key else {
			warn!("Failed to decode OperatorRetentionKey from catalog entry, skipping");
			continue;
		};
		let Some(retention) = decode_operator_retention(EncodedCatalogRow::view(&multi.bytes)) else {
			warn!(?key.operator, "Failed to decode operator retention from catalog entry, skipping");
			continue;
		};
		catalog.set_operator_retention(key.operator, version, Some(retention));
	}

	Ok(())
}
