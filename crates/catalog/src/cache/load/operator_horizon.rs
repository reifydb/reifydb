// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::key::{any::TaggedKey, operator_horizon::OperatorHorizonKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;
use tracing::warn;

use super::CatalogCache;
use crate::store::operator_horizon::decode_operator_horizon;

pub(crate) fn load_operator_horizon(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let range = OperatorHorizonKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let TaggedKey::OperatorHorizon(key) = &multi.key else {
			warn!("Failed to decode OperatorHorizonKey from catalog entry, skipping");
			continue;
		};
		let Some(horizon) = decode_operator_horizon(EncodedCatalogRow::view(&multi.bytes)) else {
			warn!(?key.operator, "Failed to decode operator horizon from catalog entry, skipping");
			continue;
		};
		catalog.set_operator_horizon(key.operator, version, Some(horizon));
	}

	Ok(())
}
