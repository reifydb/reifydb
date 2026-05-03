// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	retention_strategy::{ShapeRetentionStrategyKey, ShapeRetentionStrategyKeyRange},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogCache;
use crate::{Result, store::retention_strategy::decode_retention_strategy};

pub(crate) fn load_shape_retention_strategies(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = ShapeRetentionStrategyKeyRange::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		if let Some(key) = ShapeRetentionStrategyKey::decode(&multi.key)
			&& let Some(policy) = decode_retention_strategy(&multi.row)
		{
			catalog.set_shape_retention_strategy(key.shape, version, Some(policy));
		}
	}

	Ok(())
}
