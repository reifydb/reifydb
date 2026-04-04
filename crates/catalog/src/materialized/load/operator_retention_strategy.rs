// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	retention_strategy::{OperatorRetentionStrategyKey, OperatorRetentionStrategyKeyRange},
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::retention_strategy::decode_retention_strategy};

pub(crate) fn load_operator_retention_strategies(
	rx: &mut Transaction<'_>,
	catalog: &MaterializedCatalog,
) -> Result<()> {
	let range = OperatorRetentionStrategyKeyRange::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		if let Some(key) = OperatorRetentionStrategyKey::decode(&multi.key)
			&& let Some(policy) = decode_retention_strategy(&multi.row)
		{
			catalog.set_operator_retention_strategy(key.operator, version, Some(policy));
		}
	}

	Ok(())
}
