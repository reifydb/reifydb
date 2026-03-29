// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	retention_policy::{ShapeRetentionPolicyKey, ShapeRetentionPolicyKeyRange},
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::retention_policy::decode_retention_policy};

pub(crate) fn load_shape_retention_policies(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = ShapeRetentionPolicyKeyRange::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		if let Some(key) = ShapeRetentionPolicyKey::decode(&multi.key) {
			if let Some(policy) = decode_retention_policy(&multi.row) {
				catalog.set_shape_retention_policy(key.shape, version, Some(policy));
			}
		}
	}

	Ok(())
}
