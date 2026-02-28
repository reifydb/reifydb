// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	retention_policy::{OperatorRetentionPolicyKey, OperatorRetentionPolicyKeyRange},
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::retention_policy::decode_retention_policy};

pub(crate) fn load_operator_retention_policies(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = OperatorRetentionPolicyKeyRange::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		if let Some(key) = OperatorRetentionPolicyKey::decode(&multi.key) {
			if let Some(policy) = decode_retention_policy(&multi.values) {
				catalog.set_operator_retention_policy(key.operator, version, Some(policy));
			}
		}
	}

	Ok(())
}
