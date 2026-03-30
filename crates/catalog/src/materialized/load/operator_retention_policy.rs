// SPDX-License-Identifier: Apache-2.0
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
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		if let Some(key) = OperatorRetentionPolicyKey::decode(&multi.key)
			&& let Some(policy) = decode_retention_policy(&multi.row)
		{
			catalog.set_operator_retention_policy(key.operator, version, Some(policy));
		}
	}

	Ok(())
}
