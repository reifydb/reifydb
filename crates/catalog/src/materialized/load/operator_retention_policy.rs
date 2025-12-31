// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::key::{EncodableKey, OperatorRetentionPolicyKey, OperatorRetentionPolicyKeyRange};
use reifydb_transaction::IntoStandardTransaction;

use crate::{MaterializedCatalog, store::retention_policy::decode_retention_policy};

pub(crate) async fn load_operator_retention_policies(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = OperatorRetentionPolicyKeyRange::full_scan();
	let batch = txn.range_batch(range, 1024).await?;

	for multi in batch.items {
		let version = multi.version;

		if let Some(key) = OperatorRetentionPolicyKey::decode(&multi.key) {
			if let Some(policy) = decode_retention_policy(&multi.values) {
				catalog.set_operator_retention_policy(key.operator, version, Some(policy));
			}
		}
	}

	Ok(())
}
