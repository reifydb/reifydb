// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::QueryTransaction,
	key::{EncodableKey, OperatorRetentionPolicyKey, OperatorRetentionPolicyKeyRange},
};

use crate::{MaterializedCatalog, store::retention_policy::decode_retention_policy};

pub(crate) async fn load_operator_retention_policies(
	qt: &mut impl QueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = OperatorRetentionPolicyKeyRange::full_scan();
	let batch = qt.range(range).await?;

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
