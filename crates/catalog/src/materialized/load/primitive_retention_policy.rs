// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_core::key::{EncodableKey, PrimitiveRetentionPolicyKey, PrimitiveRetentionPolicyKeyRange};
use reifydb_transaction::IntoStandardTransaction;

use crate::{MaterializedCatalog, store::retention_policy::decode_retention_policy};

pub(crate) async fn load_source_retention_policies(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = PrimitiveRetentionPolicyKeyRange::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next().await {
		let multi = entry?;
		let version = multi.version;

		if let Some(key) = PrimitiveRetentionPolicyKey::decode(&multi.key) {
			if let Some(policy) = decode_retention_policy(&multi.values) {
				catalog.set_primitive_retention_policy(key.primitive, version, Some(policy));
			}
		}
	}

	Ok(())
}
