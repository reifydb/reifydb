// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	EncodableKey,
	retention_policy::{PrimitiveRetentionPolicyKey, PrimitiveRetentionPolicyKeyRange},
};
use reifydb_transaction::standard::IntoStandardTransaction;

use super::MaterializedCatalog;
use crate::store::retention_policy::decode_retention_policy;

pub(crate) fn load_source_retention_policies(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = PrimitiveRetentionPolicyKeyRange::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next() {
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
