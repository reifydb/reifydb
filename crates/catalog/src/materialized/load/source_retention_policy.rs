// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::MultiVersionQueryTransaction,
	key::{EncodableKey, SourceRetentionPolicyKey, SourceRetentionPolicyKeyRange},
};

use crate::{MaterializedCatalog, store::retention_policy::decode_retention_policy};

pub(crate) fn load_source_retention_policies(
	qt: &mut impl MultiVersionQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = SourceRetentionPolicyKeyRange::full_scan();
	let policies = qt.range(range)?;

	for multi in policies {
		let version = multi.version;

		if let Some(key) = SourceRetentionPolicyKey::decode(&multi.key) {
			if let Some(policy) = decode_retention_policy(&multi.values) {
				catalog.set_source_retention_policy(key.source, version, Some(policy));
			}
		}
	}

	Ok(())
}
