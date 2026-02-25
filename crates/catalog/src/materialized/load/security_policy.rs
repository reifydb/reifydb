// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::security_policy::SecurityPolicyKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::store::security_policy::convert_security_policy;

pub(crate) fn load_security_policies(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = SecurityPolicyKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let policy_def = convert_security_policy(multi);
		catalog.set_security_policy(policy_def.id, version, Some(policy_def));
	}

	Ok(())
}
