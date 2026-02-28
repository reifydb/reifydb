// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::policy::PolicyKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::policy::convert_policy};

pub(crate) fn load_policies(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = PolicyKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let policy_def = convert_policy(multi);
		catalog.set_policy(policy_def.id, version, Some(policy_def));
	}

	Ok(())
}
