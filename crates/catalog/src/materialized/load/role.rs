// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::role::RoleKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::role::convert_role};

pub(crate) fn load_roles(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = RoleKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let role = convert_role(multi);
		catalog.set_role(role.id, version, Some(role));
	}

	Ok(())
}
