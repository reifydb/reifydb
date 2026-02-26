// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::role::RoleKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::store::role::convert_role;

pub(crate) fn load_roles(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = RoleKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let role_def = convert_role(multi);
		catalog.set_role(role_def.id, version, Some(role_def));
	}

	Ok(())
}
