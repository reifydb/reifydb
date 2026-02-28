// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::user_role::UserRoleKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::user_role::convert_user_role};

pub(crate) fn load_user_roles(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = UserRoleKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let ur_def = convert_user_role(multi);
		catalog.set_user_role(ur_def.user_id, ur_def.role_id, version, Some(ur_def));
	}

	Ok(())
}
