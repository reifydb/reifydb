// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::identity_role::IdentityRoleKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::identity_role::convert_identity_role};

pub(crate) fn load_identity_roles(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = IdentityRoleKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;
		let ir_def = convert_identity_role(multi);
		catalog.set_identity_role(ir_def.identity, ir_def.role_id, version, Some(ir_def));
	}

	Ok(())
}
