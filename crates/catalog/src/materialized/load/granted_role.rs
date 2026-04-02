// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::granted_role::GrantedRoleKey;
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{Result, store::granted_role::convert_granted_role};

pub(crate) fn load_granted_roles(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = GrantedRoleKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let ir_def = convert_granted_role(multi);
		catalog.set_granted_role(ir_def.identity, ir_def.role_id, version, Some(ir_def));
	}

	Ok(())
}
