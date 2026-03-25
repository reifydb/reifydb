// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::IdentityRoleDef, key::identity_role::IdentityRoleKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use crate::{CatalogStore, Result, store::identity_role::convert_identity_role};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_roles_for_identity(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<IdentityRoleDef>> {
		let mut result = Vec::new();
		let range = IdentityRoleKey::identity_scan(identity);
		let mut stream = rx.range(range, 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_identity_role(multi));
		}

		Ok(result)
	}
}
