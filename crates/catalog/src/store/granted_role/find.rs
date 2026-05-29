// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::identity::GrantedRole, key::granted_role::GrantedRoleKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_type::value::identity::IdentityId;

use crate::{CatalogStore, Result, store::granted_role::convert_granted_role};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_roles_for_identity(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<GrantedRole>> {
		let mut result = Vec::new();
		let range = GrantedRoleKey::identity_scan(identity);
		let stream = rx.range(range, RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			result.push(convert_granted_role(multi));
		}

		Ok(result)
	}
}
