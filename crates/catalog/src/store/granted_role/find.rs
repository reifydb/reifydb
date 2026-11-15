// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::identity::GrantedRole, key::identity::GrantedRoleKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{error::ValueError, value::identity::IdentityId};

use crate::{CatalogStore, store::granted_role::convert_granted_role};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_roles_for_identity(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<GrantedRole>, ValueError> {
		let mut result = Vec::new();
		let range = GrantedRoleKey::identity_scan(identity);
		let stream = rx.range(range, RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			result.push(convert_granted_role(multi)?);
		}

		Ok(result)
	}
}
