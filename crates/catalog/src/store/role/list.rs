// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::user::RoleDef, key::role::RoleKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::role::convert_role};

impl CatalogStore {
	pub(crate) fn list_all_roles(rx: &mut Transaction<'_>) -> crate::Result<Vec<RoleDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(RoleKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_role(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_list_roles() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_role(&mut txn, "admin").unwrap();
		CatalogStore::create_role(&mut txn, "editor").unwrap();
		let roles = CatalogStore::list_all_roles(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(roles.len(), 2);
	}
}
