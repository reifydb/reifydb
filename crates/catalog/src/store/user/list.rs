// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::user::UserDef, key::user::UserKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::user::convert_user};

impl CatalogStore {
	pub(crate) fn list_all_users(rx: &mut Transaction<'_>) -> Result<Vec<UserDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(UserKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_user(multi));
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
	fn test_list_users() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_user(&mut txn, "alice").unwrap();
		CatalogStore::create_user(&mut txn, "bob").unwrap();
		let users = CatalogStore::list_all_users(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(users.len(), 2);
	}
}
