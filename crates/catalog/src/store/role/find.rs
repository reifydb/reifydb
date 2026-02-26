// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::{RoleDef, RoleId},
	key::role::RoleKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::role::{convert_role, schema::role},
};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_role(rx: &mut Transaction<'_>, id: RoleId) -> crate::Result<Option<RoleDef>> {
		Ok(rx.get(&RoleKey::encoded(id))?.map(convert_role))
	}

	pub(crate) fn find_role_by_name(rx: &mut Transaction<'_>, name: &str) -> crate::Result<Option<RoleDef>> {
		let mut stream = rx.range(RoleKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let role_name = role::SCHEMA.get_utf8(&multi.values, role::NAME);
			if name == role_name {
				return Ok(Some(convert_role(multi)));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_find_role_by_name() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_role(&mut txn, "admin").unwrap();
		let found = CatalogStore::find_role_by_name(&mut Transaction::Admin(&mut txn), "admin").unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().name, "admin");
	}

	#[test]
	fn test_find_role_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let found = CatalogStore::find_role_by_name(&mut Transaction::Admin(&mut txn), "nonexistent").unwrap();
		assert!(found.is_none());
	}
}
