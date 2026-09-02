// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::identity::{Role, RoleId},
	key::identity::RoleKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::role::{convert_role, shape::role},
};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_role(rx: &mut Transaction<'_>, id: RoleId) -> Result<Option<Role>> {
		rx.get(&RoleKey::encoded(id))?.map(convert_role).transpose()
	}

	pub(crate) fn find_role_by_name(rx: &mut Transaction<'_>, name: &str) -> Result<Option<Role>> {
		let stream = rx.range(RoleKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			let role_name = role::get_name(EncodedCatalogRow::view(&multi.bytes));
			if name == role_name {
				return Ok(Some(convert_role(multi)?));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_test_harness::engine::create_test_admin_transaction;
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
