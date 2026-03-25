// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::IdentityDef, key::identity::IdentityKey};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use crate::{
	CatalogStore, Result,
	store::identity::{convert_identity, schema::identity},
};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_identity(rx: &mut Transaction<'_>, id: IdentityId) -> Result<Option<IdentityDef>> {
		Ok(rx.get(&IdentityKey::encoded(id))?.map(convert_identity))
	}

	pub(crate) fn find_identity_by_name(rx: &mut Transaction<'_>, name: &str) -> Result<Option<IdentityDef>> {
		let mut stream = rx.range(IdentityKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let identity_name = identity::SCHEMA.get_utf8(&multi.row, identity::NAME);
			if name == identity_name {
				return Ok(Some(convert_identity(multi)));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_find_identity_by_name() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity(&mut txn, "alice").unwrap();
		let found = CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut txn), "alice").unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().name, "alice");
	}

	#[test]
	fn test_find_identity_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let found =
			CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut txn), "nonexistent").unwrap();
		assert!(found.is_none());
	}
}
