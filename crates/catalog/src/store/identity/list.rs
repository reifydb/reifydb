// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::Identity, key::identity::IdentityKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::identity::convert_identity};

impl CatalogStore {
	pub(crate) fn list_all_identities(rx: &mut Transaction<'_>) -> Result<Vec<Identity>> {
		let mut result = Vec::new();
		let mut stream = rx.range(IdentityKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_identity(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_list_identities() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity(&mut txn, "alice").unwrap();
		CatalogStore::create_identity(&mut txn, "bob").unwrap();
		let identities = CatalogStore::list_all_identities(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(identities.len(), 2);
	}
}
