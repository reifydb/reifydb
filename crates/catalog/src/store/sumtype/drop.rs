// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::sumtype::SumTypeId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_sumtype(txn: &mut AdminTransaction, sumtype: SumTypeId) -> Result<()> {
		if let Some(sumtype_def) = Self::find_sumtype(&mut Transaction::Admin(&mut *txn), sumtype)? {
			txn.remove(&NamespaceSumTypeKey::encoded(sumtype_def.namespace, sumtype))?;
		}

		txn.remove(&SumTypeKey::encoded(sumtype))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::sumtype::SumTypeId;

	use crate::{CatalogStore, test_utils::ensure_test_sumtype};

	#[test]
	fn test_drop_sumtype() {
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_sumtype(&mut txn);

		// Verify it exists
		let found = CatalogStore::find_sumtype(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		// Drop it
		CatalogStore::drop_sumtype(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_sumtype(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_sumtype() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent sumtype should not error
		let non_existent = SumTypeId(999999);
		let result = CatalogStore::drop_sumtype(&mut txn, non_existent);
		assert!(result.is_ok());
	}
}
