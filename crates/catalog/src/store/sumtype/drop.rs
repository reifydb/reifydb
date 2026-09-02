// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::{catalog::SumTypeKey, namespace::NamespaceSumTypeKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::sumtype::SumTypeId;

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
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::sumtype::SumTypeId;

	use crate::{CatalogStore, test_utils::ensure_test_sumtype};

	#[test]
	fn test_drop_sumtype() {
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_sumtype(&mut txn);

		let found = CatalogStore::find_sumtype(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		CatalogStore::drop_sumtype(&mut txn, created.id).unwrap();

		let found = CatalogStore::find_sumtype(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_sumtype() {
		// Dropping a sumtype that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		let non_existent = SumTypeId(999999);
		let result = CatalogStore::drop_sumtype(&mut txn, non_existent);
		assert!(result.is_ok());
	}
}
