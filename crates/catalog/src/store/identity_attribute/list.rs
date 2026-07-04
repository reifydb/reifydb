// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::identity::IdentityAttribute, key::identity_attribute::IdentityAttributeKey};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::identity_attribute::convert_identity_attribute};

impl CatalogStore {
	pub(crate) fn list_all_identity_attributes(rx: &mut Transaction<'_>) -> Result<Vec<IdentityAttribute>> {
		let mut result = Vec::new();
		let stream = rx.range(IdentityAttributeKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			result.push(convert_identity_attribute(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::value_type::ValueType;

	use crate::CatalogStore;

	#[test]
	fn test_list_identity_attributes() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		CatalogStore::create_identity_attribute(&mut txn, "tier", ValueType::Utf8).unwrap();
		let attributes = CatalogStore::list_all_identity_attributes(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(attributes.len(), 2);
	}
}
