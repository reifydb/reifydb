// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::identity::IdentityAttributeId,
	key::{
		EncodableKey, identity_attribute::IdentityAttributeKey,
		identity_attribute_value::IdentityAttributeValueKey,
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_identity_attribute(
		txn: &mut AdminTransaction,
		attribute: IdentityAttributeId,
	) -> Result<()> {
		{
			let range = IdentityAttributeValueKey::full_scan();
			let mut stream = txn.range(range, RangeScope::All, 1024)?;
			let mut keys_to_remove = Vec::new();
			for entry in stream.by_ref() {
				let entry = entry?;
				if let Some(key) = IdentityAttributeValueKey::decode(&entry.key)
					&& key.attribute == attribute
				{
					keys_to_remove.push(key);
				}
			}
			drop(stream);
			for key in keys_to_remove {
				txn.remove(&IdentityAttributeValueKey::encoded(key.identity, key.attribute))?;
			}
		}

		txn.remove(&IdentityAttributeKey::encoded(attribute))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::value_type::ValueType;

	use crate::CatalogStore;

	#[test]
	fn test_drop_identity_attribute() {
		let mut txn = create_test_admin_transaction();
		let attribute = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		CatalogStore::drop_identity_attribute(&mut txn, attribute.id).unwrap();
		let found = CatalogStore::find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "org_id")
			.unwrap();
		assert!(found.is_none());
	}
}
