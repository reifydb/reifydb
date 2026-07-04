// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::identity::{IdentityAttribute, IdentityAttributeId},
	key::identity_attribute::IdentityAttributeKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::identity_attribute::{convert_identity_attribute, shape::identity_attribute},
};

impl CatalogStore {
	pub(crate) fn find_identity_attribute(
		rx: &mut Transaction<'_>,
		id: IdentityAttributeId,
	) -> Result<Option<IdentityAttribute>> {
		Ok(rx.get(&IdentityAttributeKey::encoded(id))?.map(convert_identity_attribute))
	}

	pub(crate) fn find_identity_attribute_by_name(
		rx: &mut Transaction<'_>,
		name: &str,
	) -> Result<Option<IdentityAttribute>> {
		let stream = rx.range(IdentityAttributeKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			let attribute_name = identity_attribute::SHAPE.get_utf8(&multi.row, identity_attribute::NAME);
			if name == attribute_name {
				return Ok(Some(convert_identity_attribute(multi)));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::value_type::ValueType;

	use crate::CatalogStore;

	#[test]
	fn test_find_identity_attribute_by_name() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let found = CatalogStore::find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "org_id")
			.unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().name, "org_id");
	}

	#[test]
	fn test_find_identity_attribute_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let found =
			CatalogStore::find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "nonexistent")
				.unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_find_identity_attribute_by_id() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let found =
			CatalogStore::find_identity_attribute(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().id, created.id);
	}
}
