// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::identity::IdentityAttribute, key::identity_attribute::IdentityAttributeKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		identity_attribute::shape::identity_attribute::{ID, NAME, SHAPE, VALUE_TYPE},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_identity_attribute(
		txn: &mut AdminTransaction,
		name: &str,
		value_type: ValueType,
	) -> Result<IdentityAttribute> {
		if (Self::find_identity_attribute_by_name(&mut Transaction::Admin(&mut *txn), name)?).is_some() {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::IdentityAttribute,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into());
		}

		let attribute_id = SystemSequence::next_identity_attribute_id(txn)?;

		let mut row = SHAPE.allocate();
		SHAPE.set_u64(&mut row, ID, attribute_id);
		SHAPE.set_utf8(&mut row, NAME, name);
		SHAPE.set_u8(&mut row, VALUE_TYPE, value_type.to_u8());

		txn.set(&IdentityAttributeKey::encoded(attribute_id), row)?;

		Ok(IdentityAttribute {
			id: attribute_id,
			name: name.to_string(),
			value_type,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::value_type::ValueType;

	use crate::CatalogStore;

	#[test]
	fn test_create_identity_attribute() {
		let mut txn = create_test_admin_transaction();
		let attribute = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		assert_eq!(attribute.name, "org_id");
		assert_eq!(attribute.value_type, ValueType::Utf8);
	}

	#[test]
	fn test_create_identity_attribute_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let err = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_090");
	}

	#[test]
	fn test_create_identity_attribute_value_type_round_trip() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let found = CatalogStore::find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "org_id")
			.unwrap()
			.unwrap();
		assert_eq!(found.value_type, ValueType::Utf8);
	}
}
