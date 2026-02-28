// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::sumtype::SumTypeDef, return_internal_error};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_sumtype(rx: &mut Transaction<'_>, sumtype: SumTypeId) -> Result<SumTypeDef> {
		match Self::find_sumtype(rx, sumtype)? {
			Some(def) => Ok(def),
			None => return_internal_error!("SumType with ID {:?} not found in catalog.", sumtype),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::sumtype::SumTypeId;

	use crate::{
		CatalogStore,
		test_utils::{create_event, create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_event(&mut txn, "namespace_one", "event_one", vec![]);
		create_event(&mut txn, "namespace_two", "event_two", vec![]);
		create_event(&mut txn, "namespace_three", "event_three", vec![]);

		let result = CatalogStore::get_sumtype(&mut Transaction::Admin(&mut txn), SumTypeId(1026)).unwrap();

		assert_eq!(result.id, SumTypeId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "event_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_event(&mut txn, "namespace_one", "event_one", vec![]);
		create_event(&mut txn, "namespace_two", "event_two", vec![]);
		create_event(&mut txn, "namespace_three", "event_three", vec![]);

		let err = CatalogStore::get_sumtype(&mut Transaction::Admin(&mut txn), SumTypeId(42)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("SumTypeId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
