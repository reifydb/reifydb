// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{handler::HandlerDef, id::HandlerId},
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_handler(rx: &mut Transaction<'_>, handler: HandlerId) -> Result<HandlerDef> {
		match Self::find_handler(rx, handler)? {
			Some(def) => Ok(def),
			None => return_internal_error!("Handler with ID {:?} not found in catalog.", handler),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{HandlerId, NamespaceId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::sumtype::SumTypeId;

	use crate::{
		CatalogStore,
		test_utils::{create_handler, create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_handler(&mut txn, "namespace_one", "handler_one", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_two", "handler_two", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_three", "handler_three", SumTypeId(0), 0, "");

		let result = CatalogStore::get_handler(&mut Transaction::Admin(&mut txn), HandlerId(2)).unwrap();

		assert_eq!(result.id, HandlerId(2));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "handler_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_handler(&mut txn, "namespace_one", "handler_one", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_two", "handler_two", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_three", "handler_three", SumTypeId(0), 0, "");

		let err = CatalogStore::get_handler(&mut Transaction::Admin(&mut txn), HandlerId(42)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("HandlerId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
