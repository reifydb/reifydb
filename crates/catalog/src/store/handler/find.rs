// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		handler::HandlerDef,
		id::{HandlerId, NamespaceId},
	},
	key::{handler::HandlerKey, namespace_handler::NamespaceHandlerKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::handler::{handler_def_from_row, schema::handler_namespace},
};

impl CatalogStore {
	pub(crate) fn find_handler(
		rx: &mut Transaction<'_>,
		handler_id: HandlerId,
	) -> crate::Result<Option<HandlerDef>> {
		let Some(multi) = rx.get(&HandlerKey::encoded(handler_id))? else {
			return Ok(None);
		};

		Ok(Some(handler_def_from_row(&multi.values)))
	}

	pub(crate) fn find_handler_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<HandlerDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceHandlerKey::full_scan(namespace), 1024)?;

		let mut found_id = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let entry_name = handler_namespace::SCHEMA.get_utf8(row, handler_namespace::NAME);
			if name == entry_name {
				found_id =
					Some(HandlerId(handler_namespace::SCHEMA.get_u64(row, handler_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(id) = found_id else {
			return Ok(None);
		};

		Ok(Some(Self::get_handler(rx, id)?))
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

		let result = CatalogStore::find_handler_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1027),
			"handler_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id, HandlerId(2));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "handler_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_handler_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
			"some_handler",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_handler() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_handler(&mut txn, "namespace_one", "handler_one", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_two", "handler_two", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_three", "handler_three", SumTypeId(0), 0, "");

		let result = CatalogStore::find_handler_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
			"handler_nonexistent",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_namespace() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_handler(&mut txn, "namespace_one", "handler_one", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_two", "handler_two", SumTypeId(0), 0, "");
		create_handler(&mut txn, "namespace_three", "handler_three", SumTypeId(0), 0, "");

		let result = CatalogStore::find_handler_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(2),
			"handler_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}
