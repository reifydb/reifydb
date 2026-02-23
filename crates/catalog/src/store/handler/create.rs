// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{handler::HandlerDef, id::NamespaceId},
	key::{handler::HandlerKey, namespace_handler::NamespaceHandlerKey, variant_handler::VariantHandlerKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{fragment::Fragment, value::sumtype::SumTypeId};

use crate::{
	CatalogStore,
	error::{CatalogError, CatalogObjectKind},
	store::{
		handler::schema::{handler as handler_schema, handler_namespace},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct HandlerToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub on_sumtype_id: SumTypeId,
	pub on_variant_tag: u8,
	pub body_source: String,
}

impl CatalogStore {
	pub(crate) fn create_handler(
		txn: &mut AdminTransaction,
		to_create: HandlerToCreate,
	) -> crate::Result<HandlerDef> {
		let namespace_id = to_create.namespace;

		if let Some(_existing) = CatalogStore::find_handler_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Handler,
				namespace: namespace.name,
				name: to_create.name.text().to_string(),
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let handler_id = SystemSequence::next_handler_id(txn)?;

		// Write primary row
		let mut row = handler_schema::SCHEMA.allocate();
		handler_schema::SCHEMA.set_u64(&mut row, handler_schema::ID, handler_id);
		handler_schema::SCHEMA.set_u64(&mut row, handler_schema::NAMESPACE, namespace_id);
		handler_schema::SCHEMA.set_utf8(&mut row, handler_schema::NAME, to_create.name.text());
		handler_schema::SCHEMA.set_u64(&mut row, handler_schema::ON_SUMTYPE_ID, to_create.on_sumtype_id);
		handler_schema::SCHEMA.set_u8(&mut row, handler_schema::ON_VARIANT_TAG, to_create.on_variant_tag);
		handler_schema::SCHEMA.set_utf8(&mut row, handler_schema::BODY_SOURCE, &to_create.body_source);

		txn.set(&HandlerKey::encoded(handler_id), row)?;

		// Write namespace index row
		let mut ns_row = handler_namespace::SCHEMA.allocate();
		handler_namespace::SCHEMA.set_u64(&mut ns_row, handler_namespace::ID, handler_id);
		handler_namespace::SCHEMA.set_utf8(&mut ns_row, handler_namespace::NAME, to_create.name.text());

		txn.set(&NamespaceHandlerKey::encoded(namespace_id, handler_id), ns_row)?;

		// Write variant index row (empty value — key encodes all needed info)
		let mut var_row = handler_namespace::SCHEMA.allocate();
		handler_namespace::SCHEMA.set_u64(&mut var_row, handler_namespace::ID, handler_id);
		handler_namespace::SCHEMA.set_utf8(&mut var_row, handler_namespace::NAME, to_create.name.text());

		txn.set(
			&VariantHandlerKey::encoded(
				namespace_id,
				to_create.on_sumtype_id,
				to_create.on_variant_tag,
				handler_id,
			),
			var_row,
		)?;

		Ok(HandlerDef {
			id: handler_id,
			namespace: namespace_id,
			name: to_create.name.text().to_string(),
			on_sumtype_id: to_create.on_sumtype_id,
			on_variant_tag: to_create.on_variant_tag,
			body_source: to_create.body_source,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{HandlerId, NamespaceId},
		key::namespace_handler::NamespaceHandlerKey,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::{fragment::Fragment, value::sumtype::SumTypeId};

	use crate::{
		CatalogStore,
		store::handler::{create::HandlerToCreate, schema::handler_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_handler() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = HandlerToCreate {
			namespace: namespace.id,
			name: Fragment::internal("test_handler"),
			on_sumtype_id: SumTypeId(0),
			on_variant_tag: 1,
			body_source: "return 42;".to_string(),
		};

		let result = CatalogStore::create_handler(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, HandlerId(1));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_handler");
		assert_eq!(result.on_sumtype_id, SumTypeId(0));
		assert_eq!(result.on_variant_tag, 1);
		assert_eq!(result.body_source, "return 42;");

		let err = CatalogStore::create_handler(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_handler_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = HandlerToCreate {
			namespace: namespace.id,
			name: Fragment::internal("test_handler"),
			on_sumtype_id: SumTypeId(0),
			on_variant_tag: 0,
			body_source: String::new(),
		};
		CatalogStore::create_handler(&mut txn, to_create).unwrap(); // HandlerId(1)

		let to_create = HandlerToCreate {
			namespace: namespace.id,
			name: Fragment::internal("another_handler"),
			on_sumtype_id: SumTypeId(0),
			on_variant_tag: 0,
			body_source: String::new(),
		};
		CatalogStore::create_handler(&mut txn, to_create).unwrap(); // HandlerId(2)

		let links: Vec<_> = txn
			.range(NamespaceHandlerKey::full_scan(namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Descending order: HandlerId(2) encodes to smaller bytes → appears first
		let link = &links[0];
		let row = &link.values;
		assert_eq!(handler_namespace::SCHEMA.get_u64(row, handler_namespace::ID), 2);
		assert_eq!(handler_namespace::SCHEMA.get_utf8(row, handler_namespace::NAME), "another_handler");

		let link = &links[1];
		let row = &link.values;
		assert_eq!(handler_namespace::SCHEMA.get_u64(row, handler_namespace::ID), 1);
		assert_eq!(handler_namespace::SCHEMA.get_utf8(row, handler_namespace::NAME), "test_handler");
	}
}
