// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{handler::Handler, id::NamespaceId},
	key::{
		catalog::{HandlerKey, VariantHandlerKey},
		namespace::NamespaceHandlerKey,
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{fragment::Fragment, value::sumtype::VariantRef};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		handler::shape::{handler as handler_shape, handler_namespace},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct HandlerToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub variant: VariantRef,
	pub body_source: String,
}

impl CatalogStore {
	pub(crate) fn create_handler(txn: &mut AdminTransaction, to_create: HandlerToCreate) -> Result<Handler> {
		let namespace_id = to_create.namespace;

		if let Some(_existing) = CatalogStore::find_handler_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Handler,
				namespace: namespace.name().to_string(),
				name: to_create.name.text().to_string(),
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let handler_id = SystemSequence::next_handler_id(txn)?;

		let mut row = handler_shape::allocate();
		handler_shape::set_id(&mut row, u64::from(handler_id));
		handler_shape::set_namespace(&mut row, u64::from(namespace_id));
		handler_shape::set_name(&mut row, to_create.name.text());
		handler_shape::set_on_sumtype_id(&mut row, u64::from(to_create.variant.sumtype_id));
		handler_shape::set_on_variant_tag(&mut row, to_create.variant.variant_tag);
		handler_shape::set_body_source(&mut row, &to_create.body_source);

		txn.set(&HandlerKey::encoded(handler_id), row.freeze())?;

		let mut ns_row = handler_namespace::allocate();
		handler_namespace::set_id(&mut ns_row, u64::from(handler_id));
		handler_namespace::set_name(&mut ns_row, to_create.name.text());

		txn.set(&NamespaceHandlerKey::encoded(namespace_id, handler_id), ns_row.freeze())?;

		let mut var_row = handler_namespace::allocate();
		handler_namespace::set_id(&mut var_row, u64::from(handler_id));
		handler_namespace::set_name(&mut var_row, to_create.name.text());

		txn.set(
			&VariantHandlerKey::encoded(
				namespace_id,
				to_create.variant.sumtype_id,
				to_create.variant.variant_tag,
				handler_id,
			),
			var_row.freeze(),
		)?;

		Ok(Handler {
			id: handler_id,
			namespace: namespace_id,
			name: to_create.name.text().to_string(),
			variant: to_create.variant,
			body_source: to_create.body_source,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_core::{
		interface::catalog::id::{HandlerId, NamespaceId},
		key::namespace::NamespaceHandlerKey,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::{
		fragment::Fragment,
		value::sumtype::{SumTypeId, VariantRef},
	};

	use crate::{
		CatalogStore,
		store::handler::{create::HandlerToCreate, shape::handler_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_handler() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = HandlerToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("test_handler"),
			variant: VariantRef {
				sumtype_id: SumTypeId(0),
				variant_tag: 1,
			},
			body_source: "return 42;".to_string(),
		};

		let result = CatalogStore::create_handler(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, HandlerId(16385));
		assert_eq!(result.namespace, NamespaceId(16385));
		assert_eq!(result.name, "test_handler");
		assert_eq!(result.variant.sumtype_id, SumTypeId(0));
		assert_eq!(result.variant.variant_tag, 1);
		assert_eq!(result.body_source, "return 42;");

		let err = CatalogStore::create_handler(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_handler_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = HandlerToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("test_handler"),
			variant: VariantRef {
				sumtype_id: SumTypeId(0),
				variant_tag: 0,
			},
			body_source: String::new(),
		};
		CatalogStore::create_handler(&mut txn, to_create).unwrap(); // HandlerId(16385)

		let to_create = HandlerToCreate {
			namespace: namespace.id(),
			name: Fragment::internal("another_handler"),
			variant: VariantRef {
				sumtype_id: SumTypeId(0),
				variant_tag: 0,
			},
			body_source: String::new(),
		};
		CatalogStore::create_handler(&mut txn, to_create).unwrap(); // HandlerId(16386)

		let links: Vec<_> = txn
			.range(NamespaceHandlerKey::full_scan(namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Keys are descending, so the higher id sorts first.
		let link = &links[0];
		let bytes = &link.bytes;
		assert_eq!(handler_namespace::get_id(EncodedCatalogRow::view(bytes)), 16386);
		assert_eq!(handler_namespace::get_name(EncodedCatalogRow::view(bytes)), "another_handler");

		let link = &links[1];
		let bytes = &link.bytes;
		assert_eq!(handler_namespace::get_id(EncodedCatalogRow::view(bytes)), 16385);
		assert_eq!(handler_namespace::get_name(EncodedCatalogRow::view(bytes)), "test_handler");
	}
}
