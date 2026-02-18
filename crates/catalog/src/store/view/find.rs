// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, ViewId},
		view::{ViewDef, ViewKind},
	},
	key::{namespace_view::NamespaceViewKey, view::ViewKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::view::schema::{view, view_namespace},
};

impl CatalogStore {
	pub(crate) fn find_view(rx: &mut Transaction<'_>, id: ViewId) -> crate::Result<Option<ViewDef>> {
		let Some(multi) = rx.get(&ViewKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = ViewId(view::SCHEMA.get_u64(&row, view::ID));
		let namespace = NamespaceId(view::SCHEMA.get_u64(&row, view::NAMESPACE));
		let name = view::SCHEMA.get_utf8(&row, view::NAME).to_string();

		let kind = match view::SCHEMA.get_u8(&row, view::KIND) {
			0 => ViewKind::Deferred,
			1 => ViewKind::Transactional,
			_ => unimplemented!(),
		};

		Ok(Some(ViewDef {
			id,
			name,
			namespace,
			kind,
			columns: Self::list_columns(rx, id)?,
			primary_key: Self::find_view_primary_key(rx, id)?,
		}))
	}

	pub(crate) fn find_view_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceViewKey::full_scan(namespace), 1024)?;

		let mut found_view = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let view_name = view_namespace::SCHEMA.get_utf8(row, view_namespace::NAME);
			if name == view_name {
				found_view = Some(ViewId(view_namespace::SCHEMA.get_u64(row, view_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(view) = found_view else {
			return Ok(None);
		};

		Ok(Some(Self::get_view(rx, view)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, ViewId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_view, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1027),
			"view_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
			"some_view",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_view() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
			"view_four_two",
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

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result =
			CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), NamespaceId(2), "view_two")
				.unwrap();
		assert!(result.is_none());
	}
}
