// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, MultiVersionRow, NamespaceId, NamespaceViewKey, QueryTransaction, ViewDef, ViewId, ViewKey,
	ViewKind,
};

use crate::{
	CatalogStore,
	view::layout::{view, view_namespace},
};

impl CatalogStore {
	pub fn find_view(rx: &mut impl QueryTransaction, id: ViewId) -> crate::Result<Option<ViewDef>> {
		let Some(multi) = rx.get(&ViewKey {
			view: id,
		}
		.encode())?
		else {
			return Ok(None);
		};

		let row = multi.row;
		let id = ViewId(view::LAYOSVT.get_u64(&row, view::ID));
		let namespace = NamespaceId(view::LAYOSVT.get_u64(&row, view::NAMESPACE));
		let name = view::LAYOSVT.get_utf8(&row, view::NAME).to_string();

		let kind = match view::LAYOSVT.get_u8(&row, view::KIND) {
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

	pub fn find_view_by_name(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();
		let Some(view) =
			rx.range(NamespaceViewKey::full_scan(namespace))?.find_map(|multi: MultiVersionRow| {
				let row = &multi.row;
				let view_name = view_namespace::LAYOSVT.get_utf8(row, view_namespace::NAME);
				if name == view_name {
					Some(ViewId(view_namespace::LAYOSVT.get_u64(row, view_namespace::ID)))
				} else {
					None
				}
			})
		else {
			return Ok(None);
		};

		Ok(Some(Self::get_view(rx, view)?))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{NamespaceId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_view, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(1027), "view_two").unwrap().unwrap();
		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(1025), "some_view").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_view() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(1025), "view_four_two").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_namespace() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(2), "view_two").unwrap();
		assert!(result.is_none());
	}
}
