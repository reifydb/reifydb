// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		flow::FlowId,
		id::{NamespaceId, WindowedViewGroupId},
		windowed_view_group::WindowedViewGroup,
	},
	key::{catalog::WindowedViewGroupKey, namespace::NamespaceWindowedViewGroupKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::windowed_view_group::shape::{deserialize_members, windowed_view_group, windowed_view_group_namespace},
};

impl CatalogStore {
	pub(crate) fn find_windowed_view_group(
		rx: &mut Transaction<'_>,
		id: WindowedViewGroupId,
	) -> Result<Option<WindowedViewGroup>> {
		let Some(multi) = rx.get(&[WindowedViewGroupKey::new(id).into()])?.pop().flatten() else {
			return Ok(None);
		};

		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
		Ok(Some(WindowedViewGroup {
			id: WindowedViewGroupId(windowed_view_group::get_id(&bytes)),
			namespace: NamespaceId(windowed_view_group::get_namespace(&bytes)),
			name: windowed_view_group::get_name(&bytes).to_string(),
			flow: FlowId(windowed_view_group::get_flow(&bytes)),
			members: deserialize_members(&windowed_view_group::get_members(&bytes)),
		}))
	}

	pub(crate) fn find_windowed_view_group_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<WindowedViewGroup>> {
		let name = name.as_ref();
		let mut stream =
			rx.range(NamespaceWindowedViewGroupKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_group = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let bytes = EncodedCatalogRow::view(&multi.bytes);
			let group_name = windowed_view_group_namespace::get_name(bytes);
			if name == group_name {
				found_group = Some(WindowedViewGroupId(windowed_view_group_namespace::get_id(bytes)));
				break;
			}
		}

		drop(stream);

		let Some(group) = found_group else {
			return Ok(None);
		};

		Ok(Some(Self::get_windowed_view_group(rx, group)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::ViewId;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_windowed_view_group, ensure_test_namespace},
	};

	#[test]
	fn test_find_windowed_view_group_by_name_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_windowed_view_group(&mut txn, "namespace_one", "group_one", &[ViewId(1)]);
		create_windowed_view_group(&mut txn, "namespace_two", "group_two", &[ViewId(2), ViewId(3)]);

		let result = CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace_two.id(),
			"group_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.name, "group_two");
		assert_eq!(result.namespace, namespace_two.id());
		assert_eq!(result.members, vec![ViewId(2), ViewId(3)]);
	}

	#[test]
	fn test_find_windowed_view_group_by_name_empty() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"some_group",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_windowed_view_group_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_windowed_view_group(&mut txn, "test_namespace", "group_one", &[ViewId(1)]);
		create_windowed_view_group(&mut txn, "test_namespace", "group_two", &[ViewId(2)]);

		let result = CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"group_three",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_windowed_view_group_by_name_different_namespace() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_windowed_view_group(&mut txn, "namespace_one", "my_group", &[ViewId(1)]);

		let result = CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace_two.id(),
			"my_group",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_windowed_view_group_by_name_case_sensitive() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_windowed_view_group(&mut txn, "test_namespace", "MyGroup", &[ViewId(1)]);

		let result = CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"mygroup",
		)
		.unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"MyGroup",
		)
		.unwrap();
		assert!(result.is_some());
	}

	#[test]
	fn test_find_windowed_view_group_by_id() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let group = create_windowed_view_group(&mut txn, "test_namespace", "test_group", &[ViewId(4)]);

		let result = CatalogStore::find_windowed_view_group(&mut Transaction::Admin(&mut txn), group.id)
			.unwrap()
			.unwrap();
		assert_eq!(result.id, group.id);
		assert_eq!(result.name, "test_group");
		assert_eq!(result.members, vec![ViewId(4)]);
	}

	#[test]
	fn test_find_windowed_view_group_by_id_not_found() {
		let mut txn = create_test_admin_transaction();

		let result =
			CatalogStore::find_windowed_view_group(&mut Transaction::Admin(&mut txn), 999.into()).unwrap();
		assert!(result.is_none());
	}
}
