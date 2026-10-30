// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowId,
		id::{NamespaceId, ViewId, WindowedViewGroupId},
		windowed_view_group::WindowedViewGroup,
	},
	key::{catalog::WindowedViewGroupKey, namespace::NamespaceWindowedViewGroupKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		sequence::windowed_view_group::next_windowed_view_group_id,
		windowed_view_group::shape::{serialize_members, windowed_view_group, windowed_view_group_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct WindowedViewGroupToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub flow: FlowId,
	pub members: Vec<ViewId>,
}

impl CatalogStore {
	pub(crate) fn create_windowed_view_group(
		txn: &mut AdminTransaction,
		to_create: WindowedViewGroupToCreate,
	) -> Result<WindowedViewGroup> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_windowed_view_group(txn, namespace_id, &to_create.name)?;

		let group_id = next_windowed_view_group_id(txn)?;
		Self::store_windowed_view_group(txn, group_id, namespace_id, &to_create)?;
		Self::link_windowed_view_group_to_namespace(txn, namespace_id, group_id, to_create.name.text())?;
		Self::get_windowed_view_group(&mut Transaction::Admin(&mut *txn), group_id)
	}

	#[inline]
	fn reject_existing_windowed_view_group(
		txn: &mut AdminTransaction,
		namespace_id: NamespaceId,
		name: &Fragment,
	) -> Result<()> {
		if CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			name.text(),
		)?
		.is_none()
		{
			return Ok(());
		}
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::WindowedViewGroup,
			namespace: namespace.name().to_string(),
			name: name.text().to_string(),
			fragment: name.clone(),
		}
		.into())
	}

	fn store_windowed_view_group(
		txn: &mut AdminTransaction,
		group: WindowedViewGroupId,
		namespace: NamespaceId,
		to_create: &WindowedViewGroupToCreate,
	) -> Result<()> {
		let mut row = windowed_view_group::allocate();
		windowed_view_group::set_id(&mut row, u64::from(group));
		windowed_view_group::set_namespace(&mut row, u64::from(namespace));
		windowed_view_group::set_name(&mut row, to_create.name.text());
		windowed_view_group::set_flow(&mut row, u64::from(to_create.flow));
		windowed_view_group::set_members(&mut row, &serialize_members(&to_create.members));

		let key = WindowedViewGroupKey::new(group);
		txn.set(vec![(key.clone().into(), row.freeze().into())])?;

		Ok(())
	}

	fn link_windowed_view_group_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		group: WindowedViewGroupId,
		name: &str,
	) -> Result<()> {
		let mut row = windowed_view_group_namespace::allocate();
		windowed_view_group_namespace::set_id(&mut row, u64::from(group));
		windowed_view_group_namespace::set_name(&mut row, name);
		let key = NamespaceWindowedViewGroupKey::new(namespace, group);
		txn.set(vec![(key.clone().into(), row.freeze().into())])?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_core::{
		interface::catalog::{
			flow::FlowId,
			id::{NamespaceId, ViewId, WindowedViewGroupId},
		},
		key::namespace::NamespaceWindowedViewGroupKey,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
	use reifydb_value::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::windowed_view_group::{create::WindowedViewGroupToCreate, shape::windowed_view_group_namespace},
		test_utils::{create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_create_windowed_view_group() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = WindowedViewGroupToCreate {
			name: Fragment::internal("test_group"),
			namespace: test_namespace.id(),
			flow: FlowId(7),
			members: vec![ViewId(3), ViewId(1), ViewId(2)],
		};

		let result = CatalogStore::create_windowed_view_group(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, WindowedViewGroupId(1));
		assert_eq!(result.namespace, NamespaceId(16385));
		assert_eq!(result.name, "test_group");
		assert_eq!(result.flow, FlowId(7));
		assert_eq!(result.members, vec![ViewId(3), ViewId(1), ViewId(2)]);
	}

	#[test]
	fn test_create_windowed_view_group_duplicate() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = WindowedViewGroupToCreate {
			name: Fragment::internal("test_group"),
			namespace: test_namespace.id(),
			flow: FlowId(7),
			members: vec![ViewId(1)],
		};

		CatalogStore::create_windowed_view_group(&mut txn, to_create.clone()).unwrap();

		let err = CatalogStore::create_windowed_view_group(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_134");
	}

	#[test]
	fn test_windowed_view_group_with_fifteen_members_round_trips_in_order() {
		// Members must keep declaration order; a sorting or deduplicating encoding would break that.
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);
		let members: Vec<ViewId> =
			[9, 3, 15, 1, 12, 7, 2, 14, 5, 11, 4, 13, 6, 10, 8].into_iter().map(ViewId).collect();

		let created = CatalogStore::create_windowed_view_group(
			&mut txn,
			WindowedViewGroupToCreate {
				name: Fragment::internal("wide_group"),
				namespace: test_namespace.id(),
				flow: FlowId(1),
				members: members.clone(),
			},
		)
		.unwrap();
		assert_eq!(created.members, members);

		let found = CatalogStore::find_windowed_view_group(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.unwrap();
		assert_eq!(found.members, members);

		let listed = CatalogStore::list_windowed_view_groups_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(listed.len(), 1);
		assert_eq!(listed[0].members, members);
	}

	#[test]
	fn test_windowed_view_group_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = WindowedViewGroupToCreate {
			name: Fragment::internal("group_one"),
			namespace: test_namespace.id(),
			flow: FlowId(1),
			members: vec![ViewId(1)],
		};
		CatalogStore::create_windowed_view_group(&mut txn, to_create).unwrap();

		let to_create = WindowedViewGroupToCreate {
			name: Fragment::internal("group_two"),
			namespace: test_namespace.id(),
			flow: FlowId(2),
			members: vec![ViewId(2)],
		};
		CatalogStore::create_windowed_view_group(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceWindowedViewGroupKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let mut found_group_one = false;
		let mut found_group_two = false;

		for link in &links {
			let bytes = &link.bytes;
			let id = windowed_view_group_namespace::get_id(EncodedCatalogRow::view(bytes));
			let name = windowed_view_group_namespace::get_name(EncodedCatalogRow::view(bytes));

			match name {
				"group_one" => {
					assert_eq!(id, 1);
					found_group_one = true;
				}
				"group_two" => {
					assert_eq!(id, 2);
					found_group_two = true;
				}
				_ => panic!("Unexpected windowed view group name: {}", name),
			}
		}

		assert!(found_group_one, "group_one not found in namespace links");
		assert!(found_group_two, "group_two not found in namespace links");
	}

	#[test]
	fn test_create_windowed_view_group_multiple_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		let to_create = WindowedViewGroupToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_one.id(),
			flow: FlowId(1),
			members: vec![ViewId(1)],
		};
		CatalogStore::create_windowed_view_group(&mut txn, to_create).unwrap();

		let to_create = WindowedViewGroupToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_two.id(),
			flow: FlowId(2),
			members: vec![ViewId(2)],
		};
		let result = CatalogStore::create_windowed_view_group(&mut txn, to_create).unwrap();
		assert_eq!(result.name, "shared_name");
		assert_eq!(result.namespace, namespace_two.id());
	}
}
