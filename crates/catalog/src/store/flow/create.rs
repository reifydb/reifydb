// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeDomain,
	interface::catalog::{
		flow::{Flow, FlowId, FlowStatus},
		id::NamespaceId,
	},
	key::{flow::FlowKey, namespace_flow::NamespaceFlowKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		flow::shape::{flow, flow_namespace},
		sequence::flow::next_flow_id,
	},
};

#[derive(Debug, Clone)]
pub struct FlowToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub status: FlowStatus,
	pub time: Option<TimeDomain>,
}

impl CatalogStore {
	pub(crate) fn create_flow(txn: &mut AdminTransaction, to_create: FlowToCreate) -> Result<Flow> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_flow(txn, namespace_id, &to_create.name)?;

		let flow_id = next_flow_id(txn)?;
		Self::install_flow(txn, flow_id, namespace_id, &to_create)?;
		Self::get_flow(&mut Transaction::Admin(&mut *txn), flow_id)
	}

	pub(crate) fn create_flow_with_id(
		txn: &mut AdminTransaction,
		flow_id: FlowId,
		to_create: FlowToCreate,
	) -> Result<Flow> {
		let namespace_id = to_create.namespace;
		Self::install_flow(txn, flow_id, namespace_id, &to_create)?;
		Self::get_flow(&mut Transaction::Admin(&mut *txn), flow_id)
	}

	#[inline]
	fn reject_existing_flow(txn: &mut AdminTransaction, namespace_id: NamespaceId, name: &Fragment) -> Result<()> {
		if CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut *txn), namespace_id, name.text())?
			.is_none()
		{
			return Ok(());
		}
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::Flow,
			namespace: namespace.name().to_string(),
			name: name.text().to_string(),
			fragment: name.clone(),
		}
		.into())
	}

	#[inline]
	fn install_flow(
		txn: &mut AdminTransaction,
		flow_id: FlowId,
		namespace_id: NamespaceId,
		to_create: &FlowToCreate,
	) -> Result<()> {
		Self::store_flow(txn, flow_id, namespace_id, to_create)?;
		Self::link_flow_to_namespace(txn, namespace_id, flow_id, to_create.name.text())
	}

	fn store_flow(
		txn: &mut AdminTransaction,
		flow: FlowId,
		namespace: NamespaceId,
		to_create: &FlowToCreate,
	) -> Result<()> {
		let mut row = flow::SHAPE.allocate();
		flow::SHAPE.set::<u64>(&mut row, flow::ID, u64::from(flow));
		flow::SHAPE.set::<u64>(&mut row, flow::NAMESPACE, u64::from(namespace));
		flow::SHAPE.set_utf8(&mut row, flow::NAME, to_create.name.text());
		flow::SHAPE.set::<u8>(&mut row, flow::STATUS, to_create.status.to_u8());
		flow::SHAPE.set::<u8>(&mut row, flow::TIME, TimeDomain::declared_to_u8(to_create.time));

		let key = FlowKey::encoded(flow);
		txn.set(&key, row.freeze())?;

		Ok(())
	}

	fn link_flow_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		flow: FlowId,
		name: &str,
	) -> Result<()> {
		let mut row = flow_namespace::SHAPE.allocate();
		flow_namespace::SHAPE.set::<u64>(&mut row, flow_namespace::ID, u64::from(flow));
		flow_namespace::SHAPE.set_utf8(&mut row, flow_namespace::NAME, name);
		let key = NamespaceFlowKey::encoded(namespace, flow);
		txn.set(&key, row.freeze())?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::TimeDomain,
		interface::catalog::{
			flow::{FlowId, FlowStatus},
			id::NamespaceId,
		},
		key::namespace_flow::NamespaceFlowKey,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::flow::{create::FlowToCreate, shape::flow_namespace},
		test_utils::{create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_create_flow() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = FlowToCreate {
			name: Fragment::internal("test_flow"),
			namespace: test_namespace.id(),
			status: FlowStatus::Active,
			time: Some(TimeDomain::Processing),
		};

		let result = CatalogStore::create_flow(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, FlowId(1));
		assert_eq!(result.namespace, NamespaceId(16385));
		assert_eq!(result.name, "test_flow");
		assert_eq!(result.status, FlowStatus::Active);

		let err = CatalogStore::create_flow(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_030");
	}

	#[test]
	fn test_flow_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = FlowToCreate {
			name: Fragment::internal("flow_one"),
			namespace: test_namespace.id(),
			status: FlowStatus::Active,
			time: Some(TimeDomain::Processing),
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		let to_create = FlowToCreate {
			name: Fragment::internal("flow_two"),
			namespace: test_namespace.id(),
			status: FlowStatus::Paused,
			time: Some(TimeDomain::Processing),
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceFlowKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let mut found_flow_one = false;
		let mut found_flow_two = false;

		for link in &links {
			let row = &link.row;
			let id = flow_namespace::SHAPE.get::<u64>(row, flow_namespace::ID);
			let name = flow_namespace::SHAPE.get_utf8(row, flow_namespace::NAME);

			match name {
				"flow_one" => {
					assert_eq!(id, 1);
					found_flow_one = true;
				}
				"flow_two" => {
					assert_eq!(id, 2);
					found_flow_two = true;
				}
				_ => panic!("Unexpected flow name: {}", name),
			}
		}

		assert!(found_flow_one, "flow_one not found in namespace links");
		assert!(found_flow_two, "flow_two not found in namespace links");
	}

	#[test]
	fn test_create_flow_multiple_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		let to_create = FlowToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_one.id(),
			status: FlowStatus::Active,
			time: Some(TimeDomain::Processing),
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		let to_create = FlowToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_two.id(),
			status: FlowStatus::Active,
			time: Some(TimeDomain::Processing),
		};
		let result = CatalogStore::create_flow(&mut txn, to_create).unwrap();
		assert_eq!(result.name, "shared_name");
		assert_eq!(result.namespace, namespace_two.id());
	}
}
