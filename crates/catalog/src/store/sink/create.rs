// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SinkId},
		sink::Sink,
	},
	key::{namespace_sink::NamespaceSinkKey, sink::SinkKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		sequence::sink::next_sink_id,
		sink::shape::{sink, sink_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct SinkToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub source_namespace: NamespaceId,
	pub source_name: String,
	pub connector: String,
	pub config: Vec<(String, String)>,
}

impl CatalogStore {
	pub(crate) fn create_sink(txn: &mut AdminTransaction, to_create: SinkToCreate) -> Result<Sink> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_sink(txn, namespace_id, &to_create.name)?;

		let sink_id = next_sink_id(txn)?;
		Self::store_sink(txn, sink_id, namespace_id, &to_create)?;
		Self::link_sink_to_namespace(txn, namespace_id, sink_id, to_create.name.text())?;
		Self::get_sink(&mut Transaction::Admin(&mut *txn), sink_id)
	}

	#[inline]
	fn reject_existing_sink(txn: &mut AdminTransaction, namespace_id: NamespaceId, name: &Fragment) -> Result<()> {
		if CatalogStore::find_sink_by_name(&mut Transaction::Admin(&mut *txn), namespace_id, name.text())?
			.is_none()
		{
			return Ok(());
		}
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::Sink,
			namespace: namespace.name().to_string(),
			name: name.text().to_string(),
			fragment: name.clone(),
		}
		.into())
	}

	fn store_sink(
		txn: &mut AdminTransaction,
		sink: SinkId,
		namespace: NamespaceId,
		to_create: &SinkToCreate,
	) -> Result<()> {
		let config_json = to_string(&to_create.config).unwrap_or_default();

		let mut row = sink::SHAPE.allocate();
		sink::SHAPE.set_u64(&mut row, sink::ID, sink);
		sink::SHAPE.set_u64(&mut row, sink::NAMESPACE, namespace);
		sink::SHAPE.set_utf8(&mut row, sink::NAME, to_create.name.text());
		sink::SHAPE.set_u64(&mut row, sink::SOURCE_NAMESPACE, to_create.source_namespace);
		sink::SHAPE.set_utf8(&mut row, sink::SOURCE_NAME, &to_create.source_name);
		sink::SHAPE.set_utf8(&mut row, sink::CONNECTOR, &to_create.connector);
		sink::SHAPE.set_utf8(&mut row, sink::CONFIG, &config_json);
		sink::SHAPE.set_u8(&mut row, sink::STATUS, FlowStatus::Active.to_u8());

		let key = SinkKey::encoded(sink);
		txn.set(&key, row)?;

		Ok(())
	}

	fn link_sink_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		sink: SinkId,
		name: &str,
	) -> Result<()> {
		let mut row = sink_namespace::SHAPE.allocate();
		sink_namespace::SHAPE.set_u64(&mut row, sink_namespace::ID, sink);
		sink_namespace::SHAPE.set_utf8(&mut row, sink_namespace::NAME, name);
		let key = NamespaceSinkKey::encoded(namespace, sink);
		txn.set(&key, row)?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{NamespaceId, SinkId},
		key::namespace_sink::NamespaceSinkKey,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_type::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::sink::{create::SinkToCreate, shape::sink_namespace},
		test_utils::{create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_create_sink() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SinkToCreate {
			name: Fragment::internal("test_sink"),
			namespace: test_namespace.id(),
			source_namespace: test_namespace.id(),
			source_name: "source_table".to_string(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		};

		let result = CatalogStore::create_sink(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, SinkId(1));
		assert_eq!(result.namespace, NamespaceId(16385));
		assert_eq!(result.name, "test_sink");
		assert_eq!(result.connector, "kafka");
		assert_eq!(result.config, vec![("key".to_string(), "value".to_string())]);
	}

	#[test]
	fn test_create_sink_duplicate() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SinkToCreate {
			name: Fragment::internal("test_sink"),
			namespace: test_namespace.id(),
			source_namespace: test_namespace.id(),
			source_name: "source_table".to_string(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		};

		// First creation should succeed
		CatalogStore::create_sink(&mut txn, to_create.clone()).unwrap();

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_sink(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_061");
	}

	#[test]
	fn test_sink_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SinkToCreate {
			name: Fragment::internal("sink_one"),
			namespace: test_namespace.id(),
			source_namespace: test_namespace.id(),
			source_name: "source_table".to_string(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		};
		CatalogStore::create_sink(&mut txn, to_create).unwrap();

		let to_create = SinkToCreate {
			name: Fragment::internal("sink_two"),
			namespace: test_namespace.id(),
			source_namespace: test_namespace.id(),
			source_name: "source_table".to_string(),
			connector: "postgres".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		};
		CatalogStore::create_sink(&mut txn, to_create).unwrap();

		// Verify both are linked to namespace
		let links: Vec<_> = txn
			.range(NamespaceSinkKey::full_scan(test_namespace.id()), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Verify link metadata
		let mut found_sink_one = false;
		let mut found_sink_two = false;

		for link in &links {
			let row = &link.row;
			let id = sink_namespace::SHAPE.get_u64(row, sink_namespace::ID);
			let name = sink_namespace::SHAPE.get_utf8(row, sink_namespace::NAME);

			match name {
				"sink_one" => {
					assert_eq!(id, 1);
					found_sink_one = true;
				}
				"sink_two" => {
					assert_eq!(id, 2);
					found_sink_two = true;
				}
				_ => panic!("Unexpected sink name: {}", name),
			}
		}

		assert!(found_sink_one, "sink_one not found in namespace links");
		assert!(found_sink_two, "sink_two not found in namespace links");
	}

	#[test]
	fn test_create_sink_multiple_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		// Create sink in first namespace
		let to_create = SinkToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_one.id(),
			source_namespace: namespace_one.id(),
			source_name: "source_table".to_string(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		};
		CatalogStore::create_sink(&mut txn, to_create).unwrap();

		// Should be able to create sink with same name in different namespace
		let to_create = SinkToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_two.id(),
			source_namespace: namespace_two.id(),
			source_name: "source_table".to_string(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
		};
		let result = CatalogStore::create_sink(&mut txn, to_create).unwrap();
		assert_eq!(result.name, "shared_name");
		assert_eq!(result.namespace, namespace_two.id());
	}
}
