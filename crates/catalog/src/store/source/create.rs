// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SourceId},
		source::SourceDef,
	},
	key::{namespace_source::NamespaceSourceKey, source::SourceKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		sequence::source::next_source_id,
		source::schema::{source, source_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct SourceToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub connector: String,
	pub config: Vec<(String, String)>,
	pub target_namespace: NamespaceId,
	pub target_name: String,
}

impl CatalogStore {
	pub(crate) fn create_source(txn: &mut AdminTransaction, to_create: SourceToCreate) -> Result<SourceDef> {
		let namespace_id = to_create.namespace;

		// Check if source already exists
		if let Some(_source) = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Source,
				namespace: namespace.name().to_string(),
				name: to_create.name.text().to_string(),
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let source_id = next_source_id(txn)?;
		Self::store_source(txn, source_id, namespace_id, &to_create)?;
		Self::link_source_to_namespace(txn, namespace_id, source_id, to_create.name.text())?;

		Ok(Self::get_source(&mut Transaction::Admin(&mut *txn), source_id)?)
	}

	fn store_source(
		txn: &mut AdminTransaction,
		source: SourceId,
		namespace: NamespaceId,
		to_create: &SourceToCreate,
	) -> Result<()> {
		let config_json = to_string(&to_create.config).unwrap_or_default();

		let mut row = source::SCHEMA.allocate();
		source::SCHEMA.set_u64(&mut row, source::ID, source);
		source::SCHEMA.set_u64(&mut row, source::NAMESPACE, namespace);
		source::SCHEMA.set_utf8(&mut row, source::NAME, to_create.name.text());
		source::SCHEMA.set_utf8(&mut row, source::CONNECTOR, &to_create.connector);
		source::SCHEMA.set_utf8(&mut row, source::CONFIG, &config_json);
		source::SCHEMA.set_u64(&mut row, source::TARGET_NAMESPACE, to_create.target_namespace);
		source::SCHEMA.set_utf8(&mut row, source::TARGET_NAME, &to_create.target_name);
		source::SCHEMA.set_u8(&mut row, source::STATUS, FlowStatus::Active.to_u8());

		let key = SourceKey::encoded(source);
		txn.set(&key, row)?;

		Ok(())
	}

	fn link_source_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		source: SourceId,
		name: &str,
	) -> Result<()> {
		let mut row = source_namespace::SCHEMA.allocate();
		source_namespace::SCHEMA.set_u64(&mut row, source_namespace::ID, source);
		source_namespace::SCHEMA.set_utf8(&mut row, source_namespace::NAME, name);
		let key = NamespaceSourceKey::encoded(namespace, source);
		txn.set(&key, row)?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{NamespaceId, SourceId},
		key::namespace_source::NamespaceSourceKey,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_type::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::source::{create::SourceToCreate, schema::source_namespace},
		test_utils::{create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_create_source() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SourceToCreate {
			name: Fragment::internal("test_source"),
			namespace: test_namespace.id(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: test_namespace.id(),
			target_name: "target_table".to_string(),
		};

		let result = CatalogStore::create_source(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, SourceId(1));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_source");
		assert_eq!(result.connector, "kafka");
		assert_eq!(result.config, vec![("key".to_string(), "value".to_string())]);
	}

	#[test]
	fn test_create_source_duplicate() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SourceToCreate {
			name: Fragment::internal("test_source"),
			namespace: test_namespace.id(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: test_namespace.id(),
			target_name: "target_table".to_string(),
		};

		// First creation should succeed
		CatalogStore::create_source(&mut txn, to_create.clone()).unwrap();

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_source(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_060");
	}

	#[test]
	fn test_source_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = SourceToCreate {
			name: Fragment::internal("source_one"),
			namespace: test_namespace.id(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: test_namespace.id(),
			target_name: "target_table".to_string(),
		};
		CatalogStore::create_source(&mut txn, to_create).unwrap();

		let to_create = SourceToCreate {
			name: Fragment::internal("source_two"),
			namespace: test_namespace.id(),
			connector: "postgres".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: test_namespace.id(),
			target_name: "target_table".to_string(),
		};
		CatalogStore::create_source(&mut txn, to_create).unwrap();

		// Verify both are linked to namespace
		let links: Vec<_> = txn
			.range(NamespaceSourceKey::full_scan(test_namespace.id()), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Verify link metadata
		let mut found_source_one = false;
		let mut found_source_two = false;

		for link in &links {
			let row = &link.row;
			let id = source_namespace::SCHEMA.get_u64(row, source_namespace::ID);
			let name = source_namespace::SCHEMA.get_utf8(row, source_namespace::NAME);

			match name {
				"source_one" => {
					assert_eq!(id, 1);
					found_source_one = true;
				}
				"source_two" => {
					assert_eq!(id, 2);
					found_source_two = true;
				}
				_ => panic!("Unexpected source name: {}", name),
			}
		}

		assert!(found_source_one, "source_one not found in namespace links");
		assert!(found_source_two, "source_two not found in namespace links");
	}

	#[test]
	fn test_create_source_multiple_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		// Create source in first namespace
		let to_create = SourceToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_one.id(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: namespace_one.id(),
			target_name: "target_table".to_string(),
		};
		CatalogStore::create_source(&mut txn, to_create).unwrap();

		// Should be able to create source with same name in different namespace
		let to_create = SourceToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_two.id(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: namespace_two.id(),
			target_name: "target_table".to_string(),
		};
		let result = CatalogStore::create_source(&mut txn, to_create).unwrap();
		assert_eq!(result.name, "shared_name");
		assert_eq!(result.namespace, namespace_two.id());
	}
}
