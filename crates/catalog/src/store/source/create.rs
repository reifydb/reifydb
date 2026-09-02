// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SourceId},
		source::Source,
	},
	key::{catalog::SourceKey, namespace::NamespaceSourceKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		sequence::source::next_object_id,
		source::shape::{source, source_namespace},
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
	pub(crate) fn create_source(txn: &mut AdminTransaction, to_create: SourceToCreate) -> Result<Source> {
		let namespace_id = to_create.namespace;

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

		let object_id = next_object_id(txn)?;
		Self::store_source(txn, object_id, namespace_id, &to_create)?;
		Self::link_source_to_namespace(txn, namespace_id, object_id, to_create.name.text())?;

		Self::get_source(&mut Transaction::Admin(&mut *txn), object_id)
	}

	fn store_source(
		txn: &mut AdminTransaction,
		source: SourceId,
		namespace: NamespaceId,
		to_create: &SourceToCreate,
	) -> Result<()> {
		let config_json = to_string(&to_create.config).unwrap_or_default();

		let mut row = source::allocate();
		source::set_id(&mut row, u64::from(source));
		source::set_namespace(&mut row, u64::from(namespace));
		source::set_name(&mut row, to_create.name.text());
		source::set_connector(&mut row, &to_create.connector);
		source::set_config(&mut row, &config_json);
		source::set_target_namespace(&mut row, u64::from(to_create.target_namespace));
		source::set_target_name(&mut row, &to_create.target_name);
		source::set_status(&mut row, FlowStatus::Active.to_u8());

		let key = SourceKey::encoded(source);
		txn.set(&key, row.freeze())?;

		Ok(())
	}

	fn link_source_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		source: SourceId,
		name: &str,
	) -> Result<()> {
		let mut row = source_namespace::allocate();
		source_namespace::set_id(&mut row, u64::from(source));
		source_namespace::set_name(&mut row, name);
		let key = NamespaceSourceKey::encoded(namespace, source);
		txn.set(&key, row.freeze())?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_core::{
		interface::catalog::id::{NamespaceId, SourceId},
		key::namespace::NamespaceSourceKey,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::source::{create::SourceToCreate, shape::source_namespace},
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
		assert_eq!(result.namespace, NamespaceId(16385));
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

		CatalogStore::create_source(&mut txn, to_create.clone()).unwrap();

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

		let links: Vec<_> = txn
			.range(NamespaceSourceKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let mut found_source_one = false;
		let mut found_source_two = false;

		for link in &links {
			let bytes = &link.bytes;
			let id = source_namespace::get_id(EncodedCatalogRow::view(bytes));
			let name = source_namespace::get_name(EncodedCatalogRow::view(bytes));

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

		let to_create = SourceToCreate {
			name: Fragment::internal("shared_name"),
			namespace: namespace_one.id(),
			connector: "kafka".to_string(),
			config: vec![("key".to_string(), "value".to_string())],
			target_namespace: namespace_one.id(),
			target_name: "target_table".to_string(),
		};
		CatalogStore::create_source(&mut txn, to_create).unwrap();

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
