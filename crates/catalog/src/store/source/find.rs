// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SourceId},
		source::Source,
	},
	key::{namespace_source::NamespaceSourceKey, source::SourceKey},
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use crate::{
	CatalogStore, Result,
	store::source::schema::{source, source_namespace},
};

impl CatalogStore {
	pub(crate) fn find_source(rx: &mut Transaction<'_>, id: SourceId) -> Result<Option<Source>> {
		let Some(multi) = rx.get(&SourceKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = SourceId(source::SCHEMA.get_u64(&row, source::ID));
		let namespace = NamespaceId(source::SCHEMA.get_u64(&row, source::NAMESPACE));
		let name = source::SCHEMA.get_utf8(&row, source::NAME).to_string();
		let connector = source::SCHEMA.get_utf8(&row, source::CONNECTOR).to_string();
		let config_json = source::SCHEMA.get_utf8(&row, source::CONFIG);
		let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
		let target_namespace = NamespaceId(source::SCHEMA.get_u64(&row, source::TARGET_NAMESPACE));
		let target_name = source::SCHEMA.get_utf8(&row, source::TARGET_NAME).to_string();
		let status_u8 = source::SCHEMA.get_u8(&row, source::STATUS);
		let status = FlowStatus::from_u8(status_u8);

		Ok(Some(Source {
			id,
			name,
			namespace,
			connector,
			config,
			target_namespace,
			target_name,
			status,
		}))
	}

	pub(crate) fn find_source_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<Source>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSourceKey::full_scan(namespace), 1024)?;

		let mut found_source = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.row;
			let source_name = source_namespace::SCHEMA.get_utf8(row, source_namespace::NAME);
			if name == source_name {
				found_source =
					Some(SourceId(source_namespace::SCHEMA.get_u64(row, source_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(source) = found_source else {
			return Ok(None);
		};

		Ok(Some(Self::get_source(rx, source)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_source, ensure_test_namespace},
	};

	#[test]
	fn test_find_source_by_name_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_source(&mut txn, "namespace_one", "source_one", "kafka");
		create_source(&mut txn, "namespace_two", "source_two", "postgres");

		let result = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace_two.id(),
			"source_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.name, "source_two");
		assert_eq!(result.namespace, namespace_two.id());
		assert_eq!(result.connector, "postgres");
	}

	#[test]
	fn test_find_source_by_name_empty() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"some_source",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_source_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_source(&mut txn, "test_namespace", "source_one", "kafka");
		create_source(&mut txn, "test_namespace", "source_two", "postgres");

		let result = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"source_three",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_source_by_name_different_namespace() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_source(&mut txn, "namespace_one", "my_source", "kafka");

		// Source exists in namespace_one but not in namespace_two
		let result = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace_two.id(),
			"my_source",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_source_by_name_case_sensitive() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_source(&mut txn, "test_namespace", "MySource", "kafka");

		// Source names are case-sensitive
		let result = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"mysource",
		)
		.unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"MySource",
		)
		.unwrap();
		assert!(result.is_some());
	}

	#[test]
	fn test_find_source_by_id() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let source = create_source(&mut txn, "test_namespace", "test_source", "kafka");

		let result = CatalogStore::find_source(&mut Transaction::Admin(&mut txn), source.id).unwrap().unwrap();
		assert_eq!(result.id, source.id);
		assert_eq!(result.name, "test_source");
		assert_eq!(result.connector, "kafka");
	}

	#[test]
	fn test_find_source_by_id_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_source(&mut Transaction::Admin(&mut txn), 999.into()).unwrap();
		assert!(result.is_none());
	}
}
