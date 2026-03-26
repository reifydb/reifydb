// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::FlowStatus,
		id::{NamespaceId, SinkId},
		sink::SinkDef,
	},
	key::{namespace_sink::NamespaceSinkKey, sink::SinkKey},
};
use reifydb_transaction::transaction::Transaction;
use serde_json::from_str;

use crate::{
	CatalogStore, Result,
	store::sink::schema::{sink, sink_namespace},
};

impl CatalogStore {
	pub(crate) fn find_sink(rx: &mut Transaction<'_>, id: SinkId) -> Result<Option<SinkDef>> {
		let Some(multi) = rx.get(&SinkKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = SinkId(sink::SCHEMA.get_u64(&row, sink::ID));
		let namespace = NamespaceId(sink::SCHEMA.get_u64(&row, sink::NAMESPACE));
		let name = sink::SCHEMA.get_utf8(&row, sink::NAME).to_string();
		let source_namespace = NamespaceId(sink::SCHEMA.get_u64(&row, sink::SOURCE_NAMESPACE));
		let source_name = sink::SCHEMA.get_utf8(&row, sink::SOURCE_NAME).to_string();
		let connector = sink::SCHEMA.get_utf8(&row, sink::CONNECTOR).to_string();
		let config_json = sink::SCHEMA.get_utf8(&row, sink::CONFIG);
		let config: Vec<(String, String)> = from_str(config_json).unwrap_or_default();
		let status_u8 = sink::SCHEMA.get_u8(&row, sink::STATUS);
		let status = FlowStatus::from_u8(status_u8);

		Ok(Some(SinkDef {
			id,
			name,
			namespace,
			source_namespace,
			source_name,
			connector,
			config,
			status,
		}))
	}

	pub(crate) fn find_sink_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<SinkDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSinkKey::full_scan(namespace), 1024)?;

		let mut found_sink = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.row;
			let sink_name = sink_namespace::SCHEMA.get_utf8(row, sink_namespace::NAME);
			if name == sink_name {
				found_sink = Some(SinkId(sink_namespace::SCHEMA.get_u64(row, sink_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(sink) = found_sink else {
			return Ok(None);
		};

		Ok(Some(Self::get_sink(rx, sink)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_sink, ensure_test_namespace},
	};

	#[test]
	fn test_find_sink_by_name_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_sink(&mut txn, "namespace_one", "sink_one", "kafka");
		create_sink(&mut txn, "namespace_two", "sink_two", "postgres");

		let result = CatalogStore::find_sink_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace_two.id(),
			"sink_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.name, "sink_two");
		assert_eq!(result.namespace, namespace_two.id());
		assert_eq!(result.connector, "postgres");
	}

	#[test]
	fn test_find_sink_by_name_empty() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_sink_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"some_sink",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_sink_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_sink(&mut txn, "test_namespace", "sink_one", "kafka");
		create_sink(&mut txn, "test_namespace", "sink_two", "postgres");

		let result = CatalogStore::find_sink_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"sink_three",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_sink_by_name_different_namespace() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_sink(&mut txn, "namespace_one", "my_sink", "kafka");

		// Sink exists in namespace_one but not in namespace_two
		let result = CatalogStore::find_sink_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace_two.id(),
			"my_sink",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_sink_by_name_case_sensitive() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_sink(&mut txn, "test_namespace", "MySink", "kafka");

		// Sink names are case-sensitive
		let result = CatalogStore::find_sink_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"mysink",
		)
		.unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_sink_by_name(
			&mut Transaction::Admin(&mut txn),
			test_namespace.id(),
			"MySink",
		)
		.unwrap();
		assert!(result.is_some());
	}

	#[test]
	fn test_find_sink_by_id() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let sink = create_sink(&mut txn, "test_namespace", "test_sink", "kafka");

		let result = CatalogStore::find_sink(&mut Transaction::Admin(&mut txn), sink.id).unwrap().unwrap();
		assert_eq!(result.id, sink.id);
		assert_eq!(result.name, "test_sink");
		assert_eq!(result.connector, "kafka");
	}

	#[test]
	fn test_find_sink_by_id_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_sink(&mut Transaction::Admin(&mut txn), 999.into()).unwrap();
		assert!(result.is_none());
	}
}
