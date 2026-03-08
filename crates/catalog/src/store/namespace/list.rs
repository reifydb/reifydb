// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, namespace::Namespace},
	key::{Key, namespace::NamespaceKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::namespace::schema::namespace};

impl CatalogStore {
	pub(crate) fn list_namespaces_all(rx: &mut Transaction<'_>) -> Result<Vec<Namespace>> {
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let mut stream = rx.range(namespace_range, 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Namespace(namespace_key) = key {
					let namespace_id = namespace_key.namespace;

					let name =
						namespace::SCHEMA.get_utf8(&entry.values, namespace::NAME).to_string();
					let parent_id = NamespaceId(
						namespace::SCHEMA.get_u64(&entry.values, namespace::PARENT_ID),
					);
					let grpc = namespace::SCHEMA
						.try_get_utf8(&entry.values, namespace::GRPC)
						.map(|s| s.to_string())
						.filter(|s| !s.is_empty());
					let namespace = if let Some(address) = grpc {
						Namespace::Remote {
							id: namespace_id,
							name,
							parent_id,
							address,
						}
					} else {
						Namespace::Local {
							id: namespace_id,
							name,
							parent_id,
						}
					};

					result.push(namespace);
				}
			}
		}

		result.push(Namespace::system());
		result.push(Namespace::default_namespace());

		Ok(result)
	}
}
