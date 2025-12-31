// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, NamespaceDef, NamespaceKey};
use reifydb_transaction::IntoStandardTransaction;

use crate::{CatalogStore, store::namespace::layout::namespace};

impl CatalogStore {
	pub async fn list_namespaces_all(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<NamespaceDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let batch = txn.range_batch(namespace_range, 1024).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Namespace(namespace_key) = key {
					let namespace_id = namespace_key.namespace;

					let name =
						namespace::LAYOUT.get_utf8(&entry.values, namespace::NAME).to_string();
					let namespace_def = NamespaceDef {
						id: namespace_id,
						name,
					};

					result.push(namespace_def);
				}
			}
		}

		result.push(NamespaceDef::system());

		Ok(result)
	}
}
