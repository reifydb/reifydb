// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{Key, NamespaceDef, NamespaceKey};
use reifydb_transaction::IntoStandardTransaction;

use crate::{CatalogStore, store::namespace::layout::namespace};

impl CatalogStore {
	pub fn list_namespaces_all(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<NamespaceDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let mut stream = txn.range(namespace_range, 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
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
