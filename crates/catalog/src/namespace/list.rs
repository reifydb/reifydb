// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, NamespaceDef, NamespaceKey, QueryTransaction};

use crate::{CatalogStore, namespace::layout::namespace};

impl CatalogStore {
	pub fn list_namespaces_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<NamespaceDef>> {
		let mut result = Vec::new();

		let namespace_range = NamespaceKey::full_scan();

		let entries: Vec<_> = rx.range(namespace_range)?.into_iter().collect();

		for entry in entries {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Namespace(namespace_key) = key {
					let namespace_id = namespace_key.namespace;

					let name = namespace::LAYOSVT.get_utf8(&entry.row, namespace::NAME).to_string();
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
