// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{NamespaceDef, QueryTransaction};

use crate::{CatalogStore, transaction::CatalogTransaction};

impl CatalogStore {
	pub fn list_namespaces_all(
		rx: &mut (impl QueryTransaction + CatalogTransaction),
	) -> crate::Result<Vec<NamespaceDef>> {
		let catalog = rx.catalog();
		let version = CatalogTransaction::version(&*rx);
		let mut result = Vec::new();

		for entry in catalog.namespaces.iter() {
			let versioned_def = entry.value();
			if let Some(def) = versioned_def.get(version) {
				result.push(def.clone());
			}
		}

		Ok(result)
	}
}
