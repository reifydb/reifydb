// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{QueryTransaction, ViewDef};

use crate::{CatalogStore, transaction::CatalogTransaction};

impl CatalogStore {
	pub fn list_views_all(
		rx: &mut (impl QueryTransaction + CatalogTransaction),
	) -> crate::Result<Vec<ViewDef>> {
		let catalog = rx.catalog();
		let version = CatalogTransaction::version(&*rx);
		let mut result = Vec::new();

		// Iterate through all views in the materialized catalog
		for entry in catalog.views.iter() {
			let versioned_def = entry.value();
			if let Some(def) = versioned_def.get(version) {
				result.push(def.clone());
			}
		}

		Ok(result)
	}
}
