// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{QueryTransaction, SchemaDef, SchemaId};

use crate::{CatalogStore, transaction::CatalogTransaction};

impl CatalogStore {
	pub fn list_schemas_all(
		rx: &mut (impl QueryTransaction + CatalogTransaction),
	) -> crate::Result<Vec<SchemaDef>> {
		let catalog = rx.catalog();
		let version = CatalogTransaction::version(&*rx);
		let mut result = Vec::new();

		result.push(SchemaDef {
			id: SchemaId(1),
			name: "system".to_string(),
		});

		// Iterate through all schemas in the materialized catalog
		for entry in catalog.schemas.iter() {
			let versioned_def = entry.value();
			if let Some(def) = versioned_def.get(version) {
				result.push(def.clone());
			}
		}

		Ok(result)
	}
}
