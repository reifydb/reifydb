// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{PrimaryKeyDef, QueryTransaction};

use crate::{CatalogStore, transaction::CatalogTransaction};

pub struct PrimaryKeyInfo {
	pub def: PrimaryKeyDef,
	pub source_id: u64,
}

impl CatalogStore {
	pub fn list_primary_keys(
		rx: &mut (impl QueryTransaction + CatalogTransaction),
	) -> crate::Result<Vec<PrimaryKeyInfo>> {
		let catalog = rx.catalog();
		let version = CatalogTransaction::version(&*rx);
		let mut result = Vec::new();

		// Get all tables and their primary keys
		for entry in catalog.tables.iter() {
			let table_def = entry.value();
			if let Some(def) = table_def.get(version) {
				if let Some(pk_def) = def.primary_key {
					result.push(PrimaryKeyInfo {
						def: pk_def,
						source_id: def.id.0,
					});
				}
			}
		}

		// Get all views and their primary keys
		for entry in catalog.views.iter() {
			let view_def = entry.value();
			if let Some(def) = view_def.get(version) {
				if let Some(pk_def) = def.primary_key {
					result.push(PrimaryKeyInfo {
						def: pk_def,
						source_id: def.id.0,
					});
				}
			}
		}

		Ok(result)
	}

	pub fn list_primary_key_columns(
		rx: &mut (impl QueryTransaction + CatalogTransaction),
	) -> crate::Result<Vec<(u64, u64, usize)>> {
		let catalog = rx.catalog();
		let version = CatalogTransaction::version(&*rx);
		let mut result = Vec::new();

		// Iterate through all primary keys and extract column
		// relationships
		for entry in catalog.primary_keys.iter() {
			let pk_id = entry.key();
			let versioned_def = entry.value();
			if let Some(def) = versioned_def.get(version) {
				for (position, column_def) in
					def.columns.iter().enumerate()
				{
					result.push((
						pk_id.0,
						column_def.id.0,
						position,
					));
				}
			}
		}

		Ok(result)
	}
}
