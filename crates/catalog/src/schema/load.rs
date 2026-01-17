// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema Registry loading from storage.

use reifydb_transaction::standard::IntoStandardTransaction;

use super::SchemaRegistry;
use crate::store::schema::get::load_all_schemas;

/// Loads schemas from storage into the SchemaRegistry cache.
pub struct SchemaRegistryLoader;

impl SchemaRegistryLoader {
	/// Load all schemas from storage into the registry cache.
	///
	/// This is called during database startup to populate the cache
	/// with persisted schemas.
	pub fn load_all(rx: &mut impl IntoStandardTransaction, registry: &SchemaRegistry) -> crate::Result<()> {
		let mut txn = rx.into_standard_transaction();
		let schemas = load_all_schemas(&mut txn)?;

		for schema in schemas {
			registry.cache_schema(schema);
		}

		Ok(())
	}
}
