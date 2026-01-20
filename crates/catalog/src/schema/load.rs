// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema Registry loading from storage.

use reifydb_transaction::standard::IntoStandardTransaction;
use tracing::instrument;

use super::SchemaRegistry;
use crate::store::schema::find::load_all_schemas;

/// Loads schemas from storage into the SchemaRegistry cache.
pub struct SchemaRegistryLoader;

impl SchemaRegistryLoader {
	/// Load all schemas from storage into the registry cache.
	///
	/// This is called during database startup to populate the cache
	/// with persisted schemas.
	#[instrument(
		name = "schema_registry::load_all",
		level = "debug",
		skip(rx, registry),
		fields(schema_count = tracing::field::Empty)
	)]
	pub fn load_all(rx: &mut impl IntoStandardTransaction, registry: &SchemaRegistry) -> crate::Result<()> {
		let mut txn = rx.into_standard_transaction();
		let schemas = load_all_schemas(&mut txn)?;

		tracing::Span::current().record("schema_count", schemas.len());

		for schema in schemas {
			registry.cache_schema(schema);
		}

		Ok(())
	}
}
