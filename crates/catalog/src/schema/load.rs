// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema Registry loading from storage.

use reifydb_transaction::transaction::Transaction;
use tracing::{Span, field, instrument};

use super::SchemaRegistry;
use crate::{Result, store::schema::find::load_all_schemas};

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
		fields(schema_count = field::Empty)
	)]
	pub fn load_all(rx: &mut Transaction<'_>, registry: &SchemaRegistry) -> Result<()> {
		let schemas = load_all_schemas(rx)?;

		Span::current().record("schema_count", schemas.len());

		for schema in schemas {
			registry.cache_schema(schema);
		}

		Ok(())
	}
}
