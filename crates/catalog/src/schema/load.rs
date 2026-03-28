// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowSchema Registry loading from storage.

use reifydb_transaction::transaction::Transaction;
use tracing::{Span, field, instrument};

use super::RowSchemaRegistry;
use crate::{Result, store::row_schema::find::load_all_row_schemas};

/// Loads schemas from storage into the RowSchemaRegistry cache.
pub struct RowSchemaRegistryLoader;

impl RowSchemaRegistryLoader {
	/// Load all schemas from storage into the registry cache.
	///
	/// This is called during database startup to populate the cache
	/// with persisted schemas.
	#[instrument(
		name = "row_schema_registry::load_all",
		level = "debug",
		skip(rx, registry),
		fields(schema_count = field::Empty)
	)]
	pub fn load_all(rx: &mut Transaction<'_>, registry: &RowSchemaRegistry) -> Result<()> {
		let schemas = load_all_row_schemas(rx)?;

		Span::current().record("schema_count", schemas.len());

		for schema in schemas {
			registry.cache_row_schema(schema);
		}

		Ok(())
	}
}
