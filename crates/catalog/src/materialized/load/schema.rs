// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{SchemaKey, VersionedQueryTransaction};

use crate::{MaterializedCatalog, schema};

/// Load all schemas from storage
pub(crate) fn load_schemas(
	tx: &mut impl VersionedQueryTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let range = SchemaKey::full_scan();
	let schemas = tx.range(range)?;

	for versioned in schemas {
		let version = versioned.version;
		let schema_def = schema::convert_schema(versioned);
		catalog.set_schema(schema_def.id, version, Some(schema_def));
	}

	Ok(())
}
