// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{schema, table, view, MaterializedCatalog};
use reifydb_core::interface::{
	SchemaKey, TableKey, VersionedQueryTransaction, ViewKey,
};

/// Loads catalog data from storage and populates a MaterializedCatalog
pub struct MaterializedCatalogLoader;

impl MaterializedCatalogLoader {
	/// Load all catalog data from storage into the MaterializedCatalog
	pub fn load_all(
		tx: &mut impl VersionedQueryTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		Self::load_schemas(tx, catalog)?;
		Self::load_tables(tx, catalog)?;
		Self::load_views(tx, catalog)?;

		Ok(())
	}

	/// Load all schemas from storage
	pub fn load_schemas(
		tx: &mut impl VersionedQueryTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let range = SchemaKey::full_scan();
		let schemas = tx.range(range)?;

		for versioned in schemas {
			let version = versioned.version;
			let schema_def = schema::convert_schema(versioned);
			catalog.set_schema(
				schema_def.id,
				version,
				Some(schema_def),
			);
		}

		Ok(())
	}

	/// Load all tables from storage
	pub fn load_tables(
		tx: &mut impl VersionedQueryTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let range = TableKey::full_scan();
		let tables = tx.range(range)?;

		for versioned in tables {
			let version = versioned.version;
			let table_def = table::convert_table(versioned);
			catalog.set_table(
				table_def.id,
				version,
				Some(table_def),
			);
		}

		Ok(())
	}

	/// Load all views from storage
	pub fn load_views(
		tx: &mut impl VersionedQueryTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		let range = ViewKey::full_scan();
		let views = tx.range(range)?;

		for versioned in views {
			let version = versioned.version;
			let view_def = view::convert_view(versioned);
			catalog.set_view(view_def.id, version, Some(view_def));
		}

		Ok(())
	}
}
