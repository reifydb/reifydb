// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod primary_key;
pub mod schema;
pub mod table;
pub mod view;

pub(crate) use primary_key::load_primary_keys;
use reifydb_core::interface::QueryTransaction;
pub(crate) use schema::load_schemas;
pub(crate) use table::load_tables;
pub(crate) use view::load_views;

use crate::MaterializedCatalog;

/// Loads catalog data from storage and populates a MaterializedCatalog
pub struct MaterializedCatalogLoader;

impl MaterializedCatalogLoader {
	/// Load all catalog data from storage into the MaterializedCatalog
	pub fn load_all(
		qt: &mut impl QueryTransaction,
		catalog: &MaterializedCatalog,
	) -> crate::Result<()> {
		load_schemas(qt, catalog)?;
		// Load primary keys first so they're available when loading
		// tables/views
		load_primary_keys(qt, catalog)?;

		load_tables(qt, catalog)?;
		load_views(qt, catalog)?;
		Ok(())
	}
}
