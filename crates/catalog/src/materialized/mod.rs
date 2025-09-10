// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod load;
mod primary_key;
mod resolver_helpers;
mod schema;
mod table;
mod view;

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use reifydb_core::{
	interface::{
		PrimaryKeyDef, PrimaryKeyId, SchemaDef, SchemaId, TableDef,
		TableId, ViewDef, ViewId,
	},
	util::VersionedContainer,
};

use crate::system::SystemCatalog;

pub type VersionedSchemaDef = VersionedContainer<SchemaDef>;
pub type VersionedTableDef = VersionedContainer<TableDef>;
pub type VersionedViewDef = VersionedContainer<ViewDef>;
pub type VersionedPrimaryKeyDef = VersionedContainer<PrimaryKeyDef>;

/// A materialized catalog that stores versioned schema, table, and view
/// definitions. This provides fast O(1) lookups for catalog metadata without
/// hitting storage.
#[derive(Debug, Clone)]
pub struct MaterializedCatalog(Arc<MaterializedCatalogInner>);

#[derive(Debug)]
pub struct MaterializedCatalogInner {
	/// Versioned schema definitions indexed by schema ID
	pub(crate) schemas: SkipMap<SchemaId, VersionedSchemaDef>,
	/// Index from schema name to schema ID for fast name lookups
	pub(crate) schemas_by_name: SkipMap<String, SchemaId>,

	/// Versioned table definitions indexed by table ID
	pub(crate) tables: SkipMap<TableId, VersionedTableDef>,
	/// Index from (schema_id, table_name) to table ID for fast name
	/// lookups
	pub(crate) tables_by_name: SkipMap<(SchemaId, String), TableId>,

	/// Versioned view definitions indexed by view ID
	pub(crate) views: SkipMap<ViewId, VersionedViewDef>,
	/// Index from (schema_id, view_name) to view ID for fast name lookups
	pub(crate) views_by_name: SkipMap<(SchemaId, String), ViewId>,

	/// Versioned primary key definitions indexed by primary key ID
	pub(crate) primary_keys: SkipMap<PrimaryKeyId, VersionedPrimaryKeyDef>,

	/// System catalog with version information (None until initialized)
	pub(crate) system_catalog: Option<SystemCatalog>,
}

impl std::ops::Deref for MaterializedCatalog {
	type Target = MaterializedCatalogInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Default for MaterializedCatalog {
	fn default() -> Self {
		Self::new()
	}
}

impl MaterializedCatalog {
	pub fn new() -> Self {
		Self(Arc::new(MaterializedCatalogInner {
			schemas: SkipMap::new(),
			schemas_by_name: SkipMap::new(),
			tables: SkipMap::new(),
			tables_by_name: SkipMap::new(),
			views: SkipMap::new(),
			views_by_name: SkipMap::new(),
			primary_keys: SkipMap::new(),
			system_catalog: None,
		}))
	}

	/// Set the system catalog (called once during database initialization)
	pub fn set_system_catalog(&self, catalog: SystemCatalog) {
		// Use unsafe to mutate through Arc (safe because only called
		// once during init)
		unsafe {
			let inner = Arc::as_ptr(&self.0)
				as *mut MaterializedCatalogInner;
			(*inner).system_catalog = Some(catalog);
		}
	}

	/// Get the system catalog
	pub fn system_catalog(&self) -> Option<&SystemCatalog> {
		self.0.system_catalog.as_ref()
	}
}
