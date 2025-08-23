// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::{
    interface::{SchemaId, TableId, ViewId},
};

use super::versioned::{VersionedSchemaDef, VersionedTableDef, VersionedViewDef};

/// A materialized catalog that stores versioned schema, table, and view definitions.
/// This provides fast O(1) lookups for catalog metadata without hitting storage.
#[derive(Clone)]
pub struct MaterializedCatalog(Arc<MaterializedCatalogInner>);

pub struct MaterializedCatalogInner {
    /// Versioned schema definitions indexed by schema ID
    pub(crate) schemas: SkipMap<SchemaId, VersionedSchemaDef>,
    /// Index from schema name to schema ID for fast name lookups
    pub(crate) schemas_by_name: SkipMap<String, SchemaId>,
    
    /// Versioned table definitions indexed by table ID
    pub(crate) tables: SkipMap<TableId, VersionedTableDef>,
    /// Index from (schema_id, table_name) to table ID for fast name lookups
    pub(crate) tables_by_name: SkipMap<(SchemaId, String), TableId>,
    
    /// Versioned view definitions indexed by view ID  
    pub(crate) views: SkipMap<ViewId, VersionedViewDef>,
    /// Index from (schema_id, view_name) to view ID for fast name lookups
    pub(crate) views_by_name: SkipMap<(SchemaId, String), ViewId>,
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
        }))
    }
}