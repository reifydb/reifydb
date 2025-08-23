// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
    Version,
    interface::{PendingWrite},
};

use super::{
    materialized::MaterializedCatalog,
    versioned::{VersionedSchemaDef, VersionedTableDef, VersionedViewDef},
};

impl MaterializedCatalog {
    /// Apply catalog changes from pending writes at the given version
    pub fn commit(&self, pending_writes: &[PendingWrite], version: Version) {
        for write in pending_writes {
            match write {
                PendingWrite::SchemaCreate { def } | PendingWrite::SchemaUpdate { def } => {
                    let versioned = self.schemas
                        .get_or_insert_with(def.id, VersionedSchemaDef::new);
                    let schemas = versioned.value();
                    schemas.lock();
                    schemas.insert(version, Some(def.clone()));
                    schemas.unlock();
                    
                    // Update name index
                    self.schemas_by_name.insert(def.name.clone(), def.id);
                }
                PendingWrite::SchemaDelete { id } => {
                    // Mark as deleted in versioned structure
                    if let Some(versioned) = self.schemas.get(id) {
                        let schemas = versioned.value();
                        schemas.lock();
                        schemas.insert(version, None);
                        schemas.unlock();
                        
                        // Remove from name index if we can find the name
                        if let Some(last) = schemas.back() {
                            if let Some(def) = last.value() {
                                self.schemas_by_name.remove(&def.name);
                            }
                        }
                    }
                }
                PendingWrite::TableCreate { def } | PendingWrite::TableMetadataUpdate { def } => {
                    let versioned = self.tables
                        .get_or_insert_with(def.id, VersionedTableDef::new);
                    let tables = versioned.value();
                    tables.lock();
                    tables.insert(version, Some(def.clone()));
                    tables.unlock();
                    
                    // Update name index
                    self.tables_by_name.insert((def.schema, def.name.clone()), def.id);
                }
                PendingWrite::TableDelete { id } => {
                    // Mark as deleted in versioned structure
                    if let Some(versioned) = self.tables.get(id) {
                        let tables = versioned.value();
                        tables.lock();
                        tables.insert(version, None);
                        tables.unlock();
                        
                        // Remove from name index if we can find the schema and name
                        if let Some(last) = tables.back() {
                            if let Some(def) = last.value() {
                                self.tables_by_name.remove(&(def.schema, def.name.clone()));
                            }
                        }
                    }
                }
                PendingWrite::ViewCreate { def } | PendingWrite::ViewUpdate { def } => {
                    let versioned = self.views
                        .get_or_insert_with(def.id, VersionedViewDef::new);
                    let views = versioned.value();
                    views.lock();
                    views.insert(version, Some(def.clone()));
                    views.unlock();
                    
                    // Update name index
                    self.views_by_name.insert((def.schema, def.name.clone()), def.id);
                }
                PendingWrite::ViewDelete { id } => {
                    // Mark as deleted in versioned structure
                    if let Some(versioned) = self.views.get(id) {
                        let views = versioned.value();
                        views.lock();
                        views.insert(version, None);
                        views.unlock();
                        
                        // Remove from name index if we can find the schema and name
                        if let Some(last) = views.back() {
                            if let Some(def) = last.value() {
                                self.views_by_name.remove(&(def.schema, def.name.clone()));
                            }
                        }
                    }
                }
                // Skip non-catalog operations
                _ => {}
            }
        }
    }
}