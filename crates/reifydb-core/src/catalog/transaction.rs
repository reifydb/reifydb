// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use crate::{
    Version,
    interface::{SchemaId, TableId, ViewId, SchemaDef, TableDef, ViewDef, PendingWrite},
};

use super::materialized::MaterializedCatalog;

impl MaterializedCatalog {
    /// Get a schema by ID at a specific version, checking pending writes first
    pub fn get_schema(
        &self,
        id: SchemaId,
        version: Version,
        pending: &[PendingWrite],
    ) -> Option<SchemaDef> {
        // First check pending writes for uncommitted changes
        for write in pending.iter().rev() {
            match write {
                PendingWrite::SchemaCreate { def } if def.id == id => {
                    return Some(def.clone());
                }
                PendingWrite::SchemaUpdate { def } if def.id == id => {
                    return Some(def.clone());
                }
                PendingWrite::SchemaDelete { id: deleted_id } if *deleted_id == id => {
                    return None;
                }
                _ => {}
            }
        }

        // Then check materialized catalog
        self.schemas.get(&id).and_then(|entry| {
            let versioned = entry.value();
            versioned
                .upper_bound(Bound::Included(&version))
                .and_then(|v| v.value().clone())
        })
    }

    /// Find a schema by name at a specific version, checking pending writes first
    pub fn find_schema_by_name(
        &self,
        name: &str,
        version: Version,
        pending: &[PendingWrite],
    ) -> Option<SchemaDef> {
        // First check pending writes for uncommitted changes
        for write in pending.iter().rev() {
            match write {
                PendingWrite::SchemaCreate { def } if def.name == name => {
                    return Some(def.clone());
                }
                PendingWrite::SchemaUpdate { def } if def.name == name => {
                    return Some(def.clone());
                }
                PendingWrite::SchemaDelete { id } => {
                    // Check if this deleted schema had the name we're looking for
                    if let Some(existing) = self.schemas.get(id) {
                        let versioned = existing.value();
                        if let Some(last) = versioned.back() {
                            if let Some(schema) = last.value() {
                                if schema.name == name {
                                    return None;
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Then check materialized catalog
        self.schemas_by_name.get(name).and_then(|entry| {
            let schema_id = *entry.value();
            self.get_schema(schema_id, version, &[])
        })
    }

    /// Get a table by ID at a specific version, checking pending writes first
    pub fn get_table(
        &self,
        id: TableId,
        version: Version,
        pending: &[PendingWrite],
    ) -> Option<TableDef> {
        // First check pending writes for uncommitted changes
        for write in pending.iter().rev() {
            match write {
                PendingWrite::TableCreate { def } if def.id == id => {
                    return Some(def.clone());
                }
                PendingWrite::TableMetadataUpdate { def } if def.id == id => {
                    return Some(def.clone());
                }
                PendingWrite::TableDelete { id: deleted_id } if *deleted_id == id => {
                    return None;
                }
                _ => {}
            }
        }

        // Then check materialized catalog
        self.tables.get(&id).and_then(|entry| {
            let versioned = entry.value();
            versioned
                .upper_bound(Bound::Included(&version))
                .and_then(|v| v.value().clone())
        })
    }

    /// Find a table by name in a schema at a specific version, checking pending writes first
    pub fn find_table_by_name(
        &self,
        schema_id: SchemaId,
        name: &str,
        version: Version,
        pending: &[PendingWrite],
    ) -> Option<TableDef> {
        // First check pending writes for uncommitted changes
        for write in pending.iter().rev() {
            match write {
                PendingWrite::TableCreate { def } 
                    if def.schema == schema_id && def.name == name => {
                    return Some(def.clone());
                }
                PendingWrite::TableMetadataUpdate { def } 
                    if def.schema == schema_id && def.name == name => {
                    return Some(def.clone());
                }
                PendingWrite::TableDelete { id } => {
                    // Check if this deleted table had the name we're looking for
                    if let Some(existing) = self.tables.get(id) {
                        let versioned = existing.value();
                        if let Some(last) = versioned.back() {
                            if let Some(table) = last.value() {
                                if table.schema == schema_id && table.name == name {
                                    return None;
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Then check materialized catalog
        self.tables_by_name.get(&(schema_id, name.to_string())).and_then(|entry| {
            let table_id = *entry.value();
            self.get_table(table_id, version, &[])
        })
    }

    /// Get a view by ID at a specific version, checking pending writes first
    pub fn get_view(
        &self,
        id: ViewId,
        version: Version,
        pending: &[PendingWrite],
    ) -> Option<ViewDef> {
        // First check pending writes for uncommitted changes
        for write in pending.iter().rev() {
            match write {
                PendingWrite::ViewCreate { def } if def.id == id => {
                    return Some(def.clone());
                }
                PendingWrite::ViewUpdate { def } if def.id == id => {
                    return Some(def.clone());
                }
                PendingWrite::ViewDelete { id: deleted_id } if *deleted_id == id => {
                    return None;
                }
                _ => {}
            }
        }

        // Then check materialized catalog
        self.views.get(&id).and_then(|entry| {
            let versioned = entry.value();
            versioned
                .upper_bound(Bound::Included(&version))
                .and_then(|v| v.value().clone())
        })
    }

    /// Find a view by name in a schema at a specific version, checking pending writes first
    pub fn find_view_by_name(
        &self,
        schema_id: SchemaId,
        name: &str,
        version: Version,
        pending: &[PendingWrite],
    ) -> Option<ViewDef> {
        // First check pending writes for uncommitted changes
        for write in pending.iter().rev() {
            match write {
                PendingWrite::ViewCreate { def } 
                    if def.schema == schema_id && def.name == name => {
                    return Some(def.clone());
                }
                PendingWrite::ViewUpdate { def } 
                    if def.schema == schema_id && def.name == name => {
                    return Some(def.clone());
                }
                PendingWrite::ViewDelete { id } => {
                    // Check if this deleted view had the name we're looking for
                    if let Some(existing) = self.views.get(id) {
                        let versioned = existing.value();
                        if let Some(last) = versioned.back() {
                            if let Some(view) = last.value() {
                                if view.schema == schema_id && view.name == name {
                                    return None;
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Then check materialized catalog
        self.views_by_name.get(&(schema_id, name.to_string())).and_then(|entry| {
            let view_id = *entry.value();
            self.get_view(view_id, version, &[])
        })
    }
}