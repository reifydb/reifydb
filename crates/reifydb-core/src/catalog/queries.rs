// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use crate::{
    Version,
    interface::{SchemaId, TableId, ViewId, SchemaDef, TableDef, ViewDef},
};

use super::materialized::MaterializedCatalog;

impl MaterializedCatalog {
    /// Get a schema by ID at a specific version
    pub fn get_schema(
        &self,
        id: SchemaId,
        version: Version,
    ) -> Option<SchemaDef> {
        self.schemas.get(&id).and_then(|entry| {
            let versioned = entry.value();
            versioned
                .upper_bound(Bound::Included(&version))
                .and_then(|v| v.value().clone())
        })
    }

    /// Find a schema by name at a specific version
    pub fn find_schema_by_name(
        &self,
        name: &str,
        version: Version,
    ) -> Option<SchemaDef> {
        self.schemas_by_name.get(name).and_then(|entry| {
            let schema_id = *entry.value();
            self.get_schema(schema_id, version)
        })
    }

    /// Get a table by ID at a specific version
    pub fn get_table(
        &self,
        id: TableId,
        version: Version,
    ) -> Option<TableDef> {
        self.tables.get(&id).and_then(|entry| {
            let versioned = entry.value();
            versioned
                .upper_bound(Bound::Included(&version))
                .and_then(|v| v.value().clone())
        })
    }

    /// Find a table by name in a schema at a specific version
    pub fn find_table_by_name(
        &self,
        schema_id: SchemaId,
        name: &str,
        version: Version,
    ) -> Option<TableDef> {
        self.tables_by_name.get(&(schema_id, name.to_string())).and_then(|entry| {
            let table_id = *entry.value();
            self.get_table(table_id, version)
        })
    }

    /// Get a view by ID at a specific version
    pub fn get_view(
        &self,
        id: ViewId,
        version: Version,
    ) -> Option<ViewDef> {
        self.views.get(&id).and_then(|entry| {
            let versioned = entry.value();
            versioned
                .upper_bound(Bound::Included(&version))
                .and_then(|v| v.value().clone())
        })
    }

    /// Find a view by name in a schema at a specific version
    pub fn find_view_by_name(
        &self,
        schema_id: SchemaId,
        name: &str,
        version: Version,
    ) -> Option<ViewDef> {
        self.views_by_name.get(&(schema_id, name.to_string())).and_then(|entry| {
            let view_id = *entry.value();
            self.get_view(view_id, version)
        })
    }
}