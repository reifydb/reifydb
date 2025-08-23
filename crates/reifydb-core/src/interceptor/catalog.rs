// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
    catalog::MaterializedCatalog,
    interceptor::{
        SchemaDefPostCreateInterceptor, SchemaDefPostCreateContext,
        SchemaDefPreUpdateInterceptor, SchemaDefPreUpdateContext,
        SchemaDefPostUpdateInterceptor, SchemaDefPostUpdateContext,
        SchemaDefPreDeleteInterceptor, SchemaDefPreDeleteContext,
        TableDefPostCreateInterceptor, TableDefPostCreateContext,
        TableDefPreUpdateInterceptor, TableDefPreUpdateContext,
        TableDefPostUpdateInterceptor, TableDefPostUpdateContext,
        TableDefPreDeleteInterceptor, TableDefPreDeleteContext,
        ViewDefPostCreateInterceptor, ViewDefPostCreateContext,
        ViewDefPreUpdateInterceptor, ViewDefPreUpdateContext,
        ViewDefPostUpdateInterceptor, ViewDefPostUpdateContext,
        ViewDefPreDeleteInterceptor, ViewDefPreDeleteContext,
        PostCommitInterceptor, PostCommitContext,
    },
    interface::CommandTransaction,
    Version,
};

/// Interceptor that updates the materialized catalog for schema definitions
pub struct MaterializedSchemaInterceptor {
    catalog: MaterializedCatalog,
}

impl MaterializedSchemaInterceptor {
    pub fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog }
    }
}

impl<CT: CommandTransaction> SchemaDefPostCreateInterceptor<CT> 
    for MaterializedSchemaInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut SchemaDefPostCreateContext<CT>,
    ) -> crate::Result<()> {
        // Add the schema to the materialized catalog
        let schema = ctx.post.clone();
        // Use read version for now - final version assigned at commit
        let version: Version = ctx.txn.version();

        dbg!(&ctx.txn.id());

        self.catalog.schemas
            .get_or_insert_with(schema.id, || crate::catalog::versioned::VersionedSchemaDef::new())
            .value()
            .insert(version, Some(schema.clone()));
        
        // Update the name index
        self.catalog.schemas_by_name.insert(schema.name.clone(), schema.id);
        
        Ok(())
    }
}

impl<CT: CommandTransaction> SchemaDefPreUpdateInterceptor<CT> 
    for MaterializedSchemaInterceptor 
{
    fn intercept(
        &self,
        _ctx: &mut SchemaDefPreUpdateContext<CT>,
    ) -> crate::Result<()> {
        // Nothing to do on pre-update
        Ok(())
    }
}

impl<CT: CommandTransaction> SchemaDefPostUpdateInterceptor<CT> 
    for MaterializedSchemaInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut SchemaDefPostUpdateContext<CT>,
    ) -> crate::Result<()> {
        // Update the schema in the materialized catalog
        let old_schema = ctx.pre.clone();
        let new_schema = ctx.post.clone();
        
        // Update the versioned schema
        // Use read version for now - final version assigned at commit
        let version: Version = ctx.txn.version();
        if let Some(entry) = self.catalog.schemas.get(&new_schema.id) {
            entry.value().insert(version, Some(new_schema.clone()));
        }
        
        // Update name index if name changed
        if old_schema.name != new_schema.name {
            self.catalog.schemas_by_name.remove(&old_schema.name);
            self.catalog.schemas_by_name.insert(new_schema.name.clone(), new_schema.id);
        }
        
        Ok(())
    }
}

impl<CT: CommandTransaction> SchemaDefPreDeleteInterceptor<CT> 
    for MaterializedSchemaInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut SchemaDefPreDeleteContext<CT>,
    ) -> crate::Result<()> {
        // Mark the schema as deleted
        let schema = ctx.pre.clone();
        
        // TODO: Get version from transaction
        let version: Version = 0;
        if let Some(entry) = self.catalog.schemas.get(&schema.id) {
            entry.value().insert(version, None);
        }
        
        // Remove from name index
        self.catalog.schemas_by_name.remove(&schema.name);
        
        Ok(())
    }
}

/// Interceptor that updates the materialized catalog for table definitions
pub struct MaterializedTableInterceptor {
    catalog: MaterializedCatalog,
}

impl MaterializedTableInterceptor {
    pub fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog }
    }
}

impl<CT: CommandTransaction> TableDefPostCreateInterceptor<CT> 
    for MaterializedTableInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut TableDefPostCreateContext<CT>,
    ) -> crate::Result<()> {
        // Add the table to the materialized catalog
        let table = ctx.post.clone();
        // TODO: Get version from transaction
        let version: Version = 0;
        self.catalog.tables
            .get_or_insert_with(table.id, || crate::catalog::versioned::VersionedTableDef::new())
            .value()
            .insert(version, Some(table.clone()));
        
        // Update the name index
        self.catalog.tables_by_name.insert((table.schema, table.name.clone()), table.id);
        
        Ok(())
    }
}

impl<CT: CommandTransaction> TableDefPreUpdateInterceptor<CT> 
    for MaterializedTableInterceptor 
{
    fn intercept(
        &self,
        _ctx: &mut TableDefPreUpdateContext<CT>,
    ) -> crate::Result<()> {
        // Nothing to do on pre-update
        Ok(())
    }
}

impl<CT: CommandTransaction> TableDefPostUpdateInterceptor<CT> 
    for MaterializedTableInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut TableDefPostUpdateContext<CT>,
    ) -> crate::Result<()> {
        // Update the table in the materialized catalog
        let old_table = ctx.pre.clone();
        let new_table = ctx.post.clone();
        
        // Update the versioned table
        // TODO: Get version from transaction
        let version: Version = 0;
        if let Some(entry) = self.catalog.tables.get(&new_table.id) {
            entry.value().insert(version, Some(new_table.clone()));
        }
        
        // Update name index if name or schema changed
        if old_table.name != new_table.name || old_table.schema != new_table.schema {
            self.catalog.tables_by_name.remove(&(old_table.schema, old_table.name));
            self.catalog.tables_by_name.insert((new_table.schema, new_table.name.clone()), new_table.id);
        }
        
        Ok(())
    }
}

impl<CT: CommandTransaction> TableDefPreDeleteInterceptor<CT> 
    for MaterializedTableInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut TableDefPreDeleteContext<CT>,
    ) -> crate::Result<()> {
        // Mark the table as deleted
        let table = ctx.pre.clone();
        
        // TODO: Get version from transaction
        let version: Version = 0;
        if let Some(entry) = self.catalog.tables.get(&table.id) {
            entry.value().insert(version, None);
        }
        
        // Remove from name index
        self.catalog.tables_by_name.remove(&(table.schema, table.name));
        
        Ok(())
    }
}

/// Interceptor that updates the materialized catalog for view definitions
pub struct MaterializedViewInterceptor {
    catalog: MaterializedCatalog,
}

impl MaterializedViewInterceptor {
    pub fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog }
    }
}

impl<CT: CommandTransaction> ViewDefPostCreateInterceptor<CT> 
    for MaterializedViewInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut ViewDefPostCreateContext<CT>,
    ) -> crate::Result<()> {
        // Add the view to the materialized catalog
        let view = ctx.post.clone();
        // TODO: Get version from transaction
        let version: Version = 0;
        self.catalog.views
            .get_or_insert_with(view.id, || crate::catalog::versioned::VersionedViewDef::new())
            .value()
            .insert(version, Some(view.clone()));
        
        // Update the name index
        self.catalog.views_by_name.insert((view.schema, view.name.clone()), view.id);
        
        Ok(())
    }
}

impl<CT: CommandTransaction> ViewDefPreUpdateInterceptor<CT> 
    for MaterializedViewInterceptor 
{
    fn intercept(
        &self,
        _ctx: &mut ViewDefPreUpdateContext<CT>,
    ) -> crate::Result<()> {
        // Nothing to do on pre-update
        Ok(())
    }
}

impl<CT: CommandTransaction> ViewDefPostUpdateInterceptor<CT> 
    for MaterializedViewInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut ViewDefPostUpdateContext<CT>,
    ) -> crate::Result<()> {
        // Update the view in the materialized catalog
        let old_view = ctx.pre.clone();
        let new_view = ctx.post.clone();
        
        // Update the versioned view
        // TODO: Get version from transaction
        let version: Version = 0;
        if let Some(entry) = self.catalog.views.get(&new_view.id) {
            entry.value().insert(version, Some(new_view.clone()));
        }
        
        // Update name index if name or schema changed
        if old_view.name != new_view.name || old_view.schema != new_view.schema {
            self.catalog.views_by_name.remove(&(old_view.schema, old_view.name));
            self.catalog.views_by_name.insert((new_view.schema, new_view.name.clone()), new_view.id);
        }
        
        Ok(())
    }
}

impl<CT: CommandTransaction> ViewDefPreDeleteInterceptor<CT> 
    for MaterializedViewInterceptor 
{
    fn intercept(
        &self,
        ctx: &mut ViewDefPreDeleteContext<CT>,
    ) -> crate::Result<()> {
        // Mark the view as deleted
        let view = ctx.pre.clone();
        
        // TODO: Get version from transaction
        let version: Version = 0;
        if let Some(entry) = self.catalog.views.get(&view.id) {
            entry.value().insert(version, None);
        }
        
        // Remove from name index
        self.catalog.views_by_name.remove(&(view.schema, view.name));
        
        Ok(())
    }
}

/// Post-commit interceptor that finalizes catalog changes
pub struct CatalogCommitInterceptor {
    catalog: MaterializedCatalog,
}

impl CatalogCommitInterceptor {
    pub fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog }
    }
}

impl<CT: CommandTransaction> PostCommitInterceptor<CT> 
    for CatalogCommitInterceptor 
{
    fn intercept(
        &self,
        _ctx: &mut PostCommitContext,
    ) -> crate::Result<()> {
        // The actual updates have already been done by the definition interceptors
        // This is just a placeholder if we need post-commit finalization
        let version = _ctx.version;
        dbg!(&version, _ctx.id);
        Ok(())
    }
}