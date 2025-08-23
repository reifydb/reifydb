// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
    catalog::MaterializedCatalog,
    interceptor::{PostCommitInterceptor, PostCommitContext},
    interface::Transaction,
    transaction::StandardCommandTransaction,
};

/// Interceptor that updates the materialized catalog after successful commits
pub struct CatalogInterceptor {
    catalog: MaterializedCatalog,
}

impl CatalogInterceptor {
    pub fn new(catalog: MaterializedCatalog) -> Self {
        Self { catalog }
    }
}

impl<T: Transaction> PostCommitInterceptor<StandardCommandTransaction<T>> 
    for CatalogInterceptor 
{
    fn intercept(
        &self,
        _ctx: &mut PostCommitContext,
    ) -> crate::Result<()> {
        // TODO: Need access to pending writes to update the catalog
        // For now, we can't apply changes without access to the transaction
        // The version is available as ctx.version
        // But we need the pending writes to know what catalog changes to apply
        
        Ok(())
    }
}