// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub use reifydb_core::Result;

pub mod column;
pub mod column_policy;
mod loader;
mod materialized;
pub mod primary_key;
pub mod row;
pub mod schema;
pub mod sequence;
pub mod store;
pub mod table;
pub mod test_utils;
pub mod transaction;
pub mod view;

pub use loader::MaterializedCatalogLoader;
pub use materialized::MaterializedCatalog;
pub use transaction::{
	CatalogCommandTransaction, CatalogCommandTransactionOperations,
	CatalogQueryTransaction, CatalogQueryTransactionOperations,
	CatalogSchemaCommandOperations, CatalogSchemaQueryOperations,
	CatalogTableCommandOperations, CatalogTableQueryOperations,
	CatalogViewCommandOperations, CatalogViewQueryOperations,
	TransactionalChangesExt,
};

pub struct CatalogStore;
