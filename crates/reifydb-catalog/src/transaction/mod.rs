// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod command;
mod query;
mod schema;
mod table;
mod view;

// Re-export the new transaction traits
// Legacy re-exports for backward compatibility (to be removed later)
pub use command::CatalogCommandTransaction as CatalogTransaction;
pub use command::{
	CatalogCommandTransaction, CatalogCommandTransactionOperations,
	CatalogCommandTransactionOperations as CatalogTransactionOperations,
	CatalogSchemaCommandOperations, CatalogTableCommandOperations,
	CatalogViewCommandOperations,
};
pub use query::{
	CatalogQueryTransaction, CatalogQueryTransactionOperations,
	CatalogSchemaQueryOperations, CatalogTableQueryOperations,
	CatalogViewQueryOperations, TransactionalChangesExt,
};
