// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod command;
mod namespace;
mod query;
mod table;
mod view;

// Re-export the transaction traits
pub use command::{
	CatalogCommandTransaction, CatalogCommandTransactionOperations,
	CatalogNamespaceCommandOperations, CatalogTableCommandOperations,
	CatalogViewCommandOperations,
};
pub use query::{
	CatalogNamespaceQueryOperations, CatalogQueryTransaction,
	CatalogSourceQueryOperations, CatalogTableQueryOperations,
	CatalogTransaction, CatalogViewQueryOperations,
	TransactionalChangesExt,
};
