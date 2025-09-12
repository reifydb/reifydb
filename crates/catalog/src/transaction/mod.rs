// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, QueryTransaction, TransactionalChanges,
	interceptor::WithInterceptors,
};

mod namespace;
mod source;
mod table;
mod view;

pub trait MaterializedCatalogTransaction {
	fn catalog(&self) -> &MaterializedCatalog;
}

pub trait CatalogCommandTransaction:
	CatalogQueryTransaction
	+ CatalogNamespaceCommandOperations
	+ CatalogTableCommandOperations
	+ CatalogViewCommandOperations
{
}

pub trait CatalogTrackChangeOperations:
	CatalogTrackNamespaceChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackViewChangeOperations
{
}

pub trait CatalogQueryTransaction:
	CatalogNamespaceQueryOperations
	+ CatalogSourceQueryOperations
	+ CatalogTableQueryOperations
	+ CatalogViewQueryOperations
{
}

impl<
	QT: QueryTransaction
		+ MaterializedCatalogTransaction
		+ TransactionalChanges,
> CatalogQueryTransaction for QT
{
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogCommandTransaction for CT
{
}

pub use namespace::{
	CatalogNamespaceCommandOperations, CatalogNamespaceQueryOperations,
	CatalogTrackNamespaceChangeOperations,
};
pub use source::CatalogSourceQueryOperations;
pub use table::{
	CatalogTableCommandOperations, CatalogTableQueryOperations,
	CatalogTrackTableChangeOperations,
};
pub use view::{
	CatalogTrackViewChangeOperations, CatalogViewCommandOperations,
	CatalogViewQueryOperations,
};

use crate::MaterializedCatalog;
