// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, QueryTransaction, TransactionalChanges, interceptor::WithInterceptors,
};

mod flow;
mod namespace;
mod ring_buffer;
mod source;
mod table;
mod view;

pub trait MaterializedCatalogTransaction {
	fn catalog(&self) -> &MaterializedCatalog;
}

pub trait CatalogCommandTransaction:
	CatalogQueryTransaction
	+ CatalogNamespaceCommandOperations
	+ CatalogRingBufferCommandOperations
	+ CatalogTableCommandOperations
	+ CatalogViewCommandOperations
{
}

pub trait CatalogTrackChangeOperations:
	CatalogTrackNamespaceChangeOperations
	+ CatalogTrackRingBufferChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackViewChangeOperations
{
}

pub trait CatalogQueryTransaction:
	CatalogFlowQueryOperations
	+ CatalogNamespaceQueryOperations
	+ CatalogRingBufferQueryOperations
	+ CatalogSourceQueryOperations
	+ CatalogTableQueryOperations
	+ CatalogViewQueryOperations
{
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges> CatalogQueryTransaction for QT {}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogCommandTransaction for CT
{
}

pub use flow::CatalogFlowQueryOperations;
pub use namespace::{
	CatalogNamespaceCommandOperations, CatalogNamespaceQueryOperations, CatalogTrackNamespaceChangeOperations,
};
pub use ring_buffer::{
	CatalogRingBufferCommandOperations, CatalogRingBufferQueryOperations, CatalogTrackRingBufferChangeOperations,
};
pub use source::CatalogSourceQueryOperations;
pub use table::{CatalogTableCommandOperations, CatalogTableQueryOperations, CatalogTrackTableChangeOperations};
pub use view::{CatalogTrackViewChangeOperations, CatalogViewCommandOperations, CatalogViewQueryOperations};

use crate::MaterializedCatalog;
