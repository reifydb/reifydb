// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	CommandTransaction, QueryTransaction, TransactionalChanges, interceptor::WithInterceptors,
};

mod dictionary;
mod flow;
mod namespace;
mod primitive;
mod ringbuffer;
mod table;
mod table_virtual_user;
mod view;

pub trait MaterializedCatalogTransaction {
	fn catalog(&self) -> &MaterializedCatalog;
}

pub trait CatalogCommandTransaction:
	CatalogQueryTransaction
	+ CatalogDictionaryCommandOperations
	+ CatalogNamespaceCommandOperations
	+ CatalogRingBufferCommandOperations
	+ CatalogTableCommandOperations
	+ CatalogViewCommandOperations
{
}

pub trait CatalogTrackChangeOperations:
	CatalogTrackDictionaryChangeOperations
	+ CatalogTrackFlowChangeOperations
	+ CatalogTrackNamespaceChangeOperations
	+ CatalogTrackRingBufferChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackViewChangeOperations
{
}

pub trait CatalogQueryTransaction:
	CatalogDictionaryQueryOperations
	+ CatalogFlowQueryOperations
	+ CatalogNamespaceQueryOperations
	+ CatalogRingBufferQueryOperations
	+ CatalogPrimitiveQueryOperations
	+ CatalogTableQueryOperations
	+ CatalogTableVirtualUserQueryOperations
	+ CatalogViewQueryOperations
{
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction + TransactionalChanges + 'static> CatalogQueryTransaction
	for QT
{
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges
		+ Send
		+ 'static,
> CatalogCommandTransaction for CT
{
}

pub use dictionary::{
	CatalogDictionaryCommandOperations, CatalogDictionaryQueryOperations, CatalogTrackDictionaryChangeOperations,
};
pub use flow::{CatalogFlowQueryOperations, CatalogTrackFlowChangeOperations};
pub use namespace::{
	CatalogNamespaceCommandOperations, CatalogNamespaceQueryOperations, CatalogTrackNamespaceChangeOperations,
};
pub use primitive::CatalogPrimitiveQueryOperations;
pub use ringbuffer::{
	CatalogRingBufferCommandOperations, CatalogRingBufferQueryOperations, CatalogTrackRingBufferChangeOperations,
};
pub use table::{CatalogTableCommandOperations, CatalogTableQueryOperations, CatalogTrackTableChangeOperations};
pub use table_virtual_user::CatalogTableVirtualUserQueryOperations;
pub use view::{CatalogTrackViewChangeOperations, CatalogViewCommandOperations, CatalogViewQueryOperations};

use crate::MaterializedCatalog;
