// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogViewQueryOperations,
	transaction::CatalogTrackViewChangeOperations,
};
use reifydb_core::interface::{
	NamespaceId, Transaction, VersionedQueryTransaction, ViewDef, ViewId,
};

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl<T: Transaction> CatalogTrackViewChangeOperations
	for StandardCommandTransaction<T>
{
	fn track_view_def_created(
		&mut self,
		view: ViewDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}

	fn track_view_def_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}

	fn track_view_def_deleted(
		&mut self,
		view: ViewDef,
	) -> reifydb_core::Result<()> {
		todo!()
	}
}

// impl<T: Transaction> CatalogViewQueryOperations
// 	for StandardQueryTransaction<T>
// {
// 	fn find_view_by_name(
// 		&mut self,
// 		namespace: NamespaceId,
// 		name: impl AsRef<str>,
// 	) -> crate::Result<Option<ViewDef>> {
// 		let name = name.as_ref();
//
// 		Ok(self.catalog.find_view_by_name(
// 			namespace,
// 			name,
// 			VersionedQueryTransaction::version(self),
// 		))
// 	}
//
// 	fn find_view(&mut self, id: ViewId) -> crate::Result<Option<ViewDef>> {
// 		Ok(self.catalog.find_view(
// 			id,
// 			VersionedQueryTransaction::version(self),
// 		))
// 	}
//
// 	fn get_view_by_name(
// 		&mut self,
// 		_namespace: NamespaceId,
// 		_name: impl AsRef<str>,
// 	) -> reifydb_core::Result<ViewDef> {
// 		todo!()
// 	}
// }
