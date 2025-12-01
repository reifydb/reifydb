// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	error,
	interface::{NamespaceId, QueryTransaction, SourceDef, SourceId},
};
use reifydb_type::{IntoFragment, internal};

use crate::{CatalogTableQueryOperations, CatalogViewQueryOperations};

pub trait CatalogSourceQueryOperations {
	fn find_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		source: impl IntoFragment<'a>,
	) -> crate::Result<Option<SourceDef>>;

	fn find_source(&mut self, id: SourceId) -> crate::Result<Option<SourceDef>>;

	fn get_source(&mut self, id: SourceId) -> crate::Result<SourceDef>;

	fn get_source_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<SourceDef>;
}

impl<T: QueryTransaction + CatalogTableQueryOperations + CatalogViewQueryOperations> CatalogSourceQueryOperations
	for T
{
	fn find_source_by_name<'a>(
		&mut self,
		_namespace: NamespaceId,
		_source: impl IntoFragment<'a>,
	) -> reifydb_core::Result<Option<SourceDef>> {
		todo!()
	}

	fn find_source(&mut self, id: SourceId) -> reifydb_core::Result<Option<SourceDef>> {
		match id {
			SourceId::Table(table_id) => {
				Ok(self.find_table(table_id)?.and_then(|s| Some(SourceDef::Table(s))))
			}
			SourceId::View(view_id) => Ok(self.find_view(view_id)?.and_then(|s| Some(SourceDef::View(s)))),
			SourceId::Flow(_) => unimplemented!(),
			SourceId::TableVirtual(_) => unimplemented!(),
			SourceId::RingBuffer(_) => unimplemented!(),
			SourceId::Dictionary(_) => unimplemented!(),
		}
	}

	fn get_source(&mut self, id: SourceId) -> reifydb_core::Result<SourceDef> {
		self.find_source(id)?.ok_or_else(|| {
			error!(internal!(
				"Source with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	fn get_source_by_name<'a>(
		&mut self,
		_namespace: NamespaceId,
		_name: impl IntoFragment<'a>,
	) -> reifydb_core::Result<SourceDef> {
		todo!()
	}
}
