// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	error,
	interface::{NamespaceId, QueryTransaction, SourceDef, SourceId},
};
use reifydb_type::internal;
use tracing::instrument;

use crate::{CatalogTableQueryOperations, CatalogViewQueryOperations};

#[async_trait]
pub trait CatalogSourceQueryOperations {
	async fn find_source_by_name(
		&mut self,
		namespace: NamespaceId,
		source: &str,
	) -> crate::Result<Option<SourceDef>>;

	async fn find_source(&mut self, id: SourceId) -> crate::Result<Option<SourceDef>>;

	async fn get_source(&mut self, id: SourceId) -> crate::Result<SourceDef>;

	async fn get_source_by_name(&mut self, namespace: NamespaceId, name: &str) -> crate::Result<SourceDef>;
}

#[async_trait]
impl<T: QueryTransaction + CatalogTableQueryOperations + CatalogViewQueryOperations> CatalogSourceQueryOperations
	for T
{
	#[instrument(name = "catalog::source::find_by_name", level = "trace", skip(self, _source))]
	async fn find_source_by_name(
		&mut self,
		_namespace: NamespaceId,
		_source: &str,
	) -> reifydb_core::Result<Option<SourceDef>> {
		todo!()
	}

	#[instrument(name = "catalog::source::find", level = "trace", skip(self))]
	async fn find_source(&mut self, id: SourceId) -> reifydb_core::Result<Option<SourceDef>> {
		match id {
			SourceId::Table(table_id) => {
				Ok(self.find_table(table_id).await?.and_then(|s| Some(SourceDef::Table(s))))
			}
			SourceId::View(view_id) => {
				Ok(self.find_view(view_id).await?.and_then(|s| Some(SourceDef::View(s))))
			}
			SourceId::Flow(_) => unimplemented!(),
			SourceId::TableVirtual(_) => unimplemented!(),
			SourceId::RingBuffer(_) => unimplemented!(),
			SourceId::Dictionary(_) => unimplemented!(),
		}
	}

	#[instrument(name = "catalog::source::get", level = "trace", skip(self))]
	async fn get_source(&mut self, id: SourceId) -> reifydb_core::Result<SourceDef> {
		self.find_source(id).await?.ok_or_else(|| {
			error!(internal!(
				"Source with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::source::get_by_name", level = "trace", skip(self, _name))]
	async fn get_source_by_name(
		&mut self,
		_namespace: NamespaceId,
		_name: &str,
	) -> reifydb_core::Result<SourceDef> {
		todo!()
	}
}
