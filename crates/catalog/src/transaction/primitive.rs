// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_core::{
	error,
	interface::{NamespaceId, PrimitiveDef, PrimitiveId, QueryTransaction},
};
use reifydb_type::internal;
use tracing::instrument;

use crate::{CatalogTableQueryOperations, CatalogViewQueryOperations};

#[async_trait]
pub trait CatalogPrimitiveQueryOperations {
	async fn find_primitive_by_name(
		&mut self,
		namespace: NamespaceId,
		source: &str,
	) -> crate::Result<Option<PrimitiveDef>>;

	async fn find_primitive(&mut self, id: PrimitiveId) -> crate::Result<Option<PrimitiveDef>>;

	async fn get_primitive(&mut self, id: PrimitiveId) -> crate::Result<PrimitiveDef>;

	async fn get_primitive_by_name(&mut self, namespace: NamespaceId, name: &str) -> crate::Result<PrimitiveDef>;
}

#[async_trait]
impl<T: QueryTransaction + CatalogTableQueryOperations + CatalogViewQueryOperations> CatalogPrimitiveQueryOperations
	for T
{
	#[instrument(name = "catalog::primitive::find_by_name", level = "trace", skip(self, _source))]
	async fn find_primitive_by_name(
		&mut self,
		_namespace: NamespaceId,
		_source: &str,
	) -> reifydb_core::Result<Option<PrimitiveDef>> {
		todo!()
	}

	#[instrument(name = "catalog::primitive::find", level = "trace", skip(self))]
	async fn find_primitive(&mut self, id: PrimitiveId) -> reifydb_core::Result<Option<PrimitiveDef>> {
		match id {
			PrimitiveId::Table(table_id) => {
				Ok(self.find_table(table_id).await?.and_then(|s| Some(PrimitiveDef::Table(s))))
			}
			PrimitiveId::View(view_id) => {
				Ok(self.find_view(view_id).await?.and_then(|s| Some(PrimitiveDef::View(s))))
			}
			PrimitiveId::Flow(_) => unimplemented!(),
			PrimitiveId::TableVirtual(_) => unimplemented!(),
			PrimitiveId::RingBuffer(_) => unimplemented!(),
			PrimitiveId::Dictionary(_) => unimplemented!(),
		}
	}

	#[instrument(name = "catalog::primitive::get", level = "trace", skip(self))]
	async fn get_primitive(&mut self, id: PrimitiveId) -> reifydb_core::Result<PrimitiveDef> {
		self.find_primitive(id).await?.ok_or_else(|| {
			error!(internal!(
				"Primitive with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	#[instrument(name = "catalog::primitive::get_by_name", level = "trace", skip(self, _name))]
	async fn get_primitive_by_name(
		&mut self,
		_namespace: NamespaceId,
		_name: &str,
	) -> reifydb_core::Result<PrimitiveDef> {
		todo!()
	}
}
