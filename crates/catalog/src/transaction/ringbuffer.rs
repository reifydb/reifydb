// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	interface::{
		CommandTransaction, NamespaceId, QueryTransaction, RingBufferDef, RingBufferId, TransactionalChanges,
		interceptor::WithInterceptors,
	},
	return_error,
};
use reifydb_type::{
	Fragment,
	diagnostic::catalog::{ringbuffer_already_exists, ringbuffer_not_found},
};
use tracing::instrument;

use crate::{CatalogStore, store::ringbuffer::create::RingBufferToCreate, transaction::MaterializedCatalogTransaction};

#[async_trait(?Send)]
pub trait CatalogRingBufferQueryOperations: Send {
	async fn find_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<Option<RingBufferDef>>;

	async fn find_ringbuffer_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<Option<RingBufferDef>>;

	async fn get_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<RingBufferDef>;

	async fn get_ringbuffer_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<RingBufferDef>;
}

#[async_trait(?Send)]
impl<QT: QueryTransaction + MaterializedCatalogTransaction + Send> CatalogRingBufferQueryOperations for QT {
	#[instrument(name = "catalog::ringbuffer::find", level = "trace", skip(self))]
	async fn find_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<Option<RingBufferDef>> {
		CatalogStore::find_ringbuffer(self, id).await
	}

	#[instrument(name = "catalog::ringbuffer::find_by_name", level = "trace", skip(self, name))]
	async fn find_ringbuffer_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<Option<RingBufferDef>> {
		let name = name.into();
		CatalogStore::find_ringbuffer_by_name(self, namespace, name.text()).await
	}

	#[instrument(name = "catalog::ringbuffer::get", level = "trace", skip(self))]
	async fn get_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<RingBufferDef> {
		CatalogStore::get_ringbuffer(self, id).await
	}

	#[instrument(name = "catalog::ringbuffer::get_by_name", level = "trace", skip(self, name))]
	async fn get_ringbuffer_by_name(
		&mut self,
		namespace: NamespaceId,
		name: impl Into<Fragment>,
	) -> crate::Result<RingBufferDef> {
		let name = name.into();
		let name_text = name.text().to_string();
		let ringbuffer = self.find_ringbuffer_by_name(namespace, name.clone()).await?;
		match ringbuffer {
			Some(rb) => Ok(rb),
			None => {
				let namespace = CatalogStore::get_namespace(self, namespace).await?;
				return_error!(ringbuffer_not_found(name, &namespace.name, &name_text))
			}
		}
	}
}

pub trait CatalogTrackRingBufferChangeOperations {
	// Ring buffer tracking methods
	fn track_ringbuffer_def_created(&mut self, ringbuffer: RingBufferDef) -> crate::Result<()>;

	fn track_ringbuffer_def_updated(&mut self, pre: RingBufferDef, post: RingBufferDef) -> crate::Result<()>;

	fn track_ringbuffer_def_deleted(&mut self, ringbuffer: RingBufferDef) -> crate::Result<()>;
}

#[async_trait(?Send)]
pub trait CatalogRingBufferCommandOperations: CatalogRingBufferQueryOperations {
	async fn create_ringbuffer(&mut self, to_create: RingBufferToCreate) -> crate::Result<RingBufferDef>;
}

#[async_trait(?Send)]
impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackRingBufferChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges
		+ Send,
> CatalogRingBufferCommandOperations for CT
{
	#[instrument(name = "catalog::ringbuffer::create", level = "debug", skip(self, to_create))]
	async fn create_ringbuffer(&mut self, to_create: RingBufferToCreate) -> crate::Result<RingBufferDef> {
		if let Some(_ringbuffer) =
			self.find_ringbuffer_by_name(to_create.namespace, to_create.ringbuffer.as_str()).await?
		{
			let namespace = CatalogStore::get_namespace(self, to_create.namespace).await?;
			return_error!(ringbuffer_already_exists(
				to_create.fragment.unwrap_or_default(),
				&namespace.name,
				&to_create.ringbuffer
			));
		}

		CatalogStore::create_ringbuffer(self, to_create).await
	}
}
