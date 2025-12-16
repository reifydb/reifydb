// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		CommandTransaction, NamespaceId, QueryTransaction, RingBufferDef, RingBufferId, TransactionalChanges,
		interceptor::WithInterceptors,
	},
	return_error,
};
use reifydb_type::{
	IntoFragment,
	diagnostic::catalog::{ringbuffer_already_exists, ringbuffer_not_found},
};
use tracing::instrument;

use crate::{CatalogStore, store::ringbuffer::create::RingBufferToCreate, transaction::MaterializedCatalogTransaction};

pub trait CatalogRingBufferQueryOperations {
	fn find_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<Option<RingBufferDef>>;

	fn find_ringbuffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<RingBufferDef>>;

	fn get_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<RingBufferDef>;

	fn get_ringbuffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<RingBufferDef>;
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction> CatalogRingBufferQueryOperations for QT {
	#[instrument(level = "trace", skip(self))]
	fn find_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<Option<RingBufferDef>> {
		CatalogStore::find_ringbuffer(self, id)
	}

	#[instrument(level = "trace", skip(self, name))]
	fn find_ringbuffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<RingBufferDef>> {
		let name = name.into_fragment();
		CatalogStore::find_ringbuffer_by_name(self, namespace, name.text())
	}

	#[instrument(level = "trace", skip(self))]
	fn get_ringbuffer(&mut self, id: RingBufferId) -> crate::Result<RingBufferDef> {
		CatalogStore::get_ringbuffer(self, id)
	}

	#[instrument(level = "trace", skip(self, name))]
	fn get_ringbuffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<RingBufferDef> {
		let name = name.into_fragment();
		let name_text = name.text().to_string();
		let ringbuffer = self.find_ringbuffer_by_name(namespace, name.clone())?;
		match ringbuffer {
			Some(rb) => Ok(rb),
			None => {
				let namespace = CatalogStore::get_namespace(self, namespace)?;
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

pub trait CatalogRingBufferCommandOperations: CatalogRingBufferQueryOperations {
	fn create_ringbuffer(&mut self, to_create: RingBufferToCreate) -> crate::Result<RingBufferDef>;
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackRingBufferChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogRingBufferCommandOperations for CT
{
	#[instrument(level = "debug", skip(self, to_create))]
	fn create_ringbuffer(&mut self, to_create: RingBufferToCreate) -> crate::Result<RingBufferDef> {
		if let Some(_ringbuffer) = self.find_ringbuffer_by_name(to_create.namespace, &to_create.ringbuffer)? {
			let namespace = CatalogStore::get_namespace(self, to_create.namespace)?;
			return_error!(ringbuffer_already_exists(
				to_create.fragment.unwrap_or_default(),
				&namespace.name,
				&to_create.ringbuffer
			));
		}

		CatalogStore::create_ringbuffer(self, to_create)
	}
}
