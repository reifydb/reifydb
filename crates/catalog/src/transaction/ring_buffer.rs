// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		CommandTransaction, NamespaceId, QueryTransaction,
		RingBufferDef, RingBufferId, TransactionalChanges,
		interceptor::WithInterceptors,
	},
	return_error,
};
use reifydb_type::{
	IntoFragment,
	diagnostic::catalog::{
		ring_buffer_already_exists, ring_buffer_not_found,
	},
};

use crate::{
	CatalogStore, ring_buffer::create::RingBufferToCreate,
	transaction::MaterializedCatalogTransaction,
};

pub trait CatalogRingBufferQueryOperations {
	fn find_ring_buffer(
		&mut self,
		id: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>>;

	fn find_ring_buffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<RingBufferDef>>;

	fn get_ring_buffer(
		&mut self,
		id: RingBufferId,
	) -> crate::Result<RingBufferDef>;

	fn get_ring_buffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<RingBufferDef>;
}

impl<QT: QueryTransaction + MaterializedCatalogTransaction>
	CatalogRingBufferQueryOperations for QT
{
	fn find_ring_buffer(
		&mut self,
		id: RingBufferId,
	) -> crate::Result<Option<RingBufferDef>> {
		CatalogStore::find_ring_buffer(self, id)
	}

	fn find_ring_buffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<Option<RingBufferDef>> {
		let name = name.into_fragment();
		CatalogStore::find_ring_buffer_by_name(
			self,
			namespace,
			name.text(),
		)
	}

	fn get_ring_buffer(
		&mut self,
		id: RingBufferId,
	) -> crate::Result<RingBufferDef> {
		CatalogStore::get_ring_buffer(self, id)
	}

	fn get_ring_buffer_by_name<'a>(
		&mut self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> crate::Result<RingBufferDef> {
		let name = name.into_fragment();
		let name_text = name.text().to_string();
		let ring_buffer =
			self.find_ring_buffer_by_name(namespace, name.clone())?;
		match ring_buffer {
			Some(rb) => Ok(rb),
			None => {
				let namespace = CatalogStore::get_namespace(
					self, namespace,
				)?;
				return_error!(ring_buffer_not_found(
					name,
					&namespace.name,
					&name_text
				))
			}
		}
	}
}

pub trait CatalogTrackRingBufferChangeOperations {
	// Ring buffer tracking methods
	fn track_ring_buffer_def_created(
		&mut self,
		ring_buffer: RingBufferDef,
	) -> crate::Result<()>;

	fn track_ring_buffer_def_updated(
		&mut self,
		pre: RingBufferDef,
		post: RingBufferDef,
	) -> crate::Result<()>;

	fn track_ring_buffer_def_deleted(
		&mut self,
		ring_buffer: RingBufferDef,
	) -> crate::Result<()>;
}

pub trait CatalogRingBufferCommandOperations:
	CatalogRingBufferQueryOperations
{
	fn create_ring_buffer(
		&mut self,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef>;
}

impl<
	CT: CommandTransaction
		+ MaterializedCatalogTransaction
		+ CatalogTrackRingBufferChangeOperations
		+ WithInterceptors<CT>
		+ TransactionalChanges,
> CatalogRingBufferCommandOperations for CT
{
	fn create_ring_buffer(
		&mut self,
		to_create: RingBufferToCreate,
	) -> crate::Result<RingBufferDef> {
		if let Some(_ring_buffer) = self.find_ring_buffer_by_name(
			to_create.namespace,
			&to_create.ring_buffer,
		)? {
			let namespace = CatalogStore::get_namespace(
				self,
				to_create.namespace,
			)?;
			return_error!(ring_buffer_already_exists(
				to_create.fragment.unwrap_or_default(),
				&namespace.name,
				&to_create.ring_buffer
			));
		}

		CatalogStore::create_ring_buffer(self, to_create)
	}
}
