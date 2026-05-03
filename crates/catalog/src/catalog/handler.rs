// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackHandlerChangeOperations,
	handler::Handler,
	id::{HandlerId, NamespaceId},
};
use reifydb_transaction::{
	change::TransactionalHandlerChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{fragment::Fragment, value::sumtype::VariantRef};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog, store::handler::create::HandlerToCreate as StoreHandlerToCreate};

#[derive(Debug, Clone)]
pub struct HandlerToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub variant: VariantRef,
	pub body_source: String,
}

impl Catalog {
	#[instrument(name = "catalog::handler::find_by_id", level = "trace", skip(self, txn))]
	pub fn find_handler_by_id(&self, txn: &mut Transaction<'_>, id: HandlerId) -> Result<Option<Handler>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(handler) = self.materialized.find_handler_at(id, cmd.version()) {
					return Ok(Some(handler));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(handler) = TransactionalHandlerChanges::find_handler_by_id(admin, id) {
					return Ok(Some(handler.clone()));
				}

				if TransactionalHandlerChanges::is_handler_deleted(admin, id) {
					return Ok(None);
				}

				if let Some(handler) = self.materialized.find_handler_at(id, admin.version()) {
					return Ok(Some(handler));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(handler) = self.materialized.find_handler_at(id, qry.version()) {
					return Ok(Some(handler));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(handler) = TransactionalHandlerChanges::find_handler_by_id(t.inner, id) {
					return Ok(Some(handler.clone()));
				}

				if TransactionalHandlerChanges::is_handler_deleted(t.inner, id) {
					return Ok(None);
				}

				if let Some(handler) = self.materialized.find_handler_at(id, t.inner.version()) {
					return Ok(Some(handler));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(handler) = self.materialized.find_handler_at(id, rep.version()) {
					return Ok(Some(handler));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::handler::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_handler_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Handler>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(handler) =
					self.materialized.find_handler_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(handler));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(handler) =
					TransactionalHandlerChanges::find_handler_by_name(admin, namespace, name)
				{
					return Ok(Some(handler.clone()));
				}

				if TransactionalHandlerChanges::is_handler_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				if let Some(handler) =
					self.materialized.find_handler_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(handler));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(handler) =
					self.materialized.find_handler_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(handler));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(handler) =
					TransactionalHandlerChanges::find_handler_by_name(t.inner, namespace, name)
				{
					return Ok(Some(handler.clone()));
				}

				if TransactionalHandlerChanges::is_handler_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}

				if let Some(handler) =
					self.materialized.find_handler_by_name_at(namespace, name, t.inner.version())
				{
					return Ok(Some(handler));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(handler) =
					self.materialized.find_handler_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(handler));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::handler::list_for_variant", level = "trace", skip(self, txn))]
	pub fn list_handlers_for_variant(
		&self,
		txn: &mut Transaction<'_>,
		variant: VariantRef,
	) -> Result<Vec<Handler>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_handlers_for_variant_at(variant, cmd.version()))
			}
			Transaction::Admin(admin) => {
				let mut handlers =
					self.materialized.list_handlers_for_variant_at(variant, admin.version());

				for change in &admin.changes.handler {
					if let Some(h) = &change.post
						&& h.variant == variant && !handlers
						.iter()
						.any(|existing| existing.id == h.id)
					{
						handlers.push(h.clone());
					}
				}

				Ok(handlers)
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_handlers_for_variant_at(variant, qry.version()))
			}
			Transaction::Test(t) => {
				let mut handlers =
					self.materialized.list_handlers_for_variant_at(variant, t.inner.version());

				for change in &t.inner.changes.handler {
					if let Some(h) = &change.post
						&& h.variant == variant && !handlers
						.iter()
						.any(|existing| existing.id == h.id)
					{
						handlers.push(h.clone());
					}
				}

				Ok(handlers)
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_handlers_for_variant_at(variant, rep.version()))
			}
		}
	}

	#[instrument(name = "catalog::handler::drop", level = "debug", skip(self, txn))]
	pub fn drop_handler(&self, txn: &mut AdminTransaction, id: HandlerId) -> Result<()> {
		if let Some(handler) = self.find_handler_by_id(&mut Transaction::Admin(&mut *txn), id)? {
			CatalogStore::drop_handler(txn, id)?;
			txn.track_handler_deleted(handler)?;
		}
		Ok(())
	}

	#[instrument(name = "catalog::handler::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_handler(&self, txn: &mut AdminTransaction, to_create: HandlerToCreate) -> Result<Handler> {
		let store_to_create = StoreHandlerToCreate {
			name: to_create.name.clone(),
			namespace: to_create.namespace,
			variant: to_create.variant,
			body_source: to_create.body_source.clone(),
		};

		let handler = CatalogStore::create_handler(txn, store_to_create)?;

		txn.track_handler_created(handler.clone())?;

		Ok(handler)
	}
}
