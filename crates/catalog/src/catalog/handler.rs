// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackHandlerChangeOperations,
	handler::HandlerDef,
	id::{HandlerId, NamespaceId},
};
use reifydb_transaction::{
	change::TransactionalHandlerChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{fragment::Fragment, value::sumtype::SumTypeId};
use tracing::instrument;

use crate::{CatalogStore, catalog::Catalog, store::handler::create::HandlerToCreate as StoreHandlerToCreate};

/// Handler creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct HandlerToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub on_sumtype_id: SumTypeId,
	pub on_variant_tag: u8,
	pub body_source: String,
}

impl Catalog {
	#[instrument(name = "catalog::handler::find_by_id", level = "trace", skip(self, txn))]
	pub fn find_handler_by_id(
		&self,
		txn: &mut Transaction<'_>,
		id: HandlerId,
	) -> crate::Result<Option<HandlerDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(handler) = self.materialized.find_handler_at(id, cmd.version()) {
					return Ok(Some(handler));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(handler) = TransactionalHandlerChanges::find_handler_by_id(admin, id) {
					return Ok(Some(handler.clone()));
				}

				// 2. Check MaterializedCatalog
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
		}
	}

	#[instrument(name = "catalog::handler::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_handler_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<HandlerDef>> {
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
				// 1. Check transactional changes first
				if let Some(handler) =
					TransactionalHandlerChanges::find_handler_by_name(admin, namespace, name)
				{
					return Ok(Some(handler.clone()));
				}

				// 2. Check if deleted
				if TransactionalHandlerChanges::is_handler_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
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
		}
	}

	#[instrument(name = "catalog::handler::list_for_variant", level = "trace", skip(self, txn))]
	pub fn list_handlers_for_variant(
		&self,
		txn: &mut Transaction<'_>,
		sumtype_id: SumTypeId,
		variant_tag: u8,
	) -> crate::Result<Vec<HandlerDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self.materialized.list_handlers_for_variant_at(
				sumtype_id,
				variant_tag,
				cmd.version(),
			)),
			Transaction::Admin(admin) => {
				// Check materialized catalog + transactional additions
				let mut handlers = self.materialized.list_handlers_for_variant_at(
					sumtype_id,
					variant_tag,
					admin.version(),
				);

				// Also check transactional changes for newly created handlers
				for change in &admin.changes.handler_def {
					if let Some(h) = &change.post {
						if h.on_sumtype_id == sumtype_id
							&& h.on_variant_tag == variant_tag && !handlers
							.iter()
							.any(|existing| existing.id == h.id)
						{
							handlers.push(h.clone());
						}
					}
				}

				Ok(handlers)
			}
			Transaction::Query(qry) => Ok(self.materialized.list_handlers_for_variant_at(
				sumtype_id,
				variant_tag,
				qry.version(),
			)),
		}
	}

	#[instrument(name = "catalog::handler::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_handler(
		&self,
		txn: &mut AdminTransaction,
		to_create: HandlerToCreate,
	) -> crate::Result<HandlerDef> {
		let store_to_create = StoreHandlerToCreate {
			name: to_create.name.clone(),
			namespace: to_create.namespace,
			on_sumtype_id: to_create.on_sumtype_id,
			on_variant_tag: to_create.on_variant_tag,
			body_source: to_create.body_source.clone(),
		};

		let handler = CatalogStore::create_handler(txn, store_to_create)?;

		txn.track_handler_def_created(handler.clone())?;

		Ok(handler)
	}
}
