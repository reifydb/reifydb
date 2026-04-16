// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	binding::Binding,
	change::CatalogTrackBindingChangeOperations,
	id::{BindingId, ProcedureId},
};
use reifydb_transaction::{
	change::TransactionalBindingChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog, store::binding::create::BindingToCreate};

impl Catalog {
	#[instrument(name = "catalog::binding::find", level = "trace", skip(self, txn))]
	pub fn find_binding(&self, txn: &mut Transaction<'_>, id: BindingId) -> Result<Option<Binding>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(binding) = self.materialized.find_binding_at(id, cmd.version()) {
					return Ok(Some(binding));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(binding) = TransactionalBindingChanges::find_binding(admin, id) {
					return Ok(Some(binding.clone()));
				}
				if TransactionalBindingChanges::is_binding_deleted(admin, id) {
					return Ok(None);
				}
				if let Some(binding) = self.materialized.find_binding_at(id, admin.version()) {
					return Ok(Some(binding));
				}
				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(binding) = self.materialized.find_binding_at(id, qry.version()) {
					return Ok(Some(binding));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(binding) = TransactionalBindingChanges::find_binding(t.inner, id) {
					return Ok(Some(binding.clone()));
				}
				if TransactionalBindingChanges::is_binding_deleted(t.inner, id) {
					return Ok(None);
				}
				if let Some(binding) = self.materialized.find_binding_at(id, t.inner.version()) {
					return Ok(Some(binding));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(binding) = self.materialized.find_binding_at(id, rep.version()) {
					return Ok(Some(binding));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::binding::list_for_procedure", level = "trace", skip(self, txn))]
	pub fn list_bindings_for_procedure(
		&self,
		txn: &mut Transaction<'_>,
		procedure_id: ProcedureId,
	) -> Result<Vec<Binding>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_bindings_for_procedure_at(procedure_id, cmd.version()))
			}
			Transaction::Admin(admin) => {
				let mut bindings =
					self.materialized.list_bindings_for_procedure_at(procedure_id, admin.version());

				for change in &admin.changes.binding {
					if let Some(b) = &change.post
						&& b.procedure_id == procedure_id && !bindings
						.iter()
						.any(|existing| existing.id == b.id)
					{
						bindings.push(b.clone());
					}
				}

				Ok(bindings)
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_bindings_for_procedure_at(procedure_id, qry.version()))
			}
			Transaction::Test(t) => {
				let mut bindings = self
					.materialized
					.list_bindings_for_procedure_at(procedure_id, t.inner.version());

				for change in &t.inner.changes.binding {
					if let Some(b) = &change.post
						&& b.procedure_id == procedure_id && !bindings
						.iter()
						.any(|existing| existing.id == b.id)
					{
						bindings.push(b.clone());
					}
				}

				Ok(bindings)
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_bindings_for_procedure_at(procedure_id, rep.version()))
			}
		}
	}

	#[instrument(name = "catalog::binding::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_binding(&self, txn: &mut AdminTransaction, to_create: BindingToCreate) -> Result<Binding> {
		let binding = CatalogStore::create_binding(txn, to_create)?;
		txn.track_binding_created(binding.clone())?;
		Ok(binding)
	}

	#[instrument(name = "catalog::binding::drop", level = "debug", skip(self, txn))]
	pub fn drop_binding(&self, txn: &mut AdminTransaction, id: BindingId) -> Result<()> {
		let pre = CatalogStore::find_binding(&mut Transaction::Admin(&mut *txn), id)?;
		if let Some(pre) = pre {
			CatalogStore::drop_binding(txn, id)?;
			txn.track_binding_deleted(pre)?;
		}
		Ok(())
	}
}
