// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	binding::{Binding, BindingFormat, BindingProtocol},
	change::CatalogTrackBindingChangeOperations,
	id::{BindingId, NamespaceId, ProcedureId},
};
use reifydb_transaction::{
	change::TransactionalBindingChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
	store::binding::create::BindingToCreate as StoreBindingToCreate,
};

pub struct BindingToCreate {
	pub namespace: NamespaceId,
	pub name: String,
	pub procedure: ProcedureId,
	pub protocol: BindingProtocol,
	pub format: BindingFormat,
}

impl From<BindingToCreate> for StoreBindingToCreate {
	fn from(to_create: BindingToCreate) -> Self {
		StoreBindingToCreate {
			namespace: to_create.namespace,
			name: to_create.name,
			procedure: to_create.procedure,
			protocol: to_create.protocol,
			format: to_create.format,
		}
	}
}

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

	#[instrument(name = "catalog::binding::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_binding_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Binding>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(binding) =
					self.materialized.find_binding_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(binding));
				}
				CatalogStore::find_binding_by_name(
					&mut Transaction::Command(&mut *cmd),
					namespace,
					name,
				)
			}
			Transaction::Admin(admin) => {
				if let Some(binding) =
					TransactionalBindingChanges::find_binding_by_name(admin, namespace, name)
				{
					return Ok(Some(binding.clone()));
				}
				if TransactionalBindingChanges::is_binding_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}
				if let Some(binding) =
					self.materialized.find_binding_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(binding));
				}
				CatalogStore::find_binding_by_name(
					&mut Transaction::Admin(&mut *admin),
					namespace,
					name,
				)
			}
			Transaction::Query(qry) => {
				if let Some(binding) =
					self.materialized.find_binding_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(binding));
				}
				Ok(None)
			}
			Transaction::Test(t) => {
				if let Some(binding) =
					TransactionalBindingChanges::find_binding_by_name(t.inner, namespace, name)
				{
					return Ok(Some(binding.clone()));
				}
				if TransactionalBindingChanges::is_binding_deleted_by_name(t.inner, namespace, name) {
					return Ok(None);
				}
				if let Some(binding) =
					self.materialized.find_binding_by_name_at(namespace, name, t.inner.version())
				{
					return Ok(Some(binding));
				}
				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(binding) =
					self.materialized.find_binding_by_name_at(namespace, name, rep.version())
				{
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

				bindings.retain(|b| !admin.is_binding_deleted(b.id));
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

				bindings.retain(|b| !t.inner.is_binding_deleted(b.id));
				Ok(bindings)
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_bindings_for_procedure_at(procedure_id, rep.version()))
			}
		}
	}

	#[instrument(name = "catalog::binding::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_binding(&self, txn: &mut AdminTransaction, to_create: BindingToCreate) -> Result<Binding> {
		if let Some(existing) = self.materialized.find_binding_by_name(to_create.namespace, &to_create.name) {
			let _ = existing;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Binding,
				namespace: to_create.namespace.to_string(),
				name: to_create.name.clone(),
				fragment: Fragment::internal(to_create.name.clone()),
			}
			.into());
		}

		match &to_create.protocol {
			BindingProtocol::Http {
				method,
				path,
			} => {
				if self.materialized.find_http_binding_by_method_path(method.as_str(), path).is_some() {
					return Err(CatalogError::AlreadyExists {
						kind: CatalogObjectKind::Binding,
						namespace: to_create.namespace.to_string(),
						name: format!("{} {}", method.as_str(), path),
						fragment: Fragment::internal(format!("{} {}", method.as_str(), path)),
					}
					.into());
				}
			}
			BindingProtocol::Grpc {
				name,
			} => {
				if self.materialized.find_grpc_binding_by_name(name).is_some() {
					return Err(CatalogError::AlreadyExists {
						kind: CatalogObjectKind::Binding,
						namespace: to_create.namespace.to_string(),
						name: name.clone(),
						fragment: Fragment::internal(name.clone()),
					}
					.into());
				}
			}
			BindingProtocol::Ws {
				name,
			} => {
				if self.materialized.find_ws_binding_by_name(name).is_some() {
					return Err(CatalogError::AlreadyExists {
						kind: CatalogObjectKind::Binding,
						namespace: to_create.namespace.to_string(),
						name: name.clone(),
						fragment: Fragment::internal(name.clone()),
					}
					.into());
				}
			}
		}

		let binding = CatalogStore::create_binding(txn, to_create.into())?;
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
