// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackProcedureChangeOperations,
	id::{NamespaceId, ProcedureId},
	procedure::{ProcedureDef, ProcedureParamDef},
};
use reifydb_transaction::{
	change::TransactionalProcedureChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{fragment::Fragment, value::constraint::TypeConstraint};
use tracing::instrument;

use crate::catalog::Catalog;

/// Procedure creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct ProcedureToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub params: Vec<ProcedureParamDef>,
	pub return_type: Option<TypeConstraint>,
	pub body: String,
}

impl Catalog {
	#[instrument(name = "catalog::procedure::find", level = "trace", skip(self, txn))]
	pub fn find_procedure(
		&self,
		txn: &mut Transaction<'_>,
		id: ProcedureId,
	) -> crate::Result<Option<ProcedureDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(procedure) = self.materialized.find_procedure_at(id, cmd.version()) {
					return Ok(Some(procedure));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(procedure) = TransactionalProcedureChanges::find_procedure(admin, id) {
					return Ok(Some(procedure.clone()));
				}

				// 2. Check if deleted
				if TransactionalProcedureChanges::is_procedure_deleted(admin, id) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(procedure) = self.materialized.find_procedure_at(id, admin.version()) {
					return Ok(Some(procedure));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(procedure) = self.materialized.find_procedure_at(id, qry.version()) {
					return Ok(Some(procedure));
				}
				Ok(None)
			}
		}
	}

	#[instrument(name = "catalog::procedure::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_procedure_by_name(
		&self,
		txn: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<ProcedureDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(procedure) =
					self.materialized.find_procedure_by_name_at(namespace, name, cmd.version())
				{
					return Ok(Some(procedure));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check transactional changes first
				if let Some(procedure) =
					TransactionalProcedureChanges::find_procedure_by_name(admin, namespace, name)
				{
					return Ok(Some(procedure.clone()));
				}

				// 2. Check if deleted
				if TransactionalProcedureChanges::is_procedure_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

				// 3. Check MaterializedCatalog
				if let Some(procedure) =
					self.materialized.find_procedure_by_name_at(namespace, name, admin.version())
				{
					return Ok(Some(procedure));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				if let Some(procedure) =
					self.materialized.find_procedure_by_name_at(namespace, name, qry.version())
				{
					return Ok(Some(procedure));
				}
				Ok(None)
			}
		}
	}

	/// Convenience: splits "ns.name" into namespace + name, resolves namespace, then calls find_procedure_by_name
	#[instrument(name = "catalog::procedure::find_by_qualified_name", level = "trace", skip(self, txn))]
	pub fn find_procedure_by_qualified_name(
		&self,
		txn: &mut Transaction<'_>,
		qualified_name: &str,
	) -> crate::Result<Option<ProcedureDef>> {
		if let Some((ns_name, proc_name)) = qualified_name.split_once('.') {
			if let Some(ns) = self.find_namespace_by_name(txn, ns_name)? {
				return self.find_procedure_by_name(txn, ns.id, proc_name);
			}
			Ok(None)
		} else {
			// No namespace qualifier — search in default namespace
			let default_ns = NamespaceId(2); // default namespace ID
			self.find_procedure_by_name(txn, default_ns, qualified_name)
		}
	}

	#[instrument(name = "catalog::procedure::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_procedure(
		&self,
		txn: &mut AdminTransaction,
		to_create: ProcedureToCreate,
	) -> crate::Result<ProcedureDef> {
		// Generate a new procedure ID from the current transaction version
		let id = ProcedureId(txn.version().0 + 1);

		let procedure = ProcedureDef {
			id,
			namespace: to_create.namespace,
			name: to_create.name.text().to_string(),
			params: to_create.params,
			return_type: to_create.return_type,
			body: to_create.body,
		};

		txn.track_procedure_def_created(procedure.clone())?;

		Ok(procedure)
	}
}
