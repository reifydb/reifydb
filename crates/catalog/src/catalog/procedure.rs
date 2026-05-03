// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		change::CatalogTrackProcedureChangeOperations,
		id::{NamespaceId, ProcedureId},
		procedure::{Procedure, ProcedureParam, RqlTrigger},
	},
};
use reifydb_transaction::{
	change::TransactionalProcedureChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, sumtype::VariantRef},
};
use tracing::instrument;

use crate::{CatalogStore, Result, catalog::Catalog, error::CatalogError, materialized::MaterializedCatalog};

#[derive(Debug, Clone)]
pub enum ResolvedProcedure {
	Local(Procedure),
	Remote {
		address: String,
		token: Option<String>,
	},

	Test(Procedure),
}

#[derive(Debug, Clone)]
pub enum ProcedureToCreate {
	Rql {
		name: Fragment,
		namespace: NamespaceId,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		body: String,
		trigger: RqlTrigger,
	},
	Test {
		name: Fragment,
		namespace: NamespaceId,
		params: Vec<ProcedureParam>,
		return_type: Option<TypeConstraint>,
		body: String,
	},
}

impl ProcedureToCreate {
	pub fn name(&self) -> &Fragment {
		match self {
			ProcedureToCreate::Rql {
				name,
				..
			}
			| ProcedureToCreate::Test {
				name,
				..
			} => name,
		}
	}

	pub fn namespace(&self) -> NamespaceId {
		match self {
			ProcedureToCreate::Rql {
				namespace,
				..
			}
			| ProcedureToCreate::Test {
				namespace,
				..
			} => *namespace,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::procedure::find", level = "trace", skip(self, txn))]
	pub fn find_procedure(&self, txn: &mut Transaction<'_>, id: ProcedureId) -> Result<Option<Procedure>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				if let Some(procedure) = self.materialized.find_procedure_at(id, cmd.version()) {
					return Ok(Some(procedure));
				}
				Ok(None)
			}
			Transaction::Admin(admin) => {
				if let Some(procedure) = TransactionalProcedureChanges::find_procedure(admin, id) {
					return Ok(Some(procedure.clone()));
				}

				if TransactionalProcedureChanges::is_procedure_deleted(admin, id) {
					return Ok(None);
				}

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
			Transaction::Test(t) => {
				if let Some(procedure) = TransactionalProcedureChanges::find_procedure(t.inner, id) {
					return Ok(Some(procedure.clone()));
				}

				if TransactionalProcedureChanges::is_procedure_deleted(t.inner, id) {
					return Ok(None);
				}

				if let Some(procedure) = self.materialized.find_procedure_at(id, t.inner.version()) {
					return Ok(Some(procedure));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(procedure) = self.materialized.find_procedure_at(id, rep.version()) {
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
	) -> Result<Option<Procedure>> {
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
				if let Some(procedure) =
					TransactionalProcedureChanges::find_procedure_by_name(admin, namespace, name)
				{
					return Ok(Some(procedure.clone()));
				}

				if TransactionalProcedureChanges::is_procedure_deleted_by_name(admin, namespace, name) {
					return Ok(None);
				}

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
			Transaction::Test(t) => {
				if let Some(procedure) =
					TransactionalProcedureChanges::find_procedure_by_name(t.inner, namespace, name)
				{
					return Ok(Some(procedure.clone()));
				}

				if TransactionalProcedureChanges::is_procedure_deleted_by_name(t.inner, namespace, name)
				{
					return Ok(None);
				}

				if let Some(procedure) =
					self.materialized.find_procedure_by_name_at(namespace, name, t.inner.version())
				{
					return Ok(Some(procedure));
				}

				Ok(None)
			}
			Transaction::Replica(rep) => {
				if let Some(procedure) =
					self.materialized.find_procedure_by_name_at(namespace, name, rep.version())
				{
					return Ok(Some(procedure));
				}
				Ok(None)
			}
		}
	}

	pub fn split_qualified_name(qualified_name: &str) -> Option<(String, &str)> {
		qualified_name.rsplit_once("::").map(|(ns_part, entity_name)| (ns_part.to_string(), entity_name))
	}

	#[instrument(name = "catalog::procedure::find_by_qualified_name", level = "trace", skip(self, txn))]
	pub fn find_procedure_by_qualified_name(
		&self,
		txn: &mut Transaction<'_>,
		qualified_name: &str,
	) -> Result<Option<ResolvedProcedure>> {
		if let Some((ns_name, proc_name)) = Self::split_qualified_name(qualified_name) {
			if let Some(ns) = self.find_namespace_by_path(txn, &ns_name)? {
				if let Some(address) = ns.address() {
					return Ok(Some(ResolvedProcedure::Remote {
						address: address.to_string(),
						token: ns.token().map(|t| t.to_string()),
					}));
				}
				return Ok(self.find_procedure_by_name(txn, ns.id(), proc_name)?.map(|p| {
					if matches!(p, Procedure::Test { .. }) {
						ResolvedProcedure::Test(p)
					} else {
						ResolvedProcedure::Local(p)
					}
				}));
			}
			Ok(None)
		} else {
			Ok(self.find_procedure_by_name(txn, NamespaceId::DEFAULT, qualified_name)?.map(|p| {
				if matches!(p, Procedure::Test { .. }) {
					ResolvedProcedure::Test(p)
				} else {
					ResolvedProcedure::Local(p)
				}
			}))
		}
	}

	#[instrument(name = "catalog::procedure::list_for_variant", level = "trace", skip(self, txn))]
	pub fn list_procedures_for_variant(
		&self,
		txn: &mut Transaction<'_>,
		variant: VariantRef,
	) -> Result<Vec<Procedure>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				Ok(self.materialized.list_procedures_for_variant_at(variant, cmd.version()))
			}
			Transaction::Admin(admin) => {
				let mut procedures =
					self.materialized.list_procedures_for_variant_at(variant, admin.version());

				for change in &admin.changes.procedure {
					if let Some(p) = &change.post
						&& p.event_variant() == Some(variant) && !procedures
						.iter()
						.any(|existing| existing.id() == p.id())
					{
						procedures.push(p.clone());
					}
				}

				procedures.retain(|p| !admin.is_procedure_deleted(p.id()));

				Ok(procedures)
			}
			Transaction::Query(qry) => {
				Ok(self.materialized.list_procedures_for_variant_at(variant, qry.version()))
			}
			Transaction::Test(t) => {
				let mut procedures =
					self.materialized.list_procedures_for_variant_at(variant, t.inner.version());

				for change in &t.inner.changes.procedure {
					if let Some(p) = &change.post
						&& p.event_variant() == Some(variant) && !procedures
						.iter()
						.any(|existing| existing.id() == p.id())
					{
						procedures.push(p.clone());
					}
				}

				procedures.retain(|p| !t.inner.is_procedure_deleted(p.id()));

				Ok(procedures)
			}
			Transaction::Replica(rep) => {
				Ok(self.materialized.list_procedures_for_variant_at(variant, rep.version()))
			}
		}
	}

	#[instrument(name = "catalog::procedure::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_procedure(&self, txn: &mut AdminTransaction, to_create: ProcedureToCreate) -> Result<Procedure> {
		let procedure = CatalogStore::create_procedure(txn, to_create)?;
		txn.track_procedure_created(procedure.clone())?;
		Ok(procedure)
	}

	pub fn create_procedure_with_id(
		&self,
		txn: &mut AdminTransaction,
		id: ProcedureId,
		to_create: ProcedureToCreate,
	) -> Result<Procedure> {
		let procedure = CatalogStore::create_procedure_with_id(txn, id, to_create)?;
		txn.track_procedure_created(procedure.clone())?;
		Ok(procedure)
	}

	#[instrument(name = "catalog::procedure::drop", level = "debug", skip(self, txn))]
	pub fn drop_procedure(&self, txn: &mut AdminTransaction, id: ProcedureId) -> Result<()> {
		let pre = CatalogStore::get_procedure(&mut Transaction::Admin(&mut *txn), id)?;
		if !pre.is_persistent() {
			return Err(CatalogError::CannotDropEphemeralProcedure {
				kind: pre.kind().as_str().to_string(),
				name: pre.name().to_string(),
				fragment: Fragment::internal(pre.name()),
			}
			.into());
		}
		CatalogStore::drop_procedure(txn, id)?;
		txn.track_procedure_deleted(pre)?;
		Ok(())
	}

	pub fn register_ephemeral_procedure(
		catalog: &MaterializedCatalog,
		version: CommitVersion,
		procedure: Procedure,
	) -> Result<()> {
		if procedure.is_persistent() {
			return Err(CatalogError::CannotRegisterPersistentAsEphemeral {
				kind: procedure.kind().as_str().to_string(),
			}
			.into());
		}
		catalog.set_procedure(procedure.id(), version, Some(procedure));
		Ok(())
	}
}
