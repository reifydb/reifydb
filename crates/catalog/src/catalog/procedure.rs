// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackProcedureChangeOperations,
	id::{NamespaceId, ProcedureId},
	procedure::{ProcedureDef, ProcedureParamDef, ProcedureTrigger},
};
use reifydb_transaction::{
	change::TransactionalProcedureChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, sumtype::SumTypeId},
};
use tracing::instrument;

use crate::{catalog::Catalog, store::sequence::system::SystemSequence};

/// Procedure creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct ProcedureToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub params: Vec<ProcedureParamDef>,
	pub return_type: Option<TypeConstraint>,
	pub body: String,
	pub trigger: ProcedureTrigger,
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

	/// Splits a `::` qualified name (e.g., `"ns::proc"` or `"a::b::proc"`) into (namespace_name, entity_name).
	/// The namespace part converts `::` to `.` to match stored namespace names.
	/// Returns `None` if there's no `::` separator (unqualified name).
	pub fn split_qualified_name(qualified_name: &str) -> Option<(String, &str)> {
		qualified_name.rsplit_once("::").map(|(ns_part, entity_name)| (ns_part.replace("::", "."), entity_name))
	}

	/// Convenience: splits "ns::name" into namespace + name, resolves namespace, then calls find_procedure_by_name
	#[instrument(name = "catalog::procedure::find_by_qualified_name", level = "trace", skip(self, txn))]
	pub fn find_procedure_by_qualified_name(
		&self,
		txn: &mut Transaction<'_>,
		qualified_name: &str,
	) -> crate::Result<Option<ProcedureDef>> {
		if let Some((ns_name, proc_name)) = Self::split_qualified_name(qualified_name) {
			if let Some(ns) = self.find_namespace_by_name(txn, &ns_name)? {
				return self.find_procedure_by_name(txn, ns.id, proc_name);
			}
			Ok(None)
		} else {
			// No namespace qualifier — search in default namespace
			let default_ns = NamespaceId(2); // default namespace ID
			self.find_procedure_by_name(txn, default_ns, qualified_name)
		}
	}

	#[instrument(name = "catalog::procedure::list_for_variant", level = "trace", skip(self, txn))]
	pub fn list_procedures_for_variant(
		&self,
		txn: &mut Transaction<'_>,
		sumtype_id: SumTypeId,
		variant_tag: u8,
	) -> crate::Result<Vec<ProcedureDef>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => Ok(self.materialized.list_procedures_for_variant_at(
				sumtype_id,
				variant_tag,
				cmd.version(),
			)),
			Transaction::Admin(admin) => {
				// Check materialized catalog + transactional additions
				let mut procedures = self.materialized.list_procedures_for_variant_at(
					sumtype_id,
					variant_tag,
					admin.version(),
				);

				// Also check transactional changes for newly created procedures with event binding
				for change in &admin.changes.procedure_def {
					if let Some(p) = &change.post {
						if let ProcedureTrigger::Event {
							sumtype_id: sid,
							variant_tag: vtag,
						} = &p.trigger
						{
							if *sid == sumtype_id
								&& *vtag == variant_tag && !procedures
								.iter()
								.any(|existing| existing.id == p.id)
							{
								procedures.push(p.clone());
							}
						}
					}
				}

				Ok(procedures)
			}
			Transaction::Query(qry) => Ok(self.materialized.list_procedures_for_variant_at(
				sumtype_id,
				variant_tag,
				qry.version(),
			)),
		}
	}

	#[instrument(name = "catalog::procedure::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_procedure(
		&self,
		txn: &mut AdminTransaction,
		to_create: ProcedureToCreate,
	) -> crate::Result<ProcedureDef> {
		let id = SystemSequence::next_procedure_id(txn)?;

		let procedure = ProcedureDef {
			id,
			namespace: to_create.namespace,
			name: to_create.name.text().to_string(),
			params: to_create.params,
			return_type: to_create.return_type,
			body: to_create.body,
			trigger: to_create.trigger,
		};

		txn.track_procedure_def_created(procedure.clone())?;

		Ok(procedure)
	}
}
