// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{Procedure, ProcedureParam, RqlTrigger},
	},
	key::{
		namespace_procedure::NamespaceProcedureKey, procedure::ProcedureKey, procedure_param::ProcedureParamKey,
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::constraint::TypeConstraint;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	catalog::procedure::ProcedureToCreate,
	error::{CatalogError, CatalogObjectKind},
	store::{
		procedure::shape::{namespace_procedure, procedure, procedure_param},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_procedure(txn: &mut AdminTransaction, to_create: ProcedureToCreate) -> Result<Procedure> {
		let namespace_id = to_create.namespace();
		let name = to_create.name().clone();
		let name_text = name.text().to_string();

		if let Some(existing) =
			Self::find_procedure_by_name(&mut Transaction::Admin(&mut *txn), namespace_id, &name_text)?
		{
			let namespace = Self::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			let kind = if matches!(existing, Procedure::Test { .. }) {
				CatalogObjectKind::TestProcedure
			} else {
				CatalogObjectKind::Procedure
			};
			return Err(CatalogError::AlreadyExists {
				kind,
				namespace: namespace.name().to_string(),
				name: name_text,
				fragment: name,
			}
			.into());
		}

		let id = SystemSequence::next_procedure_id(txn)?;
		Self::create_procedure_with_id(txn, id, to_create)
	}

	pub(crate) fn create_procedure_with_id(
		txn: &mut AdminTransaction,
		id: ProcedureId,
		to_create: ProcedureToCreate,
	) -> Result<Procedure> {
		match to_create {
			ProcedureToCreate::Rql {
				name,
				namespace,
				params,
				return_type,
				body,
				trigger,
			} => {
				let name_text = name.text().to_string();
				store_procedure_row(
					txn,
					id,
					namespace,
					&name_text,
					procedure::VARIANT_RQL,
					&body,
					&trigger,
					&return_type,
				)?;
				link_procedure_to_namespace(txn, namespace, id, &name_text)?;
				insert_params(txn, id, &params)?;
				Ok(Procedure::Rql {
					id,
					namespace,
					name: name_text,
					params,
					return_type,
					body,
					trigger,
				})
			}
			ProcedureToCreate::Test {
				name,
				namespace,
				params,
				return_type,
				body,
			} => {
				let name_text = name.text().to_string();
				store_procedure_row(
					txn,
					id,
					namespace,
					&name_text,
					procedure::VARIANT_TEST,
					&body,
					&RqlTrigger::Call,
					&return_type,
				)?;
				link_procedure_to_namespace(txn, namespace, id, &name_text)?;
				insert_params(txn, id, &params)?;
				Ok(Procedure::Test {
					id,
					namespace,
					name: name_text,
					params,
					return_type,
					body,
				})
			}
		}
	}
}

#[allow(clippy::too_many_arguments)]
fn store_procedure_row(
	txn: &mut AdminTransaction,
	id: ProcedureId,
	namespace: NamespaceId,
	name: &str,
	variant: u8,
	body: &str,
	trigger: &RqlTrigger,
	return_type: &Option<TypeConstraint>,
) -> Result<()> {
	let mut row = procedure::SHAPE.allocate();
	procedure::SHAPE.set_u64(&mut row, procedure::ID, id);
	procedure::SHAPE.set_u64(&mut row, procedure::NAMESPACE, namespace);
	procedure::SHAPE.set_utf8(&mut row, procedure::NAME, name);
	procedure::SHAPE.set_u8(&mut row, procedure::VARIANT, variant);
	procedure::SHAPE.set_utf8(&mut row, procedure::BODY, body);

	let (trigger_kind, sumtype, vidx) = match trigger {
		RqlTrigger::Call => (procedure::TRIGGER_CALL, 0u64, 0u16),
		RqlTrigger::Event {
			variant: v,
		} => (procedure::TRIGGER_EVENT, v.sumtype_id.0, v.variant_tag as u16),
	};
	procedure::SHAPE.set_u8(&mut row, procedure::TRIGGER_KIND, trigger_kind);
	procedure::SHAPE.set_u64(&mut row, procedure::TRIGGER_VARIANT_SUMTYPE, sumtype);
	procedure::SHAPE.set_u16(&mut row, procedure::TRIGGER_VARIANT_INDEX, vidx);

	let return_type_json = match return_type {
		Some(rt) => to_string(rt).expect("TypeConstraint serializes"),
		None => String::new(),
	};
	procedure::SHAPE.set_utf8(&mut row, procedure::RETURN_TYPE, &return_type_json);

	txn.set(&ProcedureKey::encoded(id), row)?;
	Ok(())
}

fn link_procedure_to_namespace(
	txn: &mut AdminTransaction,
	namespace: NamespaceId,
	procedure: ProcedureId,
	name: &str,
) -> Result<()> {
	let mut row = namespace_procedure::SHAPE.allocate();
	namespace_procedure::SHAPE.set_u64(&mut row, namespace_procedure::ID, procedure);
	namespace_procedure::SHAPE.set_utf8(&mut row, namespace_procedure::NAME, name);
	txn.set(&NamespaceProcedureKey::encoded(namespace, procedure), row)?;
	Ok(())
}

fn insert_params(txn: &mut AdminTransaction, procedure: ProcedureId, params: &[ProcedureParam]) -> Result<()> {
	for (index, param) in params.iter().enumerate() {
		let mut row = procedure_param::SHAPE.allocate();
		procedure_param::SHAPE.set_u64(&mut row, procedure_param::PROCEDURE_ID, procedure);
		procedure_param::SHAPE.set_u16(&mut row, procedure_param::INDEX, index as u16);
		procedure_param::SHAPE.set_utf8(&mut row, procedure_param::NAME, &param.name);
		let json = to_string(&param.param_type).expect("TypeConstraint serializes");
		procedure_param::SHAPE.set_utf8(&mut row, procedure_param::TYPE_CONSTRAINT, &json);
		txn.set(&ProcedureParamKey::encoded(procedure, index as u16), row)?;
	}
	Ok(())
}
