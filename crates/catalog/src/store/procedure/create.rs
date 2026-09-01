// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{Procedure, ProcedureParam, RqlTrigger},
	},
	key::{
		namespace::NamespaceProcedureKey,
		procedure::{ProcedureKey, ProcedureParamKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::constraint::TypeConstraint;
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	catalog::procedure::ProcedureToCreate,
	error::{CatalogError, CatalogObjectKind},
	store::{
		procedure::shape::{
			TRIGGER_CALL, TRIGGER_EVENT, VARIANT_RQL, VARIANT_TEST, namespace_procedure, procedure,
			procedure_param,
		},
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
					VARIANT_RQL,
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
					VARIANT_TEST,
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
	let mut row = procedure::allocate();
	procedure::set_id(&mut row, u64::from(id));
	procedure::set_namespace(&mut row, u64::from(namespace));
	procedure::set_name(&mut row, name);
	procedure::set_variant(&mut row, variant);
	procedure::set_body(&mut row, body);

	let (trigger_kind, sumtype, vidx) = match trigger {
		RqlTrigger::Call => (TRIGGER_CALL, 0u64, 0u16),
		RqlTrigger::Event {
			variant: v,
		} => (TRIGGER_EVENT, v.sumtype_id.0, v.variant_tag as u16),
	};
	procedure::set_trigger_kind(&mut row, trigger_kind);
	procedure::set_trigger_variant_sumtype(&mut row, sumtype);
	procedure::set_trigger_variant_index(&mut row, vidx);

	let return_type_json = match return_type {
		Some(rt) => to_string(rt).expect("TypeConstraint serializes"),
		None => String::new(),
	};
	procedure::set_return_type(&mut row, &return_type_json);

	txn.set(&ProcedureKey::encoded(id), row.freeze())?;
	Ok(())
}

fn link_procedure_to_namespace(
	txn: &mut AdminTransaction,
	namespace: NamespaceId,
	procedure: ProcedureId,
	name: &str,
) -> Result<()> {
	let mut row = namespace_procedure::allocate();
	namespace_procedure::set_id(&mut row, u64::from(procedure));
	namespace_procedure::set_name(&mut row, name);
	txn.set(&NamespaceProcedureKey::encoded(namespace, procedure), row.freeze())?;
	Ok(())
}

fn insert_params(txn: &mut AdminTransaction, procedure: ProcedureId, params: &[ProcedureParam]) -> Result<()> {
	for (index, param) in params.iter().enumerate() {
		let mut row = procedure_param::allocate();
		procedure_param::set_procedure_id(&mut row, u64::from(procedure));
		procedure_param::set_index(&mut row, index as u16);
		procedure_param::set_name(&mut row, &param.name);
		let json = to_string(&param.param_type).expect("TypeConstraint serializes");
		procedure_param::set_type_constraint(&mut row, &json);
		txn.set(&ProcedureParamKey::encoded(procedure, index as u16), row.freeze())?;
	}
	Ok(())
}
