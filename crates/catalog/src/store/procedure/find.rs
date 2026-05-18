// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{Procedure, ProcedureParam, RqlTrigger},
	},
	key::{
		namespace_procedure::NamespaceProcedureKey, procedure::ProcedureKey, procedure_param::ProcedureParamKey,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{
	constraint::TypeConstraint,
	sumtype::{SumTypeId, VariantRef},
};
use serde_json::from_str;

use crate::{
	CatalogStore, Result,
	store::procedure::shape::{namespace_procedure, procedure, procedure_param},
};

impl CatalogStore {
	pub(crate) fn find_procedure(rx: &mut Transaction<'_>, id: ProcedureId) -> Result<Option<Procedure>> {
		let Some(multi) = rx.get(&ProcedureKey::encoded(id))? else {
			return Ok(None);
		};
		let params = load_params(rx, id)?;
		Ok(Some(decode_procedure(&multi.row, params)))
	}

	pub(crate) fn find_procedure_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Procedure>> {
		let mut found_id = None;
		let mut stream = rx.range(NamespaceProcedureKey::full_scan(namespace), 1024)?;
		for entry in stream.by_ref() {
			let multi = entry?;
			let row = &multi.row;
			let candidate = namespace_procedure::SHAPE.get_utf8(row, namespace_procedure::NAME);
			if candidate == name {
				found_id = Some(ProcedureId::from_raw(
					namespace_procedure::SHAPE.get_u64(row, namespace_procedure::ID),
				));
				break;
			}
		}
		drop(stream);

		let Some(id) = found_id else {
			return Ok(None);
		};
		Self::find_procedure(rx, id)
	}
}

pub(crate) fn load_params(rx: &mut Transaction<'_>, procedure_id: ProcedureId) -> Result<Vec<ProcedureParam>> {
	let mut entries: Vec<(u16, ProcedureParam)> = Vec::new();
	let mut stream = rx.range(ProcedureParamKey::full_scan(procedure_id), 1024)?;
	for entry in stream.by_ref() {
		let multi = entry?;
		let row = &multi.row;
		let index = procedure_param::SHAPE.get_u16(row, procedure_param::INDEX);
		let name = procedure_param::SHAPE.get_utf8(row, procedure_param::NAME).to_string();
		let json = procedure_param::SHAPE.get_utf8(row, procedure_param::TYPE_CONSTRAINT);
		let param_type: TypeConstraint = from_str(json).expect("TypeConstraint deserializes from stored JSON");
		entries.push((
			index,
			ProcedureParam {
				name,
				param_type,
			},
		));
	}
	drop(stream);
	entries.sort_by_key(|(i, _)| *i);
	Ok(entries.into_iter().map(|(_, p)| p).collect())
}

pub(crate) fn decode_procedure(row: &EncodedRow, params: Vec<ProcedureParam>) -> Procedure {
	let id = ProcedureId::from_raw(procedure::SHAPE.get_u64(row, procedure::ID));
	let namespace = NamespaceId(procedure::SHAPE.get_u64(row, procedure::NAMESPACE));
	let name = procedure::SHAPE.get_utf8(row, procedure::NAME).to_string();
	let variant = procedure::SHAPE.get_u8(row, procedure::VARIANT);
	let body = procedure::SHAPE.get_utf8(row, procedure::BODY).to_string();

	let return_type_json = procedure::SHAPE.get_utf8(row, procedure::RETURN_TYPE);
	let return_type: Option<TypeConstraint> = if return_type_json.is_empty() {
		None
	} else {
		Some(from_str(return_type_json).expect("TypeConstraint deserializes from stored JSON"))
	};

	if variant == procedure::VARIANT_TEST {
		Procedure::Test {
			id,
			namespace,
			name,
			params,
			return_type,
			body,
		}
	} else {
		let trigger_kind = procedure::SHAPE.get_u8(row, procedure::TRIGGER_KIND);
		let trigger = if trigger_kind == procedure::TRIGGER_EVENT {
			let sumtype = procedure::SHAPE.get_u64(row, procedure::TRIGGER_VARIANT_SUMTYPE);
			let vidx = procedure::SHAPE.get_u16(row, procedure::TRIGGER_VARIANT_INDEX);
			RqlTrigger::Event {
				variant: VariantRef {
					sumtype_id: SumTypeId(sumtype),
					variant_tag: vidx as u8,
				},
			}
		} else {
			RqlTrigger::Call
		};
		Procedure::Rql {
			id,
			namespace,
			name,
			params,
			return_type,
			body,
			trigger,
		}
	}
}
