// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, ProcedureId},
		procedure::{Procedure, ProcedureParam, RqlTrigger},
	},
	key::{
		namespace_procedure::NamespaceProcedureKey, procedure::ProcedureKey, procedure_param::ProcedureParamKey,
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{
	constraint::TypeConstraint,
	sumtype::{SumTypeId, VariantRef},
};
use serde_json::from_str;

use crate::{
	CatalogStore, Result,
	store::procedure::shape::{TRIGGER_EVENT, VARIANT_TEST, namespace_procedure, procedure, procedure_param},
};

impl CatalogStore {
	pub(crate) fn find_procedure(rx: &mut Transaction<'_>, id: ProcedureId) -> Result<Option<Procedure>> {
		let Some(multi) = rx.get(&ProcedureKey::encoded(id))? else {
			return Ok(None);
		};
		let params = load_params(rx, id)?;
		Ok(Some(decode_procedure(&multi.bytes, params)))
	}

	pub(crate) fn find_procedure_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: &str,
	) -> Result<Option<Procedure>> {
		let mut found_id = None;
		let mut stream = rx.range(NamespaceProcedureKey::full_scan(namespace), RangeScope::All, 1024)?;
		for entry in stream.by_ref() {
			let multi = entry?;
			let bytes = &multi.bytes;
			let candidate = namespace_procedure::get_name(bytes);
			if candidate == name {
				found_id = Some(ProcedureId::from_raw(namespace_procedure::get_id(bytes)));
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
	let mut stream = rx.range(ProcedureParamKey::full_scan(procedure_id), RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let multi = entry?;
		let bytes = &multi.bytes;
		let index = procedure_param::get_index(bytes);
		let name = procedure_param::get_name(bytes).to_string();
		let json = procedure_param::get_type_constraint(bytes);
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

pub(crate) fn decode_procedure(bytes: &EncodedBytes, params: Vec<ProcedureParam>) -> Procedure {
	let id = ProcedureId::from_raw(procedure::get_id(bytes));
	let namespace = NamespaceId(procedure::get_namespace(bytes));
	let name = procedure::get_name(bytes).to_string();
	let variant = procedure::get_variant(bytes);
	let body = procedure::get_body(bytes).to_string();

	let return_type_json = procedure::get_return_type(bytes);
	let return_type: Option<TypeConstraint> = if return_type_json.is_empty() {
		None
	} else {
		Some(from_str(return_type_json).expect("TypeConstraint deserializes from stored JSON"))
	};

	if variant == VARIANT_TEST {
		Procedure::Test {
			id,
			namespace,
			name,
			params,
			return_type,
			body,
		}
	} else {
		let trigger_kind = procedure::get_trigger_kind(bytes);
		let trigger = if trigger_kind == TRIGGER_EVENT {
			let sumtype = procedure::get_trigger_variant_sumtype(bytes);
			let vidx = procedure::get_trigger_variant_index(bytes);
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
