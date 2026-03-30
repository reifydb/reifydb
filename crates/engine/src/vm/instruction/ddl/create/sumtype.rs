// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::sumtype::SumTypeToCreate;
use reifydb_core::{
	interface::catalog::sumtype::{Field, SumTypeKind, Variant},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateSumTypeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_sumtype(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSumTypeNode,
) -> Result<Columns> {
	if let Some(existing) = services.catalog.find_sumtype_by_name(
		&mut Transaction::Admin(&mut *txn),
		plan.namespace.id(),
		plan.name.text(),
	)? && plan.if_not_exists
	{
		return Ok(Columns::single_row([
			("id", Value::Uint8(existing.id.0)),
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("sumtype", Value::Utf8(plan.name.text().to_string())),
			("created", Value::Boolean(false)),
		]));
	}

	let mut variants = Vec::with_capacity(plan.variants.len());
	for (tag, variant) in plan.variants.iter().enumerate() {
		let mut fields = Vec::with_capacity(variant.columns.len());
		for col in &variant.columns {
			fields.push(Field {
				name: col.name.to_lowercase(),
				field_type: col.column_type.clone(),
			});
		}
		variants.push(Variant {
			tag: tag as u8,
			name: variant.name.to_lowercase(),
			fields,
		});
	}

	let result = services.catalog.create_sumtype(
		txn,
		SumTypeToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			variants,
			kind: SumTypeKind::Enum,
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("sumtype", Value::Utf8(plan.name.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
