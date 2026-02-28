// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::sumtype::SumTypeToCreate;
use reifydb_core::{
	interface::catalog::sumtype::{FieldDef, SumTypeKind, VariantDef},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateEventNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_event(services: &Services, txn: &mut AdminTransaction, plan: CreateEventNode) -> Result<Columns> {
	let mut variant_defs = Vec::with_capacity(plan.variants.len());
	for (tag, variant) in plan.variants.iter().enumerate() {
		let mut fields = Vec::with_capacity(variant.columns.len());
		for col in &variant.columns {
			fields.push(FieldDef {
				name: col.name.to_lowercase(),
				field_type: col.column_type.clone(),
			});
		}
		variant_defs.push(VariantDef {
			tag: tag as u8,
			name: variant.name.to_lowercase(),
			fields,
		});
	}

	services.catalog.create_sumtype(
		txn,
		SumTypeToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id,
			variants: variant_defs,
			kind: SumTypeKind::Event,
		},
	)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.clone())),
		("event", Value::Utf8(plan.name.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
