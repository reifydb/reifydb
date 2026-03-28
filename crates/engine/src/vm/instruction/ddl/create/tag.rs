// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::sumtype::SumTypeToCreate;
use reifydb_core::{
	interface::catalog::sumtype::{Field, SumTypeKind, Variant},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateTagNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_tag(services: &Services, txn: &mut AdminTransaction, plan: CreateTagNode) -> Result<Columns> {
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

	services.catalog.create_sumtype(
		txn,
		SumTypeToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			variants,
			kind: SumTypeKind::Tag,
		},
	)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("tag", Value::Utf8(plan.name.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
