// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::sumtype_in_use, value::column::columns::Columns};
use reifydb_rql::nodes::DropSumTypeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	return_error,
	value::{Value, constraint::Constraint},
};

use crate::vm::services::Services;

pub(crate) fn drop_sumtype(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropSumTypeNode,
) -> crate::Result<Columns> {
	let Some(sumtype_id) = plan.sumtype_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("enum", Value::Utf8(plan.sumtype_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_sumtype(&mut Transaction::Admin(txn), sumtype_id)?;

	// Check for dependent columns across all entity types
	let columns = services.catalog.list_columns_all(&mut Transaction::Admin(txn))?;
	let mut dependents = Vec::new();
	for info in &columns {
		if let Some(Constraint::SumType(id)) = info.column.constraint.constraint() {
			if *id == sumtype_id {
				let ns = services
					.catalog
					.find_namespace(&mut Transaction::Admin(txn), info.namespace)?;
				let ns_name = ns.map(|n| n.name).unwrap_or_else(|| "?".to_string());
				dependents.push(format!(
					"column `{}` in {} `{}.{}`",
					info.column.name, info.entity_kind, ns_name, info.entity_name
				));
			}
		}
	}
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return_error!(sumtype_in_use(
			plan.sumtype_name.clone(),
			plan.namespace_name.text(),
			plan.sumtype_name.text(),
			&dependents_str,
		));
	}

	services.catalog.drop_sumtype(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("enum", Value::Utf8(plan.sumtype_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
