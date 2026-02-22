// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::dictionary_in_use, value::column::columns::Columns};
use reifydb_rql::nodes::DropDictionaryNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use crate::vm::services::Services;

pub(crate) fn drop_dictionary(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropDictionaryNode,
) -> crate::Result<Columns> {
	let Some(dictionary_id) = plan.dictionary_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("dictionary", Value::Utf8(plan.dictionary_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_dictionary(&mut Transaction::Admin(txn), dictionary_id)?;

	// Check for dependent columns across all entity types
	let columns = services.catalog.list_columns_all(&mut Transaction::Admin(txn))?;
	let mut dependents = Vec::new();
	for info in &columns {
		if info.column.dictionary_id == Some(dictionary_id) {
			let ns = services.catalog.find_namespace(&mut Transaction::Admin(txn), info.namespace)?;
			let ns_name = ns.map(|n| n.name).unwrap_or_else(|| "?".to_string());
			dependents.push(format!(
				"column `{}` in {} `{}.{}`",
				info.column.name, info.entity_kind, ns_name, info.entity_name
			));
		}
	}
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return_error!(dictionary_in_use(
			plan.dictionary_name.clone(),
			plan.namespace_name.text(),
			plan.dictionary_name.text(),
			&dependents_str,
		));
	}

	services.catalog.drop_dictionary(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("dictionary", Value::Utf8(plan.dictionary_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
