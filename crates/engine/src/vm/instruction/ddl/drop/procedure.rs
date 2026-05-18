// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::procedure_has_live_bindings, value::column::columns::Columns};
use reifydb_rql::nodes::DropProcedureNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use crate::{Result, vm::services::Services};

pub(crate) fn drop_procedure(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropProcedureNode,
) -> Result<Columns> {
	let Some(procedure_id) = plan.procedure_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("procedure", Value::Utf8(plan.procedure_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let live_bindings = services.catalog.list_bindings_for_procedure(&mut Transaction::Admin(txn), procedure_id)?;
	if !live_bindings.is_empty() {
		let names: Vec<String> = live_bindings.iter().map(|b| b.name.clone()).collect();
		return_error!(procedure_has_live_bindings(
			plan.procedure_name.clone(),
			plan.namespace_name.text(),
			&names
		));
	}

	services.catalog.drop_procedure(txn, procedure_id)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("procedure", Value::Utf8(plan.procedure_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
