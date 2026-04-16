// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropProcedureNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

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

	services.catalog.drop_procedure(txn, procedure_id)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("procedure", Value::Utf8(plan.procedure_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
