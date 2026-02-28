// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::procedure::ProcedureToCreate;
use reifydb_core::{interface::catalog::procedure::ProcedureTrigger, value::column::columns::Columns};
use reifydb_rql::nodes::CreateProcedureNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_procedure(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateProcedureNode,
) -> Result<Columns> {
	let is_handler = matches!(plan.trigger, ProcedureTrigger::Event { .. });

	let procedure = services.catalog.create_procedure(
		txn,
		ProcedureToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id,
			params: plan.params,
			return_type: None,
			body: plan.body_source,
			trigger: plan.trigger,
		},
	)?;

	if is_handler {
		// Return handler-style output for backwards compatibility
		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.clone())),
			("handler", Value::Utf8(procedure.name)),
			("created", Value::Boolean(true)),
		]))
	} else {
		Ok(Columns::single_row([("procedure", Value::Utf8(procedure.name)), ("created", Value::Boolean(true))]))
	}
}
