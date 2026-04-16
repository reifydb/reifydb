// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::procedure::ProcedureToCreate;
use reifydb_core::{interface::catalog::procedure::RqlTrigger, value::column::columns::Columns};
use reifydb_rql::nodes::CreateProcedureNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_procedure(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateProcedureNode,
) -> Result<Columns> {
	let is_handler = matches!(plan.trigger, RqlTrigger::Event { .. });

	let to_create = if plan.is_test {
		ProcedureToCreate::Test {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			params: plan.params,
			return_type: None,
			body: plan.body_source,
		}
	} else {
		ProcedureToCreate::Rql {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			params: plan.params,
			return_type: None,
			body: plan.body_source,
			trigger: plan.trigger,
		}
	};

	let procedure = services.catalog.create_procedure(txn, to_create)?;

	if is_handler {
		// Return handler-style output for backwards compatibility
		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name().to_string())),
			("handler", Value::Utf8(procedure.name().to_string())),
			("created", Value::Boolean(true)),
		]))
	} else {
		Ok(Columns::single_row([
			("procedure", Value::Utf8(procedure.name().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}
