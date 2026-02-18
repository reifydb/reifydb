// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::flow::FlowToCreate;
use reifydb_core::{interface::catalog::flow::FlowStatus, value::column::columns::Columns};
use reifydb_rql::nodes::CreateFlowNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{flow::compiler::compile_flow, vm::services::Services};

pub(crate) fn create_flow(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateFlowNode,
) -> crate::Result<Columns> {
	if let Some(_) =
		services.catalog.find_flow_by_name(&mut Transaction::Admin(txn), plan.namespace.id, plan.flow.text())?
	{
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name.to_string())),
				("flow", Value::Utf8(plan.flow.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}
	}

	// Create the flow entry first to get a FlowId
	let flow_def = services.catalog.create_flow(
		txn,
		FlowToCreate {
			name: plan.flow.clone(),
			namespace: plan.namespace.id,
			status: FlowStatus::Active,
		},
	)?;

	// Compile flow with the obtained FlowId - nodes and edges are persisted by the compiler
	let _flow = compile_flow(&services.catalog, txn, *plan.as_clause, None, flow_def.id)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.to_string())),
		("flow", Value::Utf8(plan.flow.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
