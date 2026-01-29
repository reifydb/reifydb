// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::flow::FlowToCreate;
use reifydb_core::{interface::catalog::flow::FlowStatus, value::column::columns::Columns};
use reifydb_rql::plan::physical::CreateFlowNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{execute::Executor, flow::compiler::compile_flow};

impl Executor {
	pub(crate) fn create_flow<'a>(
		&self,
		txn: &mut AdminTransaction,
		plan: CreateFlowNode,
	) -> crate::Result<Columns> {
		if let Some(_) = self.catalog.find_flow_by_name(txn, plan.namespace.id, plan.flow.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("flow", Value::Utf8(plan.flow.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
		}

		// Create the flow entry first to get a FlowId
		let flow_def = self.catalog.create_flow(
			txn,
			FlowToCreate {
				fragment: Some(plan.flow.clone()),
				name: plan.flow.text().to_string(),
				namespace: plan.namespace.id,
				status: FlowStatus::Active,
			},
		)?;

		// Compile flow with the obtained FlowId - nodes and edges are persisted by the compiler
		let _flow = compile_flow(&self.catalog, txn, *plan.as_clause, None, flow_def.id)?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("flow", Value::Utf8(plan.flow.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}
