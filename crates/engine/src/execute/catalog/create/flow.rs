// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate, transaction::CatalogFlowQueryOperations};
use reifydb_core::{interface::FlowStatus, value::column::Columns};
use reifydb_rql::{flow::compile_flow, plan::physical::CreateFlowNode};
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_flow<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateFlowNode,
	) -> crate::Result<Columns<'a>> {
		if let Some(_) = txn.find_flow_by_name(plan.namespace.id, plan.flow.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("flow", Value::Utf8(plan.flow.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
		}

		// Compile flow - nodes and edges are persisted by the compiler
		let _flow = compile_flow(txn, *plan.as_clause, None)?;

		CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: Some(plan.flow.clone().into_owned()),
				name: plan.flow.text().to_string(),
				namespace: plan.namespace.id,
				status: FlowStatus::Active,
			},
		)?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("flow", Value::Utf8(plan.flow.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}
