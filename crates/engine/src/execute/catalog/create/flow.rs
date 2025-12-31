// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate};
use reifydb_core::{
	interface::{CatalogTrackFlowChangeOperations, FlowStatus},
	value::column::Columns,
};
use reifydb_rql::plan::physical::CreateFlowNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor, flow::compile_flow};

impl Executor {
	pub(crate) async fn create_flow<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateFlowNode,
	) -> crate::Result<Columns> {
		if let Some(_) = self.catalog.find_flow_by_name(txn, plan.namespace.id, plan.flow.text()).await? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("flow", Value::Utf8(plan.flow.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
		}

		// Create the flow entry first to get a FlowId
		let flow_def = CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: Some(plan.flow.clone()),
				name: plan.flow.text().to_string(),
				namespace: plan.namespace.id,
				status: FlowStatus::Active,
			},
		)
		.await?;
		txn.track_flow_def_created(flow_def.clone())?;

		// Compile flow with the obtained FlowId - nodes and edges are persisted by the compiler
		let _flow = compile_flow(txn, *plan.as_clause, None, flow_def.id).await?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("flow", Value::Utf8(plan.flow.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}
