// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, store::flow::create::FlowToCreate, transaction::CatalogFlowQueryOperations};
use reifydb_core::{interface::FlowStatus, value::column::Columns};
use reifydb_rql::plan::physical::CreateFlowNode;
use reifydb_type::Value;

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_flow<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateFlowNode,
	) -> crate::Result<Columns<'a>> {
		// Check if flow already exists using the transaction's catalog operations
		if let Some(_) = txn.find_flow_by_name(plan.namespace.id, plan.flow.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("flow", Value::Utf8(plan.flow.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}
			// The error will be returned by create_flow if the flow exists
		}

		// TODO: Properly serialize the physical plan
		// For now, store a placeholder. The actual flow graph compilation
		// will happen when the flow is started.
		let query_placeholder = format!("FLOW: {}", plan.flow.text());
		let query_blob = reifydb_type::Blob::from(query_placeholder.as_bytes());

		CatalogStore::create_flow(
			txn,
			FlowToCreate {
				fragment: Some(plan.flow.clone().into_owned()),
				name: plan.flow.text().to_string(),
				namespace: plan.namespace.id,
				query: query_blob,
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
