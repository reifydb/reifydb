// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::namespace_not_found, interface::FlowStatus, return_error, value::column::Columns,
};
use reifydb_rql::plan::physical::{AlterFlowAction, AlterFlowNode};
use reifydb_type::{Fragment, Value};

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) async fn execute_alter_flow<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: AlterFlowNode,
	) -> crate::Result<Columns> {
		// Get namespace and flow names from MaybeQualified type
		let namespace_name = plan.flow.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let flow_name = plan.flow.name.text();

		// Find the namespace
		let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name).await? else {
			let ns_fragment = plan
				.flow
				.namespace
				.clone()
				.unwrap_or_else(|| Fragment::internal("default".to_string()));

			return_error!(namespace_not_found(ns_fragment, namespace_name,));
		};

		// Find the flow
		let Some(flow) = CatalogStore::find_flow_by_name(txn, namespace.id, flow_name).await? else {
			return_error!(reifydb_core::diagnostic::catalog::flow_not_found(
				plan.flow.name.clone().into_owned(),
				&namespace.name,
				flow_name,
			));
		};

		// Execute the action
		let (operation, details) = match plan.action {
			AlterFlowAction::Rename {
				new_name,
			} => {
				CatalogStore::update_flow_name(txn, flow.id, new_name.text().to_string()).await?;
				("RENAME", Value::Utf8(format!("{} -> {}", flow_name, new_name.text())))
			}
			AlterFlowAction::SetQuery {
				query: _query,
			} => {
				unimplemented!();
			}
			AlterFlowAction::Pause => {
				CatalogStore::update_flow_status(txn, flow.id, FlowStatus::Paused).await?;
				("PAUSE", Value::Utf8("Flow paused".to_string()))
			}
			AlterFlowAction::Resume => {
				CatalogStore::update_flow_status(txn, flow.id, FlowStatus::Active).await?;
				("RESUME", Value::Utf8("Flow resumed".to_string()))
			}
		};

		Ok(Columns::single_row([
			("operation", Value::Utf8(operation.to_string())),
			("namespace", Value::Utf8(namespace.name)),
			("flow", Value::Utf8(flow.name)),
			("details", details),
		]))
	}
}
