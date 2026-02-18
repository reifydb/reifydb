// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::{flow_not_found, namespace_not_found},
	interface::catalog::flow::FlowStatus,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::{AlterFlowAction, AlterFlowNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{fragment::Fragment, return_error, value::Value};

use crate::vm::services::Services;

pub(crate) fn execute_alter_flow(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterFlowNode,
) -> crate::Result<Columns> {
	// Get namespace and flow names from MaybeQualified type
	let namespace_name = plan.flow.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
	let flow_name = plan.flow.name.text();

	// Find the namespace
	let Some(namespace) = services.catalog.find_namespace_by_name(&mut Transaction::Admin(txn), namespace_name)?
	else {
		let ns_fragment =
			plan.flow.namespace.clone().unwrap_or_else(|| Fragment::internal("default".to_string()));

		return_error!(namespace_not_found(ns_fragment, namespace_name,));
	};

	// Find the flow
	let Some(flow) = services.catalog.find_flow_by_name(&mut Transaction::Admin(txn), namespace.id, flow_name)?
	else {
		return_error!(flow_not_found(plan.flow.name.clone(), &namespace.name, flow_name,));
	};

	// Execute the action
	let (operation, details) = match plan.action {
		AlterFlowAction::Rename {
			new_name,
		} => {
			services.catalog.update_flow_name(txn, flow.id, new_name.text().to_string())?;
			("RENAME", Value::Utf8(format!("{} -> {}", flow_name, new_name.text())))
		}
		AlterFlowAction::SetQuery {
			query: _query,
		} => {
			unimplemented!();
		}
		AlterFlowAction::Pause => {
			services.catalog.update_flow_status(txn, flow.id, FlowStatus::Paused)?;
			("PAUSE", Value::Utf8("Flow paused".to_string()))
		}
		AlterFlowAction::Resume => {
			services.catalog.update_flow_status(txn, flow.id, FlowStatus::Active)?;
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
