// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{error::diagnostic::catalog::table_in_use, value::column::columns::Columns};
use reifydb_rql::{flow::node::FlowNodeType, nodes::DropTableNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use super::dependent::find_flow_dependents;
use crate::vm::services::Services;

pub(crate) fn drop_table(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropTableNode,
) -> crate::Result<Columns> {
	let Some(table_id) = plan.table_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("table", Value::Utf8(plan.table_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_table(&mut Transaction::Admin(txn), table_id)?;

	// Check for flows that reference this table
	let nodes = services.catalog.list_flow_nodes_all(&mut Transaction::Admin(txn))?;
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	let dependents = find_flow_dependents(
		&services.catalog,
		txn,
		&nodes,
		&flows,
		|node_type| matches!(node_type, FlowNodeType::SourceTable { table } if *table == table_id),
	)?;
	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return_error!(table_in_use(
			plan.table_name.clone(),
			plan.namespace_name.text(),
			plan.table_name.text(),
			&dependents_str,
		));
	}

	services.catalog.drop_table(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("table", Value::Utf8(plan.table_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
