// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_catalog::{catalog::Catalog, store::column::list::ColumnInfo};
use reifydb_core::{
	interface::catalog::flow::{FlowDef, FlowNodeDef},
	internal_error,
};
use reifydb_rql::flow::node::FlowNodeType;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::Result;

pub(crate) fn find_column_dependents(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	columns: &[ColumnInfo],
	check: impl Fn(&ColumnInfo) -> Option<String>,
) -> Result<Vec<String>> {
	let mut dependents = Vec::new();
	for info in columns {
		if let Some(suffix) = check(info) {
			let ns = catalog.find_namespace(&mut Transaction::Admin(txn), info.namespace)?;
			let ns_name = ns.map(|n| n.name).unwrap_or_else(|| "?".to_string());
			let mut desc = format!(
				"column `{}` in {} `{}.{}`",
				info.column.name, info.entity_kind, ns_name, info.entity_name
			);
			if !suffix.is_empty() {
				desc.push_str(&suffix);
			}
			dependents.push(desc);
		}
	}
	Ok(dependents)
}

pub(crate) fn find_flow_dependents(
	catalog: &Catalog,
	txn: &mut AdminTransaction,
	nodes: &[FlowNodeDef],
	flows: &[FlowDef],
	check: impl Fn(&FlowNodeType) -> bool,
) -> Result<Vec<String>> {
	let mut dependents = Vec::new();
	let mut seen_flows = HashSet::new();
	for node in nodes {
		let node_type: FlowNodeType = postcard::from_bytes(node.data.as_ref())
			.map_err(|e| internal_error!("Failed to deserialize flow node type: {}", e))?;
		if check(&node_type) && seen_flows.insert(node.flow) {
			if let Some(flow) = flows.iter().find(|f| f.id == node.flow) {
				let ns = catalog.find_namespace(&mut Transaction::Admin(txn), flow.namespace)?;
				let ns_name = ns.map(|n| n.name).unwrap_or_else(|| "?".to_string());
				dependents.push(format!("flow `{}.{}`", ns_name, flow.name));
			}
		}
	}
	Ok(dependents)
}
