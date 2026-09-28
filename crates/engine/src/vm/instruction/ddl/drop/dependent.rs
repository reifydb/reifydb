// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{catalog::Catalog, store::column::list::ColumnInfo};
use reifydb_core::{
	flow::{dag::FlowDag, operator::OperatorDef},
	interface::catalog::flow::Flow,
};
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
			let ns_name = ns.map(|n| n.name().to_string()).unwrap_or_else(|| "?".to_string());
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
	dags: &[FlowDag],
	flows: &[Flow],
	check: impl Fn(&OperatorDef) -> bool,
) -> Result<Vec<String>> {
	let mut dependents = Vec::new();
	for dag in dags {
		if dag.get_operator_ids().any(|id| dag.get_operator(&id).is_some_and(|node| check(&node.ty)))
			&& let Some(flow) = flows.iter().find(|f| f.id == dag.id())
		{
			let ns = catalog.find_namespace(&mut Transaction::Admin(txn), flow.namespace)?;
			let ns_name = ns.map(|n| n.name().to_string()).unwrap_or_else(|| "?".to_string());
			dependents.push(format!("flow `{}.{}`", ns_name, flow.name));
		}
	}
	Ok(dependents)
}
