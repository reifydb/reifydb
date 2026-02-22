// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_core::{error::diagnostic::catalog::namespace_in_use, value::column::columns::Columns};
use reifydb_rql::{flow::node::FlowNodeType, nodes::DropNamespaceNode};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	return_error,
	value::{Value, constraint::Constraint},
};

use super::dependent::{find_column_dependents, find_flow_dependents};
use crate::vm::services::Services;

pub(crate) fn drop_namespace(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropNamespaceNode,
) -> crate::Result<Columns> {
	let Some(namespace_id) = plan.namespace_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_namespace(&mut Transaction::Admin(txn), namespace_id)?;

	// Build the set of all descendant namespace IDs (including namespace_id itself)
	let all_namespaces = services.catalog.list_namespaces_all(&mut Transaction::Admin(txn))?;
	let mut descendant_ids: HashSet<_> = HashSet::new();
	descendant_ids.insert(namespace_id);
	loop {
		let mut changed = false;
		for ns in &all_namespaces {
			if !descendant_ids.contains(&ns.id) && descendant_ids.contains(&ns.parent_id) {
				descendant_ids.insert(ns.id);
				changed = true;
			}
		}
		if !changed {
			break;
		}
	}

	let mut dependents = Vec::new();

	// Check for dictionary/sumtype column references from outside descendant namespaces
	let mut dictionaries = Vec::new();
	let mut sumtypes = Vec::new();
	for &ns_id in &descendant_ids {
		dictionaries.extend(services.catalog.list_dictionaries(&mut Transaction::Admin(txn), ns_id)?);
		sumtypes.extend(services.catalog.list_sumtypes(&mut Transaction::Admin(txn), ns_id)?);
	}
	let dictionary_ids: HashSet<_> = dictionaries.iter().map(|d| d.id).collect();
	let sumtype_ids: HashSet<_> = sumtypes.iter().map(|s| s.id).collect();

	if !dictionary_ids.is_empty() || !sumtype_ids.is_empty() {
		let columns = services.catalog.list_columns_all(&mut Transaction::Admin(txn))?;

		dependents.extend(find_column_dependents(&services.catalog, txn, &columns, |info| {
			if descendant_ids.contains(&info.namespace) {
				return None;
			}
			if let Some(dict_id) = info.column.dictionary_id {
				if dictionary_ids.contains(&dict_id) {
					let name = dictionaries
						.iter()
						.find(|d| d.id == dict_id)
						.map(|d| d.name.as_str())
						.unwrap_or("?");
					return Some(format!(" references dictionary `{}`", name));
				}
			}
			None
		})?);

		dependents.extend(find_column_dependents(&services.catalog, txn, &columns, |info| {
			if descendant_ids.contains(&info.namespace) {
				return None;
			}
			if let Some(Constraint::SumType(id)) = info.column.constraint.constraint() {
				if sumtype_ids.contains(id) {
					let name = sumtypes
						.iter()
						.find(|s| s.id == *id)
						.map(|s| s.name.as_str())
						.unwrap_or("?");
					return Some(format!(" references enum `{}`", name));
				}
			}
			None
		})?);
	}

	// Check for flow references to tables/views/ringbuffers in descendant namespaces from external flows
	let all_tables = services.catalog.list_tables_all(&mut Transaction::Admin(txn))?;
	let all_views = services.catalog.list_views_all(&mut Transaction::Admin(txn))?;
	let all_ringbuffers = services.catalog.list_ringbuffers_all(&mut Transaction::Admin(txn))?;
	let table_ids: HashSet<_> =
		all_tables.iter().filter(|t| descendant_ids.contains(&t.namespace)).map(|t| t.id).collect();
	let view_ids: HashSet<_> =
		all_views.iter().filter(|v| descendant_ids.contains(&v.namespace)).map(|v| v.id).collect();
	let ringbuffer_ids: HashSet<_> =
		all_ringbuffers.iter().filter(|r| descendant_ids.contains(&r.namespace)).map(|r| r.id).collect();

	if !table_ids.is_empty() || !view_ids.is_empty() || !ringbuffer_ids.is_empty() {
		let nodes = services.catalog.list_flow_nodes_all(&mut Transaction::Admin(txn))?;
		let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;

		// Filter to only nodes belonging to flows OUTSIDE descendant namespaces
		let external_nodes: Vec<_> = nodes
			.iter()
			.filter(|n| {
				flows.iter()
					.find(|f| f.id == n.flow)
					.map(|f| !descendant_ids.contains(&f.namespace))
					.unwrap_or(false)
			})
			.cloned()
			.collect();

		dependents.extend(find_flow_dependents(
			&services.catalog,
			txn,
			&external_nodes,
			&flows,
			|node_type| matches!(node_type, FlowNodeType::SourceTable { table } if table_ids.contains(table)),
		)?);

		dependents.extend(find_flow_dependents(
			&services.catalog,
			txn,
			&external_nodes,
			&flows,
			|node_type| {
				matches!(node_type, FlowNodeType::SourceView { view } if view_ids.contains(view))
					|| matches!(node_type, FlowNodeType::SinkView { view } if view_ids.contains(view))
			},
		)?);

		dependents.extend(find_flow_dependents(&services.catalog, txn, &external_nodes, &flows, |node_type| {
			matches!(node_type, FlowNodeType::SourceRingBuffer { ringbuffer } if ringbuffer_ids.contains(ringbuffer))
		})?);
	}

	if !dependents.is_empty() {
		let dependents_str = dependents.join(", ");
		return_error!(namespace_in_use(
			plan.namespace_name.clone(),
			plan.namespace_name.text(),
			&dependents_str,
		));
	}

	services.catalog.drop_namespace(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
