// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use reifydb_core::{error::diagnostic::catalog::namespace_in_use, value::column::columns::Columns};
use reifydb_rql::nodes::DropNamespaceNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	return_error,
	value::{Value, constraint::Constraint},
};

use super::dependent::find_column_dependents;
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

	// Collect dictionaries and sumtypes from all descendant namespaces
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

		let mut dependents = find_column_dependents(&services.catalog, txn, &columns, |info| {
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
		})?;

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

		if !dependents.is_empty() {
			let dependents_str = dependents.join(", ");
			return_error!(namespace_in_use(
				plan.namespace_name.clone(),
				plan.namespace_name.text(),
				&dependents_str,
			));
		}
	}

	services.catalog.drop_namespace(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
