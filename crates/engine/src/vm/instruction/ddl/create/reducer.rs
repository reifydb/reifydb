// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::reducer::ReducerToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateReducerNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_reducer(
	services: &Services,
	txn: &mut AdminTransaction,
	node: CreateReducerNode,
) -> crate::Result<Columns> {
	// Check if reducer already exists
	if let Some(_) = services.catalog.find_reducer_by_name(txn, node.namespace.id, node.reducer.text())? {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(node.namespace.name.to_string())),
			("reducer", Value::Utf8(node.reducer.text().to_string())),
			("created", Value::Boolean(false)),
		]));
	}

	let key_columns: Vec<String> = node.key.iter().map(|k| k.text().to_string()).collect();

	// Create the reducer in the catalog
	let _reducer_def = services.catalog.create_reducer(
		txn,
		ReducerToCreate {
			name: node.reducer.clone(),
			namespace: node.namespace.id,
			key_columns,
		},
	)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(node.namespace.name.to_string())),
		("reducer", Value::Utf8(node.reducer.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
