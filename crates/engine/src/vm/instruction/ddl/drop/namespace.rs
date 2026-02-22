// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropNamespaceNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

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
	services.catalog.drop_namespace(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
