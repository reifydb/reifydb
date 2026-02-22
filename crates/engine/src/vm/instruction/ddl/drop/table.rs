// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropTableNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

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
	services.catalog.drop_table(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("table", Value::Utf8(plan.table_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
