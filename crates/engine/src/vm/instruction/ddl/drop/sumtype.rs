// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSumTypeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_sumtype(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropSumTypeNode,
) -> crate::Result<Columns> {
	let Some(sumtype_id) = plan.sumtype_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("enum", Value::Utf8(plan.sumtype_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_sumtype(&mut Transaction::Admin(txn), sumtype_id)?;
	services.catalog.drop_sumtype(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("enum", Value::Utf8(plan.sumtype_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
