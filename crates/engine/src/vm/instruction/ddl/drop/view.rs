// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropViewNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_view(services: &Services, txn: &mut AdminTransaction, plan: DropViewNode) -> crate::Result<Columns> {
	let Some(view_id) = plan.view_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("view", Value::Utf8(plan.view_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_view(&mut Transaction::Admin(txn), view_id)?;
	services.catalog.drop_view(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("view", Value::Utf8(plan.view_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
