// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropFlowNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_flow(services: &Services, txn: &mut AdminTransaction, plan: DropFlowNode) -> crate::Result<Columns> {
	let Some(flow_id) = plan.flow_id else {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("flow", Value::Utf8(plan.flow_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	};

	let def = services.catalog.get_flow(&mut Transaction::Admin(txn), flow_id)?;
	services.catalog.drop_flow(txn, def)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("flow", Value::Utf8(plan.flow_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
