// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropHandlerNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn drop_handler(services: &Services, txn: &mut AdminTransaction, plan: DropHandlerNode) -> Result<Columns> {
	if plan.procedure_id.is_none() && plan.handler_id.is_none() {
		return Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
			("handler", Value::Utf8(plan.handler_name.text().to_string())),
			("dropped", Value::Boolean(false)),
		]));
	}

	if let Some(procedure_id) = plan.procedure_id {
		services.catalog.drop_procedure(txn, procedure_id)?;
	}

	if let Some(handler_id) = plan.handler_id {
		services.catalog.drop_handler(txn, handler_id)?;
	}

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace_name.text().to_string())),
		("handler", Value::Utf8(plan.handler_name.text().to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
