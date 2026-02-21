// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::procedure::ProcedureToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateProcedureNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_procedure(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateProcedureNode,
) -> crate::Result<Columns> {
	let procedure = services.catalog.create_procedure(
		txn,
		ProcedureToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id,
			params: plan.params,
			return_type: None,
			body: plan.body_source,
		},
	)?;

	Ok(Columns::single_row([("procedure", Value::Utf8(procedure.name)), ("created", Value::Boolean(true))]))
}
