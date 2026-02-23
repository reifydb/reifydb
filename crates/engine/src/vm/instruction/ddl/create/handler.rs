// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::handler::HandlerToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateHandlerNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_handler(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateHandlerNode,
) -> crate::Result<Columns> {
	let handler = services.catalog.create_handler(
		txn,
		HandlerToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id,
			on_sumtype_id: plan.on_sumtype_id,
			on_variant_tag: plan.on_variant_tag,
			body_source: plan.body_source.clone(),
		},
	)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.clone())),
		("handler", Value::Utf8(handler.name)),
		("created", Value::Boolean(true)),
	]))
}
