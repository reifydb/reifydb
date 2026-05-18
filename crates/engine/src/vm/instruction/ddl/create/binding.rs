// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::binding::BindingToCreate;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateBindingNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_binding(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateBindingNode,
) -> Result<Columns> {
	let binding = services.catalog.create_binding(
		txn,
		BindingToCreate {
			namespace: plan.namespace.id(),
			name: plan.name.text().to_string(),
			procedure: plan.procedure_id,
			protocol: plan.protocol.clone(),
			format: plan.format,
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(binding.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("binding", Value::Utf8(plan.name.text().to_string())),
		("protocol", Value::Utf8(binding.protocol.protocol_str().to_string())),
		("format", Value::Utf8(binding.format.as_str().to_string())),
		("created", Value::Boolean(true)),
	]))
}
