// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::source::SourceToCreate;
use reifydb_core::{interface::catalog::flow::FlowStatus, value::column::columns::Columns};
use reifydb_rql::nodes::CreateSourceNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_source(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSourceNode,
) -> Result<Columns> {
	let result = services.catalog.create_source(
		txn,
		SourceToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			connector: plan.connector.text().to_string(),
			config: plan
				.config
				.iter()
				.map(|p| (p.key.text().to_string(), p.value.text().to_string()))
				.collect(),
			target_namespace: plan.target_namespace.id(),
			target_name: plan.target_name.text().to_string(),
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("source", Value::Utf8(plan.name.text().to_string())),
		("connector", Value::Utf8(result.connector)),
		("status", Value::Utf8(format!("{:?}", FlowStatus::Active))),
		("created", Value::Boolean(true)),
	]))
}
