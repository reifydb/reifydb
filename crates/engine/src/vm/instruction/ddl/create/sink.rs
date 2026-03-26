// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::sink::SinkToCreate;
use reifydb_core::{interface::catalog::flow::FlowStatus, value::column::columns::Columns};
use reifydb_rql::nodes::CreateSinkNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_sink(services: &Services, txn: &mut AdminTransaction, plan: CreateSinkNode) -> Result<Columns> {
	let result = services.catalog.create_sink(
		txn,
		SinkToCreate {
			name: plan.name.clone(),
			namespace: plan.namespace.id(),
			source_namespace: plan.source_namespace.id(),
			source_name: plan.source_name.text().to_string(),
			connector: plan.connector.text().to_string(),
			config: plan
				.config
				.iter()
				.map(|p| (p.key.text().to_string(), p.value.text().to_string()))
				.collect(),
		},
	)?;

	Ok(Columns::single_row([
		("id", Value::Uint8(result.id.0)),
		("namespace", Value::Utf8(plan.namespace.name().to_string())),
		("sink", Value::Utf8(plan.name.text().to_string())),
		("connector", Value::Utf8(result.connector)),
		("status", Value::Utf8(format!("{:?}", FlowStatus::Active))),
		("created", Value::Boolean(true)),
	]))
}
