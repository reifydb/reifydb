// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::policy::{PolicyOpToCreate, PolicyTargetType, PolicyToCreate},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreatePolicyNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_policy(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreatePolicyNode,
) -> crate::Result<Columns> {
	let target_type = match plan.target_type.as_str() {
		"table" => PolicyTargetType::Table,
		"column" => PolicyTargetType::Column,
		"namespace" => PolicyTargetType::Namespace,
		"procedure" => PolicyTargetType::Procedure,
		"function" => PolicyTargetType::Function,
		"flow" => PolicyTargetType::Flow,
		"subscription" => PolicyTargetType::Subscription,
		"series" => PolicyTargetType::Series,
		"dictionary" => PolicyTargetType::Dictionary,
		"session" => PolicyTargetType::Session,
		"feature" => PolicyTargetType::Feature,
		_ => PolicyTargetType::Table,
	};

	let operations = plan
		.operations
		.iter()
		.map(|op| PolicyOpToCreate {
			operation: op.operation.clone(),
			body_source: op.body_source.clone(),
		})
		.collect();

	let to_create = PolicyToCreate {
		name: plan.name.as_ref().map(|f| f.text().to_string()),
		target_type,
		target_namespace: plan.scope_namespace.as_ref().map(|f| f.text().to_string()),
		target_object: plan.scope_object.as_ref().map(|f| f.text().to_string()),
		operations,
	};

	let (def, _ops) = services.catalog.create_policy(txn, to_create)?;

	let display_name = def.name.unwrap_or_else(|| format!("policy_{}", def.id));

	Ok(Columns::single_row([("policy", Value::Utf8(display_name)), ("created", Value::Boolean(true))]))
}
