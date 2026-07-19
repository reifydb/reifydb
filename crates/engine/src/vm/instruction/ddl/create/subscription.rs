// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::subscription::subscription_missing_as_clause,
	interface::catalog::{flow::FlowId, id::SubscriptionId},
	value::column::columns::Columns,
};
use reifydb_rql::{nodes::CreateSubscriptionNode, query::QueryPlan};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, fragment::Fragment, params::Params, value::Value};

use crate::{
	Result,
	flow::compiler::compile_subscription_flow_ephemeral,
	subscription::{SubscriptionContext, SubscriptionServiceRef},
	vm::{services::Services, stack::SymbolTable},
};

pub(crate) fn create_subscription(
	services: &Services,
	txn: &mut Transaction<'_>,
	plan: CreateSubscriptionNode,
	symbols: &SymbolTable,
	params: &Params,
) -> Result<Columns> {
	if let Some(ref as_clause) = plan.as_clause
		&& let QueryPlan::RemoteScan(ref remote) = **as_clause
	{
		let token_value = match &remote.token {
			Some(t) => Value::Utf8(t.clone()),
			None => Value::none(),
		};
		let max_rows_value = match plan.hydration.max_rows {
			Some(n) => Value::Uint8(n),
			None => Value::none(),
		};
		let throttle_value = match plan.throttle {
			Some(d) => Value::Uint8(u64::try_from(d.milliseconds()?).unwrap_or(u64::MAX)),
			None => Value::none(),
		};
		let linger_value = match plan.linger {
			Some(d) => Value::Uint8(u64::try_from(d.milliseconds()?).unwrap_or(u64::MAX)),
			None => Value::none(),
		};
		return Ok(Columns::single_row([
			("remote_address", Value::Utf8(remote.address.clone())),
			("remote_body", Value::Utf8(remote.remote_rql.clone())),
			("remote_token", token_value),
			("remote_hydration_enabled", Value::Boolean(plan.hydration.enabled)),
			("remote_hydration_max_rows", max_rows_value),
			("remote_throttle_ms", throttle_value),
			("remote_linger_ms", linger_value),
		]));
	}

	let sub_service = services.ioc.resolve::<SubscriptionServiceRef>()?;

	let subscription_id = sub_service.next_id();

	let mut column_names: Vec<String> = plan.columns.iter().map(|c| c.name.clone()).collect();
	column_names.push("_op".to_string());

	let as_clause =
		plan.as_clause.ok_or_else(|| Error(Box::new(subscription_missing_as_clause(Fragment::None))))?;

	let flow_id = FlowId(subscription_id.0);
	let flow_dag = compile_subscription_flow_ephemeral(
		&services.catalog,
		&services.routines,
		txn,
		*as_clause,
		subscription_id,
		flow_id,
	)?;

	let ctx = subscription_context(subscription_id, txn, symbols, params);
	sub_service.register_subscription(flow_dag, column_names, plan.hydration.enabled, ctx, txn)?;

	let hydration_max_rows = match plan.hydration.max_rows {
		Some(n) => Value::Uint8(n),
		None => Value::none(),
	};
	let throttle_value = match plan.throttle {
		Some(d) => Value::Uint8(u64::try_from(d.milliseconds()?).unwrap_or(u64::MAX)),
		None => Value::none(),
	};
	let linger_value = match plan.linger {
		Some(d) => Value::Uint8(u64::try_from(d.milliseconds()?).unwrap_or(u64::MAX)),
		None => Value::none(),
	};

	Ok(Columns::single_row([
		("subscription_id", Value::Uint8(subscription_id.0)),
		("created", Value::Boolean(true)),
		("hydration_enabled", Value::Boolean(plan.hydration.enabled)),
		("hydration_max_rows", hydration_max_rows),
		("throttle_ms", throttle_value),
		("linger_ms", linger_value),
	]))
}

fn subscription_context(
	id: SubscriptionId,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
	params: &Params,
) -> SubscriptionContext {
	SubscriptionContext {
		id,
		identity: txn.identity(),
		symbols: symbols.clone(),
		params: params.clone(),
	}
}
