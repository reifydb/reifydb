// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_schema::Schema;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	flow::{dag::FlowDag, operator::OperatorDef},
	operator_with::ApplyWith,
};
use reifydb_engine::subscription::SubscriptionContext;
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::{engine::FlowEngineInner, operator::apply::ApplyOperator};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, value::value_type::field::from_field};

use crate::delivery::{
	DeliveryBuffer,
	sink::{EnumNames, EphemeralSinkSubscriptionOperator},
};

pub(crate) fn register_ephemeral_flow(
	engine: &mut FlowEngineInner,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	flow: FlowDag,
	ctx: &SubscriptionContext,
	delivery: Arc<DeliveryBuffer>,
) -> Result<()> {
	let flow_ctx = Arc::new(FlowContext {
		identity: ctx.identity,
		symbols: ctx.symbols.clone(),
		params: ctx.params.clone(),
	});
	for operator_id in flow.topological_order() {
		let operator = flow.get_operator(operator_id).unwrap();
		match &operator.ty {
			OperatorDef::SinkSubscription {
				..
			} => {
				let parent_schema = engine
					.operator(flow.id, operator.inputs[0])
					.expect("Parent operator not found")
					.output_schema();
				let names = enum_names(catalog, txn, parent_schema.as_deref())?;
				let op = EphemeralSinkSubscriptionOperator::new(
					*operator_id,
					ctx.id,
					&ctx.named_system_columns,
					names,
					delivery.clone(),
				);
				engine.insert_operator(
					flow.id,
					*operator_id,
					Box::new(ApplyOperator::new(
						parent_schema,
						*operator_id,
						Box::new(op),
						&ApplyWith::default(),
					)),
				);
			}
			_ => {
				engine.add_core(txn, &flow, operator, &flow_ctx)?;
			}
		}
	}
	engine.register_flow_dag(flow);
	Ok(())
}

fn enum_names(catalog: &Catalog, txn: &mut Transaction<'_>, schema: Option<&Schema>) -> Result<EnumNames> {
	let mut names = HashMap::new();
	for field in schema.map(|schema| schema.fields().iter()).into_iter().flatten() {
		let Some(id) = from_field(field)?.sumtype else {
			continue;
		};
		if names.contains_key(&id) {
			continue;
		}
		let sumtype = catalog.get_sumtype(txn, id)?;
		let namespace = catalog.get_namespace(txn, sumtype.namespace)?;
		let variants = sumtype.variants.into_iter().map(|variant| (variant.tag, variant.name)).collect();
		names.insert(id, (format!("{}::{}", namespace.name(), sumtype.name), variants));
	}
	Ok(EnumNames::new(names))
}
