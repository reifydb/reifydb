// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_core::{
	error::diagnostic::flow::flow_seal_shorter_below, flow::dag::FlowDag, interface::catalog::flow::OperatorId,
};
use reifydb_engine::{engine::StandardEngine, flow_check::FlowRegistrationCheck};
use reifydb_flow_async::{
	engine::FlowEngineInner, operator::metrics::OperatorSampleRegistry, transaction::substrate::FlowSubstrate,
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{error, error::ValueError, value::duration::Duration};

use crate::{builder::CustomOperators, operator::provider::StandardOperatorProvider};

pub(crate) struct FlowRegistrationProbe {
	pub(crate) engine: StandardEngine,
	pub(crate) custom_operators: CustomOperators,
	pub(crate) substrate: FlowSubstrate,
}

impl FlowRegistrationCheck for FlowRegistrationProbe {
	fn check(&self, txn: &mut AdminTransaction, dag: &FlowDag) -> Result<(), ValueError> {
		let mut probe = FlowEngineInner::new(
			self.engine.catalog(),
			self.engine.executor().routines.clone(),
			RuntimeContext::builder(self.engine.runtime_context().spawner.clone())
				.clock(self.engine.runtime_context().clock.clone())
				.rng(self.engine.runtime_context().rng.fork(b"sub-flow/check"))
				.build(),
			Arc::new(StandardOperatorProvider::new(self.custom_operators.clone())),
			self.substrate.clone(),
			OperatorSampleRegistry::new(),
		);
		probe.register_with_transaction(&mut Transaction::Admin(txn), dag.clone())?;
		check_seal_order(&probe, dag)
	}
}

fn check_seal_order(probe: &FlowEngineInner, dag: &FlowDag) -> Result<(), ValueError> {
	let mut widest_above: BTreeMap<OperatorId, (Duration, String)> = BTreeMap::new();
	for operator_id in dag.topological_order() {
		let Some(node) = dag.get_operator(operator_id) else {
			continue;
		};
		let mut widest: Option<(Duration, String)> = None;
		for input in &node.inputs {
			if let Some((span, upper)) = widest_above.get(input)
				&& widest.as_ref().is_none_or(|(current, _)| span > current)
			{
				widest = Some((*span, upper.clone()));
			}
		}
		let own = probe.operator(dag.id, *operator_id).and_then(|operator| operator.seal_span());
		if let (Some(own_span), Some((upper_span, upper))) = (own, &widest)
			&& own_span < *upper_span
		{
			return Err(error!(flow_seal_shorter_below(
				&format!("flow {}", dag.id.0),
				&node.ty.label(),
				own_span,
				upper,
				*upper_span,
			)));
		}
		let leaving = match (widest, own.map(|span| (span, node.ty.label()))) {
			(Some(upper), Some(own)) if own.0 > upper.0 => Some(own),
			(upper, own) => upper.or(own),
		};
		if let Some(leaving) = leaving {
			widest_above.insert(*operator_id, leaving);
		}
	}
	Ok(())
}
