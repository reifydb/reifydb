// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::{TimeDomain, WindowKind},
	error::diagnostic::flow::flow_rolling_lag_requires_event_time,
};
use reifydb_rql::flow::{flow::FlowDag, operator::OperatorDef};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

pub fn source_time_domain(_catalog: &Catalog, _txn: &mut Transaction<'_>, _flow: &FlowDag) -> Result<TimeDomain> {
	unimplemented!(
		"a flow's time domain must be resolved by walking to its sources; the flow-level \
		 declaration that used to answer this was deleted with the #time singularity change"
	)
}

pub fn check_window_time_requirements(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	for operator_id in flow.topological_order()? {
		let operator = flow.get_operator(&operator_id).unwrap();

		if let OperatorDef::Window {
			kind: WindowKind::Rolling {
				lag: Some(_),
				..
			},
			..
		} = &operator.ty && source_time_domain(catalog, &mut txn.reborrow(), flow)? != TimeDomain::Event
		{
			return Err(Error(Box::new(flow_rolling_lag_requires_event_time(&flow_name))));
		}
	}

	Ok(())
}
