// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::error::diagnostic::flow::flow_span_on_unageable_node;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

pub fn check_declared_spans(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	for operator_id in flow.topological_order()? {
		let operator = flow.get_operator(&operator_id).unwrap();

		let Some(settings) = catalog.find_operator_settings(txn, operator.id)? else {
			continue;
		};
		if settings.ttl.is_none() && settings.join.is_none() {
			continue;
		}
		if !operator.ty.consults_declared_span() {
			return Err(Error(Box::new(flow_span_on_unageable_node(&flow_name, &operator.ty.label()))));
		}
	}

	Ok(())
}
