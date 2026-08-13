// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{CatalogStore, catalog::Catalog};
use reifydb_core::{
	common::{TimeDomain, WindowKind},
	error::diagnostic::flow::{
		flow_join_right_seal_conflicts_with_flag, flow_join_seal_requires_event_time,
		flow_rolling_lag_requires_event_time,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

use crate::flow::{flow::FlowDag, operator::OperatorDef};

fn declared_source_domain(flow: &FlowDag) -> Result<TimeDomain> {
	let mut resolved: Option<TimeDomain> = None;

	for operator_id in flow.topological_order()? {
		let Some(operator) = flow.get_operator(&operator_id) else {
			continue;
		};

		let declared = match &operator.ty {
			OperatorDef::SourceTable {
				time_domain,
				..
			}
			| OperatorDef::SourceRingBuffer {
				time_domain,
				..
			}
			| OperatorDef::SourceSeries {
				time_domain,
				..
			} => *time_domain,
			_ => continue,
		};

		if declared == TimeDomain::None {
			continue;
		}

		resolved = match resolved {
			None | Some(TimeDomain::Event) => Some(declared),
			Some(weaker) => Some(weaker),
		};
	}

	Ok(resolved.unwrap_or(TimeDomain::None))
}

pub fn source_time_domain(_catalog: &Catalog, _txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<TimeDomain> {
	declared_source_domain(flow)
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

pub fn check_join_seal_requirements(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	for operator_id in flow.topological_order()? {
		let Some(operator) = flow.get_operator(&operator_id) else {
			continue;
		};
		let OperatorDef::Join {
			snapshot,
			latest,
			..
		} = &operator.ty
		else {
			continue;
		};
		let Some(seal) = CatalogStore::find_operator_settings(txn, operator_id)?.and_then(|s| s.join) else {
			continue;
		};
		if seal.left.is_none() && seal.right.is_none() {
			continue;
		}

		if seal.right.is_some() {
			if *snapshot {
				return Err(Error(Box::new(flow_join_right_seal_conflicts_with_flag(
					&flow_name, "snapshot",
				))));
			}
			if *latest {
				return Err(Error(Box::new(flow_join_right_seal_conflicts_with_flag(
					&flow_name, "latest",
				))));
			}
		}

		if source_time_domain(catalog, &mut txn.reborrow(), flow)? != TimeDomain::Event {
			return Err(Error(Box::new(flow_join_seal_requires_event_time(&flow_name))));
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::{
		flow::{FlowId, OperatorId},
		id::{TableId, ViewId},
	};

	use super::*;
	use crate::flow::{
		flow::FlowBuilder,
		operator::{FlowEdge, FlowNode},
	};

	const FLOW: FlowId = FlowId(1);

	fn table(id: u64, time_domain: TimeDomain) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SourceTable {
				table: TableId(id),
				time_domain,
			},
		)
	}

	fn view_source(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SourceView {
				view: ViewId(id),
			},
		)
	}

	fn sink(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SinkTableView {
				view: ViewId(id),
				table: TableId(id),
			},
		)
	}

	struct Harness {
		builder: FlowBuilder,
		edges: u64,
	}

	impl Harness {
		fn new() -> Self {
			Self {
				builder: FlowDag::builder(FLOW),
				edges: 0,
			}
		}

		fn node(mut self, node: FlowNode) -> Self {
			self.builder.add_node(node);
			self
		}

		fn edge(mut self, from: u64, to: u64) -> Self {
			self.edges += 1;
			self.builder.add_edge(FlowEdge::new(self.edges, OperatorId(from), OperatorId(to))).unwrap();
			self
		}

		fn domain(self) -> TimeDomain {
			declared_source_domain(&self.builder.build()).unwrap()
		}
	}

	#[test]
	fn a_single_event_source_resolves_to_event() {
		// Anything but Event here rejects every lagged rolling view at DDL.
		let domain = Harness::new().node(table(1, TimeDomain::Event)).node(sink(2)).edge(1, 2).domain();

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn every_source_must_be_event_for_the_flow_to_be_event() {
		// A processing-time source must drag the verdict down, otherwise the lag boundary is applied to rows
		// timed by the ingest clock.
		let domain = Harness::new()
			.node(table(1, TimeDomain::Event))
			.node(table(2, TimeDomain::Processing))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain();

		assert_eq!(domain, TimeDomain::Processing);
	}

	#[test]
	fn a_processing_source_is_not_upgraded_by_an_event_source_discovered_later() {
		// The verdict must never depend on topological order, or the rule passes or fails by graph layout.
		let domain = Harness::new()
			.node(table(1, TimeDomain::Processing))
			.node(table(2, TimeDomain::Event))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain();

		assert_eq!(domain, TimeDomain::Processing);
	}

	#[test]
	fn a_source_declaring_no_time_is_skipped_rather_than_vetoing_the_flow() {
		// A time-less lookup table must never veto a lag over a genuine event stream joined against it.
		let domain = Harness::new()
			.node(table(1, TimeDomain::Event))
			.node(table(2, TimeDomain::None))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain();

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn a_flow_whose_every_source_declares_no_time_resolves_to_none() {
		// Skipping time-less sources must never leave the fold claiming Event by default.
		let domain = Harness::new()
			.node(table(1, TimeDomain::None))
			.node(table(2, TimeDomain::None))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain();

		assert_eq!(domain, TimeDomain::None);
	}

	#[test]
	fn a_view_source_declares_no_domain_of_its_own_and_does_not_supply_event_time() {
		// SourceView names no domain, so it must never vouch for event time on its own.
		let domain = Harness::new().node(view_source(1)).node(sink(2)).edge(1, 2).domain();

		assert_eq!(domain, TimeDomain::None);
	}
}
