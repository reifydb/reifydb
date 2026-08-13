// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_catalog::{CatalogStore, catalog::Catalog};
use reifydb_core::{
	common::{TimeDomain, WindowKind},
	error::diagnostic::flow::{
		flow_join_right_seal_conflicts_with_flag, flow_join_seal_requires_event_time,
		flow_rolling_lag_requires_event_time,
	},
	interface::catalog::{flow::FlowId, id::ViewId},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

use crate::flow::{flow::FlowDag, loader::load_flow_dag, operator::OperatorDef};

fn declared_source_domain(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	flow: &FlowDag,
	path: &mut HashSet<FlowId>,
) -> Result<TimeDomain> {
	if !path.insert(flow.id) {
		return Err(Error(Box::new(internal!("flow {} reaches itself through its own sources", flow.id.0))));
	}

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
			OperatorDef::SourceView {
				view,
			} => upstream_view_domain(catalog, &mut txn.reborrow(), *view, path)?,
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

	path.remove(&flow.id);

	Ok(resolved.unwrap_or(TimeDomain::None))
}

fn upstream_view_domain(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	view: ViewId,
	path: &mut HashSet<FlowId>,
) -> Result<TimeDomain> {
	let Some(def) = catalog.find_view(&mut txn.reborrow(), view)? else {
		return Err(Error(Box::new(internal!("view {} has no catalog entry", view.0))));
	};

	let Some(flow) = catalog.find_flow_by_name(&mut txn.reborrow(), def.namespace(), def.name())? else {
		return Err(Error(Box::new(internal!("view {} has no flow to supply its time domain", def.name()))));
	};

	let dag = load_flow_dag(&mut txn.reborrow(), flow.id)?;

	declared_source_domain(catalog, txn, &dag, path)
}

pub fn source_time_domain(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<TimeDomain> {
	declared_source_domain(catalog, txn, flow, &mut HashSet::new())
}

pub fn check_window_time_requirements(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	let mut lagged = false;

	for operator_id in flow.topological_order()? {
		let operator = flow.get_operator(&operator_id).unwrap();

		if let OperatorDef::Window {
			kind: WindowKind::Rolling {
				lag: Some(_),
				..
			},
			..
		} = &operator.ty
		{
			lagged = true;
			break;
		}
	}

	if lagged && source_time_domain(catalog, txn, flow)? != TimeDomain::Event {
		return Err(Error(Box::new(flow_rolling_lag_requires_event_time(&flow_name))));
	}

	Ok(())
}

pub fn check_join_seal_requirements(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);
	let mut sealed = false;

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

		sealed = true;
	}

	if sealed && source_time_domain(catalog, txn, flow)? != TimeDomain::Event {
		return Err(Error(Box::new(flow_join_seal_requires_event_time(&flow_name))));
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use postcard::to_allocvec;
	use reifydb_catalog::test_utils::{create_flow, create_namespace, create_operator, create_view};
	use reifydb_core::interface::catalog::{flow::OperatorId, id::TableId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::admin::AdminTransaction;

	use super::*;
	use crate::flow::{
		flow::FlowBuilder,
		operator::{FlowEdge, FlowNode},
	};

	const FLOW: FlowId = FlowId(9_999);

	fn table(id: u64, time_domain: TimeDomain) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SourceTable {
				table: TableId(id),
				time_domain,
			},
		)
	}

	fn view_source(id: u64, view: ViewId) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SourceView {
				view,
			},
		)
	}

	fn source_table(time_domain: TimeDomain) -> OperatorDef {
		OperatorDef::SourceTable {
			table: TableId(1),
			time_domain,
		}
	}

	fn upstream(txn: &mut AdminTransaction, name: &str, sources: Vec<OperatorDef>) -> ViewId {
		let view = create_view(txn, "test", name, &[]);
		let flow = create_flow(txn, "test", name);
		for source in sources {
			let encoded = to_allocvec(&source).expect("encode");
			create_operator(txn, flow.id, source.discriminator(), &encoded);
		}
		view.id()
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

		fn resolve(self, txn: &mut AdminTransaction) -> Result<TimeDomain> {
			source_time_domain(&Catalog::testing(), &mut Transaction::Admin(txn), &self.builder.build())
		}

		fn domain(self, txn: &mut AdminTransaction) -> TimeDomain {
			self.resolve(txn).unwrap()
		}
	}

	#[test]
	fn a_single_event_source_resolves_to_event() {
		// Anything but Event here rejects every lagged rolling view at DDL.
		let mut txn = create_test_admin_transaction();

		let domain = Harness::new().node(table(1, TimeDomain::Event)).node(sink(2)).edge(1, 2).domain(&mut txn);

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn every_source_must_be_event_for_the_flow_to_be_event() {
		// A processing-time source must drag the verdict down, otherwise the lag bound is applied to rows timed
		// by the ingest clock.
		let mut txn = create_test_admin_transaction();

		let domain = Harness::new()
			.node(table(1, TimeDomain::Event))
			.node(table(2, TimeDomain::Processing))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain(&mut txn);

		assert_eq!(domain, TimeDomain::Processing);
	}

	#[test]
	fn a_processing_source_is_not_upgraded_by_an_event_source_discovered_later() {
		// The verdict must never depend on topological order, or the rule passes or fails by graph layout.
		let mut txn = create_test_admin_transaction();

		let domain = Harness::new()
			.node(table(1, TimeDomain::Processing))
			.node(table(2, TimeDomain::Event))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain(&mut txn);

		assert_eq!(domain, TimeDomain::Processing);
	}

	#[test]
	fn a_source_declaring_no_time_is_skipped_rather_than_vetoing_the_flow() {
		// A time-less lookup table must never veto a lag over a genuine event stream joined against it.
		let mut txn = create_test_admin_transaction();

		let domain = Harness::new()
			.node(table(1, TimeDomain::Event))
			.node(table(2, TimeDomain::None))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain(&mut txn);

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn a_flow_whose_every_source_declares_no_time_resolves_to_none() {
		// Skipping time-less sources must never leave the fold claiming Event by default.
		let mut txn = create_test_admin_transaction();

		let domain = Harness::new()
			.node(table(1, TimeDomain::None))
			.node(table(2, TimeDomain::None))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain(&mut txn);

		assert_eq!(domain, TimeDomain::None);
	}

	#[test]
	fn a_view_source_over_a_time_less_flow_supplies_no_time_either() {
		// A view must pass on exactly what its own sources supply, or a seal is admitted on a chain that stamps
		// no event time at all.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let quiet = upstream(&mut txn, "quiet", vec![source_table(TimeDomain::None)]);

		let domain = Harness::new().node(view_source(1, quiet)).node(sink(2)).edge(1, 2).domain(&mut txn);

		assert_eq!(domain, TimeDomain::None);
	}

	#[test]
	fn a_view_source_supplies_the_event_time_of_the_flow_that_fills_it() {
		// #time is stamped at the source table and rides into the view's stored rows, so reading a view as
		// time-less rejects every sealed join between two views.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let trades = upstream(&mut txn, "trades", vec![source_table(TimeDomain::Event)]);

		let domain = Harness::new().node(view_source(1, trades)).node(sink(2)).edge(1, 2).domain(&mut txn);

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn a_view_source_resolves_through_a_chain_of_views() {
		// Without recursing past the first hop the top of every multi-view chain resolves to None while the
		// one-hop case still passes.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let trades = upstream(&mut txn, "trades", vec![source_table(TimeDomain::Event)]);
		let price = upstream(
			&mut txn,
			"price",
			vec![OperatorDef::SourceView {
				view: trades,
			}],
		);

		let domain = Harness::new().node(view_source(1, price)).node(sink(2)).edge(1, 2).domain(&mut txn);

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn a_processing_time_source_two_views_up_still_drags_the_chain_down() {
		// The weakest source anywhere upstream must decide, otherwise a seal is admitted against rows timed by
		// the ingest clock.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let mixed = upstream(
			&mut txn,
			"mixed",
			vec![source_table(TimeDomain::Event), source_table(TimeDomain::Processing)],
		);
		let price = upstream(
			&mut txn,
			"price",
			vec![OperatorDef::SourceView {
				view: mixed,
			}],
		);

		let domain = Harness::new().node(view_source(1, price)).node(sink(2)).edge(1, 2).domain(&mut txn);

		assert_eq!(domain, TimeDomain::Processing);
	}

	#[test]
	fn the_same_view_read_twice_in_one_flow_is_not_mistaken_for_a_cycle() {
		// The guard must catch an ancestor and never a repeat visit, or a union appending one view twice is
		// rejected as self-referential.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let trades = upstream(&mut txn, "trades", vec![source_table(TimeDomain::Event)]);

		let domain = Harness::new()
			.node(view_source(1, trades))
			.node(view_source(2, trades))
			.node(sink(3))
			.edge(1, 3)
			.edge(2, 3)
			.domain(&mut txn);

		assert_eq!(domain, TimeDomain::Event);
	}

	#[test]
	fn a_view_source_with_no_flow_behind_it_is_an_error_not_a_silent_none() {
		// A missing upstream must never read as "declares no time", or catalog corruption surfaces as a
		// diagnostic blaming the author's DDL.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let orphan = create_view(&mut txn, "test", "orphan", &[]).id();

		let result = Harness::new().node(view_source(1, orphan)).node(sink(2)).edge(1, 2).resolve(&mut txn);

		assert!(result.is_err(), "a view with no flow must not resolve to a domain");
	}
}
