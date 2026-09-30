// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::{TimeDomain, WindowKind},
	error::diagnostic::flow::{flow_join_retention_requires_event_time, flow_rolling_lag_requires_event_time},
	flow::{
		dag::FlowDag,
		operator::{FlowNode, OperatorDef},
	},
	interface::catalog::{
		flow::{FlowId, OperatorId},
		id::ViewId,
	},
	internal,
	operator_with::{JoinWith, LookupWith, WindowWith},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error};

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

	for operator_id in flow.topological_order() {
		let Some(operator) = flow.get_operator(operator_id) else {
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

fn view_flow_dag(catalog: &Catalog, txn: &mut Transaction<'_>, view: ViewId) -> Result<FlowDag> {
	let Some(def) = catalog.find_view(&mut txn.reborrow(), view)? else {
		return Err(Error(Box::new(internal!("view {} has no catalog entry", view.0))));
	};

	let Some(flow) = catalog.find_flow_by_name(&mut txn.reborrow(), def.namespace(), def.name())? else {
		return Err(Error(Box::new(internal!("view {} has no flow to supply its time domain", def.name()))));
	};

	catalog.get_flow_dag(&mut txn.reborrow(), flow.id)
}

fn upstream_view_domain(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	view: ViewId,
	path: &mut HashSet<FlowId>,
) -> Result<TimeDomain> {
	let dag = view_flow_dag(catalog, txn, view)?;

	declared_source_domain(catalog, txn, &dag, path)
}

pub fn source_time_domain(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<TimeDomain> {
	declared_source_domain(catalog, txn, flow, &mut HashSet::new())
}

pub fn check_window_time_requirements(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	let mut lagged = false;

	for operator_id in flow.topological_order() {
		let operator = flow.get_operator(operator_id).unwrap();

		if let OperatorDef::Window {
			with: WindowWith {
				kind: WindowKind::Rolling {
					lag: Some(_),
					..
				},
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

pub fn check_join_retention_requirements(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);
	let mut retained = Vec::new();

	for operator_id in flow.topological_order() {
		let Some(operator) = flow.get_operator(operator_id) else {
			continue;
		};
		match &operator.ty {
			OperatorDef::Join {
				with: JoinWith {
					retention: Some(retention),
					..
				},
				..
			} => {
				if retention.left.is_some() {
					retained.push(side_input(operator, 0)?);
				}
				if retention.right.is_some() {
					retained.push(side_input(operator, 1)?);
				}
			}
			OperatorDef::Lookup {
				with: LookupWith {
					retention: Some(_),
				},
				..
			} => retained.push(side_input(operator, 0)?),
			_ => {}
		}
	}

	if retained.is_empty() {
		return Ok(());
	}

	if source_time_domain(catalog, txn, flow)? != TimeDomain::Event {
		return Err(Error(Box::new(flow_join_retention_requires_event_time(&flow_name))));
	}

	for input in retained {
		if !fed_by_event_time_only(catalog, txn, flow, input)? {
			return Err(Error(Box::new(flow_join_retention_requires_event_time(&flow_name))));
		}
	}

	Ok(())
}

fn side_input(operator: &FlowNode, side: usize) -> Result<OperatorId> {
	match operator.inputs.get(side) {
		Some(input) => Ok(*input),
		None => Err(Error(Box::new(internal!("operator {} has no input {}", operator.id.0, side)))),
	}
}

fn fed_by_event_time_only(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	flow: &FlowDag,
	input: OperatorId,
) -> Result<bool> {
	let mut path = HashSet::from([flow.id]);
	let mut seen = HashSet::new();
	let mut pending = vec![input];

	while let Some(operator_id) = pending.pop() {
		if !seen.insert(operator_id) {
			continue;
		}
		let Some(operator) = flow.get_operator(&operator_id) else {
			return Err(Error(Box::new(internal!("flow {} has no operator {}", flow.id.0, operator_id.0))));
		};
		if !operator.ty.is_source() {
			pending.extend(operator.inputs.iter().copied());
		} else if !source_is_event(catalog, txn, &operator.ty, &mut path)? {
			return Ok(false);
		}
	}

	Ok(true)
}

fn source_is_event(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	source: &OperatorDef,
	path: &mut HashSet<FlowId>,
) -> Result<bool> {
	match source {
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
		} => Ok(*time_domain == TimeDomain::Event),
		OperatorDef::SourceView {
			view,
		} => view_is_event(catalog, txn, *view, path),
		_ => Ok(false),
	}
}

fn view_is_event(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	view: ViewId,
	path: &mut HashSet<FlowId>,
) -> Result<bool> {
	let dag = view_flow_dag(catalog, txn, view)?;

	if !path.insert(dag.id) {
		return Err(Error(Box::new(internal!("flow {} reaches itself through its own sources", dag.id.0))));
	}

	let mut event = true;
	for operator_id in dag.topological_order() {
		let Some(operator) = dag.get_operator(operator_id) else {
			continue;
		};
		if operator.ty.is_source() && !source_is_event(catalog, txn, &operator.ty, path)? {
			event = false;
			break;
		}
	}

	path.remove(&dag.id);

	Ok(event)
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::{
		catalog::flow::FlowToCreate,
		test_utils::{create_namespace, create_view},
	};
	use reifydb_core::{
		common::JoinType,
		flow::{
			dag::FlowBuilder,
			operator::{FlowNode, LookupObject},
		},
		interface::catalog::{
			flow::{FlowEdge, FlowStatus, OperatorId},
			id::TableId,
		},
		row::{JoinRetention, OperatorRetention},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::{fragment::Fragment, value::duration::Duration};

	use super::*;

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
		let catalog = Catalog::testing();
		let mut builder = FlowDag::builder(catalog.next_flow_id(txn).unwrap());
		for source in sources {
			builder.add_node(FlowNode::new(catalog.next_operator_id(txn).unwrap(), source));
		}
		catalog.create_flow(
			txn,
			FlowToCreate {
				name: Fragment::internal(name),
				namespace: view.namespace(),
				status: FlowStatus::Active,
			},
			builder.build(),
		)
		.unwrap();
		view.id()
	}

	fn sink(id: u64) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::SinkTableView {
				view: ViewId(id),
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
			self.builder
				.add_edge(FlowEdge::new(
					self.edges,
					self.builder.id(),
					OperatorId(from),
					OperatorId(to),
				))
				.unwrap();
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

	fn lookup(id: u64, retention: Option<Duration>) -> FlowNode {
		FlowNode::new(
			OperatorId(id),
			OperatorDef::Lookup {
				join_type: JoinType::Inner,
				right: LookupObject::Table(TableId(9_000)),
				left: vec![],
				alias: None,
				with: LookupWith {
					retention: retention.map(|duration| OperatorRetention {
						duration,
					}),
				},
			},
		)
	}

	fn join(id: u64, left: Option<Duration>, right: Option<Duration>) -> FlowNode {
		let retained = |duration: Option<Duration>| {
			duration.map(|duration| OperatorRetention {
				duration,
			})
		};
		FlowNode::new(
			OperatorId(id),
			OperatorDef::Join {
				join_type: JoinType::Left,
				left: vec![],
				right: vec![],
				alias: None,
				natural: false,
				with: JoinWith {
					retention: Some(JoinRetention {
						left: retained(left),
						right: retained(right),
					}),
					..JoinWith::default()
				},
			},
		)
	}

	fn check_retention(harness: Harness, txn: &mut AdminTransaction) -> Result<()> {
		check_join_retention_requirements(
			&Catalog::testing(),
			&mut Transaction::Admin(txn),
			&harness.builder.build(),
		)
	}

	#[test]
	fn a_lookup_retention_over_a_processing_source_is_rejected_with_flow_049() {
		// The lookup frees left rows by event time like the join; an ingest clock frees them at random.
		let mut txn = create_test_admin_transaction();
		let harness = Harness::new()
			.node(table(1, TimeDomain::Processing))
			.node(lookup(2, Some(Duration::from_seconds(10).unwrap())))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3);

		let err = check_retention(harness, &mut txn).expect_err("a lookup retention needs event time");

		assert_eq!(err.diagnostic().code, "FLOW_049");
	}

	#[test]
	fn a_lookup_retention_over_an_event_source_is_accepted() {
		// Event time is exactly what the left expiry measures against, so this is the shape polaris ships.
		let mut txn = create_test_admin_transaction();
		let harness = Harness::new()
			.node(table(1, TimeDomain::Event))
			.node(lookup(2, Some(Duration::from_seconds(10).unwrap())))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3);

		check_retention(harness, &mut txn).expect("event time satisfies a lookup retention");
	}

	#[test]
	fn a_lookup_without_retention_does_not_demand_event_time() {
		// Only a declared retention frees rows by event time; without one the check must stay silent.
		let mut txn = create_test_admin_transaction();
		let harness = Harness::new()
			.node(table(1, TimeDomain::Processing))
			.node(lookup(2, None))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3);

		check_retention(harness, &mut txn).expect("no retention means no event-time requirement");
	}

	#[test]
	fn a_lookup_retention_fed_by_a_time_less_source_is_rejected_with_flow_049() {
		// An unmatched time-less left row reaches the lookup with no #time, so it is never armed and never
		// freed.
		let mut txn = create_test_admin_transaction();
		let harness = Harness::new()
			.node(table(1, TimeDomain::None))
			.node(table(2, TimeDomain::Event))
			.node(join(3, None, None))
			.node(lookup(4, Some(Duration::from_seconds(10).unwrap())))
			.node(sink(5))
			.edge(1, 3)
			.edge(2, 3)
			.edge(3, 4)
			.edge(4, 5);

		let err = check_retention(harness, &mut txn).expect_err("a time-less row would outlive the retention");

		assert_eq!(err.diagnostic().code, "FLOW_049");
	}

	#[test]
	fn a_join_retention_on_a_side_fed_by_a_time_less_source_is_rejected_with_flow_049() {
		// The event source elsewhere in the flow must not vouch for the side that actually holds time-less
		// rows.
		let mut txn = create_test_admin_transaction();
		let harness = Harness::new()
			.node(table(1, TimeDomain::None))
			.node(table(2, TimeDomain::Event))
			.node(join(3, Some(Duration::from_seconds(10).unwrap()), None))
			.node(sink(4))
			.edge(1, 3)
			.edge(2, 3)
			.edge(3, 4);

		let err = check_retention(harness, &mut txn).expect_err("the left side holds rows with no #time");

		assert_eq!(err.diagnostic().code, "FLOW_049");
	}

	#[test]
	fn a_join_retention_on_an_event_side_is_accepted_beside_a_time_less_side() {
		// Only the retained side is walked, otherwise a time-less reference table vetoes every right retention.
		let mut txn = create_test_admin_transaction();
		let harness = Harness::new()
			.node(table(1, TimeDomain::None))
			.node(table(2, TimeDomain::Event))
			.node(join(3, None, Some(Duration::from_seconds(10).unwrap())))
			.node(sink(4))
			.edge(1, 3)
			.edge(2, 3)
			.edge(3, 4);

		check_retention(harness, &mut txn).expect("the retained right side is fed by event time only");
	}

	#[test]
	fn a_lookup_retention_over_a_view_of_a_time_less_flow_is_rejected_with_flow_049() {
		// The view may hold rows with no #time from its time-less source, so it must not pass as event time.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let mixed = upstream(
			&mut txn,
			"mixed",
			vec![source_table(TimeDomain::None), source_table(TimeDomain::Event)],
		);
		let harness = Harness::new()
			.node(view_source(1, mixed))
			.node(lookup(2, Some(Duration::from_seconds(10).unwrap())))
			.node(sink(3))
			.edge(1, 2)
			.edge(2, 3);

		let err = check_retention(harness, &mut txn).expect_err("the view carries time-less rows");

		assert_eq!(err.diagnostic().code, "FLOW_049");
	}
}
