// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Reconciliation of a flow's declared time domain against a source object's.
//!
//! Invariant: this is the whole policy, separated from the catalog lookups that feed it, so the accept/reject matrix
//! can be stated exhaustively. A flow declares which domain it operates in; a source object declares where #time is
//! populated from. The two must be checked against each other, and silence must not pick a domain when the sources
//! imply one.
//!
//! A source VIEW declares neither - it inherits from the flow that materialises it - so the reconciliation for a chained
//! view runs against that upstream flow's declared domain, with an undeclared upstream reading as processing exactly as
//! it does at runtime. Skipping views instead would leave the whole matrix unenforced one link down the chain, which is
//! where a long pipeline spends most of its nodes.
//!
//! The walk runs at DEFINITION time, so it fires wherever a view is created - including a test transaction, which
//! never commits and therefore never reaches flow registration. Registration re-runs it as a second line of defence
//! for flows loaded from the catalog on restart, whose sources may have been altered since.

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::{TimeDomain, WindowKind},
	error::diagnostic::flow::{
		flow_event_time_over_inline_data, flow_event_time_over_processing_source,
		flow_event_time_over_processing_view, flow_rolling_lag_requires_event_time, flow_time_domain_undeclared,
	},
};
use reifydb_rql::flow::{flow::FlowDag, node::FlowNodeType};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error, error::Diagnostic};

type ProcessingConflict = fn(&str, &str) -> Diagnostic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeDomainConflict {
	EventOverProcessingSource,
	UndeclaredOverEventSource,
}

pub fn reconcile_time_domain(
	declared: Option<TimeDomain>,
	source: TimeDomain,
) -> std::result::Result<(), TimeDomainConflict> {
	match (declared, source) {
		(Some(TimeDomain::Event), TimeDomain::Processing) => Err(TimeDomainConflict::EventOverProcessingSource),
		(None, TimeDomain::Event) => Err(TimeDomainConflict::UndeclaredOverEventSource),

		_ => Ok(()),
	}
}

pub fn check_time_domain(catalog: &Catalog, txn: &mut Transaction<'_>, flow: &FlowDag) -> Result<()> {
	let flow_name = format!("flow {}", flow.id.0);

	for node_id in flow.topological_order()? {
		let node = flow.get_node(&node_id).unwrap();

		if let FlowNodeType::Window {
			kind:
				WindowKind::Rolling {
					lag: Some(_),
					..
				},
			..
		} = &node.ty && flow.time != Some(TimeDomain::Event)
		{
			return Err(Error(Box::new(flow_rolling_lag_requires_event_time(&flow_name))));
		}

		let source: (String, TimeDomain, ProcessingConflict) = match &node.ty {
			FlowNodeType::SourceInlineData {} => {
				if flow.time == Some(TimeDomain::Event) {
					return Err(Error(Box::new(flow_event_time_over_inline_data(&flow_name))));
				}
				continue;
			}
			FlowNodeType::SourceTable {
				table,
			} => {
				let def = catalog.get_table(&mut txn.reborrow(), *table)?;
				(format!("table {}", def.name), def.time.domain(), flow_event_time_over_processing_source)
			}
			FlowNodeType::SourceSeries {
				series,
			} => {
				let def = catalog.get_series(&mut txn.reborrow(), *series)?;
				(format!("series {}", def.name), def.time.domain(), flow_event_time_over_processing_source)
			}
			FlowNodeType::SourceRingBuffer {
				ringbuffer,
			} => {
				let def = catalog.get_ringbuffer(&mut txn.reborrow(), *ringbuffer)?;
				(
					format!("ringbuffer {}", def.name),
					def.time.domain(),
					flow_event_time_over_processing_source,
				)
			}
			FlowNodeType::SourceView {
				view,
			} => {
				let def = catalog.get_view(&mut txn.reborrow(), *view)?;
				let upstream =
					catalog.find_flow_by_name(&mut txn.reborrow(), def.namespace(), def.name())?;
				match upstream {
					Some(upstream) => (
						format!("view {}", def.name()),
						upstream.time.unwrap_or(TimeDomain::Processing),
						flow_event_time_over_processing_view as ProcessingConflict,
					),
					None => continue,
				}
			}
			_ => continue,
		};

		let (source_name, source_time, over_processing) = source;
		match reconcile_time_domain(flow.time, source_time) {
			Ok(()) => {}
			Err(TimeDomainConflict::EventOverProcessingSource) => {
				return Err(Error(Box::new(over_processing(&flow_name, &source_name))));
			}
			Err(TimeDomainConflict::UndeclaredOverEventSource) => {
				return Err(Error(Box::new(flow_time_domain_undeclared(&flow_name, &source_name))));
			}
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	// Intent: a flow cannot demand event time from a source that supplies none. There is no column to populate
	// #time from, so every row would silently fall back to arrival and every windowed rollup over the flow would
	// bucket by wall clock while claiming to bucket by event time.
	// Mutation: return Ok for this pair and the flow registers, then quietly behaves as processing time.
	fn a_flow_demanding_event_time_over_a_processing_source_is_rejected() {
		assert_eq!(
			reconcile_time_domain(Some(TimeDomain::Event), TimeDomain::Processing),
			Err(TimeDomainConflict::EventOverProcessingSource)
		);
	}

	#[test]
	// Intent: silence must not pick a domain when the sources imply one. An undeclared flow over an event-time
	// source is the trap this rule exists for - the author declared a populator on the table, so they believe the
	// view follows it, while the engine would default the flow to processing time.
	// Mutation: treat None as processing and this pair silently succeeds.
	fn an_undeclared_flow_over_an_event_time_source_is_rejected() {
		assert_eq!(
			reconcile_time_domain(None, TimeDomain::Event),
			Err(TimeDomainConflict::UndeclaredOverEventSource)
		);
	}

	#[test]
	// Intent: declaring processing over an event-time source is an EXPLICIT override and must be accepted. This is
	// the pair that distinguishes the rule above from a blanket ban: the author said processing on purpose, and the
	// only difference from the rejected case is that they said it at all.
	// Mutation: reject this pair too and there is no way to opt out of a source's event time.
	fn an_explicit_processing_override_over_an_event_source_is_accepted() {
		assert_eq!(reconcile_time_domain(Some(TimeDomain::Processing), TimeDomain::Event), Ok(()));
	}

	#[test]
	// Intent: pin the whole 3x2 matrix. The two rejected cells sit diagonally opposite the two that look identical
	// at runtime (declared-processing and undeclared behave the same once running), so only the full table shows
	// that declaredness is load-bearing at definition time and irrelevant afterwards.
	fn the_full_reconciliation_matrix_is_pinned() {
		let expected = [
			((None, TimeDomain::Processing), Ok(())),
			((None, TimeDomain::Event), Err(TimeDomainConflict::UndeclaredOverEventSource)),
			((Some(TimeDomain::Processing), TimeDomain::Processing), Ok(())),
			((Some(TimeDomain::Processing), TimeDomain::Event), Ok(())),
			(
				(Some(TimeDomain::Event), TimeDomain::Processing),
				Err(TimeDomainConflict::EventOverProcessingSource),
			),
			((Some(TimeDomain::Event), TimeDomain::Event), Ok(())),
		];

		for ((declared, source), want) in expected {
			assert_eq!(
				reconcile_time_domain(declared, source),
				want,
				"declared={declared:?} source={source:?}"
			);
		}
	}
}
