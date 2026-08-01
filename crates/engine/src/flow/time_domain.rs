// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::{TimeDomain, WindowKind},
	error::diagnostic::flow::{
		flow_event_time_over_inline_data, flow_event_time_over_processing_source,
		flow_event_time_over_processing_view, flow_rolling_lag_requires_event_time,
		flow_time_domain_undeclared,
	},
};
use reifydb_rql::flow::{flow::FlowDag, operator::OperatorDef};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
};

type ProcessingConflict = fn(&str, &str) -> Diagnostic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeDomainConflict {
	EventOverProcessingSource,
	UndeclaredOverEventSource,
}

pub fn reconcile_time_domain(declared: Option<TimeDomain>, source: TimeDomain) -> StdResult<(), TimeDomainConflict> {
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

		if let OperatorDef::Window {
			kind: WindowKind::Rolling {
				lag: Some(_),
				..
			},
			..
		} = &node.ty && flow.time != Some(TimeDomain::Event)
		{
			return Err(Error(Box::new(flow_rolling_lag_requires_event_time(&flow_name))));
		}

		let source: (String, TimeDomain, ProcessingConflict) = match &node.ty {
			OperatorDef::SourceInlineData {} => {
				if flow.time == Some(TimeDomain::Event) {
					return Err(Error(Box::new(flow_event_time_over_inline_data(&flow_name))));
				}
				continue;
			}
			OperatorDef::SourceTable {
				table,
			} => {
				let def = catalog.get_table(&mut txn.reborrow(), *table)?;
				(
					format!("table {}", def.name),
					def.time.domain(),
					flow_event_time_over_processing_source,
				)
			}
			OperatorDef::SourceSeries {
				series,
			} => {
				let def = catalog.get_series(&mut txn.reborrow(), *series)?;
				(
					format!("series {}", def.name),
					def.time.domain(),
					flow_event_time_over_processing_source,
				)
			}
			OperatorDef::SourceRingBuffer {
				ringbuffer,
			} => {
				let def = catalog.get_ringbuffer(&mut txn.reborrow(), *ringbuffer)?;
				(
					format!("ringbuffer {}", def.name),
					def.time.domain(),
					flow_event_time_over_processing_source,
				)
			}
			OperatorDef::SourceView {
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
	fn a_flow_demanding_event_time_over_a_processing_source_is_rejected() {
		// With no column to populate #time from, rows fall back to arrival and every windowed
		// rollup buckets by wall clock while claiming to bucket by event time.
		assert_eq!(
			reconcile_time_domain(Some(TimeDomain::Event), TimeDomain::Processing),
			Err(TimeDomainConflict::EventOverProcessingSource)
		);
	}

	#[test]
	fn an_undeclared_flow_over_an_event_time_source_is_rejected() {
		// The author declared a populator on the source and believes the view follows it, while
		// silence would default the flow to processing time.
		assert_eq!(
			reconcile_time_domain(None, TimeDomain::Event),
			Err(TimeDomainConflict::UndeclaredOverEventSource)
		);
	}

	#[test]
	fn an_explicit_processing_override_over_an_event_source_is_accepted() {
		// Rejecting this too would leave no way to opt out of a source's event time; the only
		// difference from the rejected case is that the author said it at all.
		assert_eq!(reconcile_time_domain(Some(TimeDomain::Processing), TimeDomain::Event), Ok(()));
	}

	#[test]
	fn the_full_reconciliation_matrix_is_pinned() {
		// Declared-processing and undeclared behave identically once running, so only the full
		// table shows that declaredness is load-bearing at definition time and not after.
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
