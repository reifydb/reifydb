// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Reconciliation of a flow's declared time domain against a source object's.
//!
//! Invariant: this is the whole policy, separated from the catalog lookups that feed it, so the accept/reject matrix
//! can be stated exhaustively. A flow declares which domain it operates in; a source object declares where #time is
//! populated from. The two must be checked against each other, and silence must not pick a domain when the sources
//! imply one.

use reifydb_core::common::TimeDomain;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeDomainConflict {
	EventOverProcessingSource,
	UndeclaredOverEventSource,
}

pub(crate) fn reconcile_time_domain(
	declared: Option<TimeDomain>,
	source: TimeDomain,
) -> Result<(), TimeDomainConflict> {
	match (declared, source) {
		(Some(TimeDomain::Event), TimeDomain::Processing) => Err(TimeDomainConflict::EventOverProcessingSource),
		(None, TimeDomain::Event) => Err(TimeDomainConflict::UndeclaredOverEventSource),

		_ => Ok(()),
	}
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
