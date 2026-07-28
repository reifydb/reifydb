// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{TimeDomain, TimeSource};
use reifydb_value::fragment::Fragment;

use crate::{Result, ast::ast::AstTimeDeclaration, diagnostic::AstError};

#[derive(Debug, Default, Clone)]
pub struct TimeDeclaration {
	pub time: Option<Fragment>,
	pub ts: Option<Fragment>,
}

impl From<&AstTimeDeclaration<'_>> for TimeDeclaration {
	fn from(ast: &AstTimeDeclaration<'_>) -> Self {
		Self {
			time: ast.time.as_ref().map(|token| token.fragment.to_owned()),
			ts: ast.ts.as_ref().map(|token| token.fragment.to_owned()),
		}
	}
}

fn match_domain(time: &Fragment) -> Result<TimeDomain> {
	match time.text().to_ascii_lowercase().as_str() {
		"event" => Ok(TimeDomain::Event),
		"processing" => Ok(TimeDomain::Processing),
		_ => Err(AstError::UnexpectedToken {
			expected: "\"event\" or \"processing\"".to_string(),
			fragment: time.clone(),
		}
		.into()),
	}
}

pub fn resolve_source_time(declaration: &TimeDeclaration) -> Result<TimeSource> {
	let Some(time) = declaration.time.as_ref() else {
		return Ok(match declaration.ts.as_ref() {
			Some(ts) => TimeSource::Event {
				ts: ts.text().to_string(),
			},
			None => TimeSource::Processing,
		});
	};

	match match_domain(time)? {
		TimeDomain::Event => {
			let Some(ts) = declaration.ts.as_ref() else {
				return Err(AstError::UnexpectedToken {
					expected: "time: event requires a ts column".to_string(),
					fragment: time.clone(),
				}
				.into());
			};
			Ok(TimeSource::Event {
				ts: ts.text().to_string(),
			})
		}
		TimeDomain::Processing => {
			if let Some(ts) = declaration.ts.as_ref() {
				return Err(AstError::UnexpectedToken {
					expected: "time: processing must not declare a ts column".to_string(),
					fragment: ts.clone(),
				}
				.into());
			}
			Ok(TimeSource::Processing)
		}
	}
}

pub fn resolve_flow_time(declaration: &TimeDeclaration) -> Result<Option<TimeDomain>> {
	if let Some(ts) = declaration.ts.as_ref() {
		return Err(AstError::UnexpectedToken {
			expected: "a flow declares a time domain, not a ts column; declare `ts` on the source object"
				.to_string(),
			fragment: ts.clone(),
		}
		.into());
	}

	match declaration.time.as_ref() {
		Some(time) => match_domain(time).map(Some),
		None => Ok(None),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn declaration(time: Option<&str>, ts: Option<&str>) -> TimeDeclaration {
		TimeDeclaration {
			time: time.map(Fragment::internal),
			ts: ts.map(Fragment::internal),
		}
	}

	fn event(ts: &str) -> TimeSource {
		TimeSource::Event {
			ts: ts.to_string(),
		}
	}

	#[test]
	// Intent: a source object names the column #time is populated from, so `time: event` without
	// one describes nothing the engine can act on. It must fail at the `time` key, which is the
	// incomplete half of the declaration.
	fn a_source_declaring_event_without_ts_is_rejected_at_the_time_key() {
		let err = resolve_source_time(&TimeDeclaration {
			time: Some(Fragment::statement("event", 3, 11)),
			ts: None,
		})
		.unwrap_err();

		assert_eq!(err.fragment.text(), "event", "the incomplete `time` key is the one to edit");
		assert_eq!(err.fragment.line().0, 3);
		assert_eq!(err.fragment.column().0, 11);
	}

	#[test]
	// Intent: a populator the engine would discard is the silent-trap class this whole redesign
	// exists to kill - the author believes they declared event time and got processing. The span
	// must point at the stray `ts`, the key to remove, not at `time`.
	fn a_source_declaring_processing_with_ts_is_rejected_at_the_ts_key() {
		let err = resolve_source_time(&TimeDeclaration {
			time: Some(Fragment::statement("processing", 3, 11)),
			ts: Some(Fragment::statement("block_time", 4, 7)),
		})
		.unwrap_err();

		assert_eq!(err.fragment.text(), "block_time", "the stray `ts` is the one to remove, not `time`");
		assert_eq!(err.fragment.line().0, 4);
		assert_eq!(err.fragment.column().0, 7);
	}

	#[test]
	// Intent: silence on a source object is a legitimate default and means processing time.
	fn a_bare_source_is_processing() {
		assert_eq!(resolve_source_time(&declaration(None, None)).unwrap(), TimeSource::Processing);
	}

	#[test]
	// Intent: THE divergence between the two levels. A flow declares which domain it operates in
	// and never names a populator - that lives on the source, because a flow's rows may come from
	// several sources with different stamp names, and after a projection or an aggregate no input
	// column survives to be named. Accepting-and-ignoring a flow-level `ts` is exactly the silent
	// trap the split exists to prevent. Mutation: drop this guard and return the domain anyway;
	// this fails while every source-level test still passes, which is the divergence being caught.
	fn a_flow_may_never_name_a_ts_column() {
		let err = resolve_flow_time(&TimeDeclaration {
			time: Some(Fragment::statement("event", 3, 11)),
			ts: Some(Fragment::statement("block_time", 4, 7)),
		})
		.unwrap_err();
		assert_eq!(err.fragment.text(), "block_time", "the flow-level `ts` is the key to delete");

		let err = resolve_flow_time(&declaration(None, Some("block_time"))).unwrap_err();
		assert!(
			err.fragment.text() == "block_time",
			"a bare `ts` must be rejected at the flow level too, not silently read as event time"
		);
	}

	#[test]
	// Intent: an unrecognized time value is rejected at both levels rather than silently
	// defaulted, and points at the value the author typed.
	fn an_unknown_time_value_is_rejected_at_both_levels() {
		let unknown = TimeDeclaration {
			time: Some(Fragment::statement("wallclock", 9, 2)),
			ts: None,
		};

		let err = resolve_source_time(&unknown).unwrap_err();
		assert_eq!(err.fragment.text(), "wallclock");
		assert_eq!(err.fragment.line().0, 9);

		let err = resolve_flow_time(&unknown).unwrap_err();
		assert_eq!(err.fragment.text(), "wallclock");
	}

	#[test]
	// Intent: the declaration is matched case-insensitively, and identically at both levels, so
	// `time: Event` cannot be legal in one declaration form and a hard error in the other for no
	// reason the author can see.
	fn the_time_value_is_matched_case_insensitively_at_both_levels() {
		assert_eq!(resolve_source_time(&declaration(Some("EVENT"), Some("at"))).unwrap(), event("at"));
		assert_eq!(
			resolve_source_time(&declaration(Some("Processing"), None)).unwrap(),
			TimeSource::Processing
		);
		assert_eq!(resolve_flow_time(&declaration(Some("EVENT"), None)).unwrap(), Some(TimeDomain::Event));
		assert_eq!(
			resolve_flow_time(&declaration(Some("Processing"), None)).unwrap(),
			Some(TimeDomain::Processing)
		);
	}

	#[test]
	// Intent: pin the whole input space for BOTH wrappers side by side. The two levels answer
	// different questions and therefore have different legal cells - the ts column is required
	// for an event source and forbidden on every flow - and writing them as one table is what
	// makes an accidental convergence visible. A single shared matrix would hide exactly the
	// drift this split was introduced to create.
	fn the_full_declaration_matrix_is_pinned_for_both_levels() {
		let source: [((Option<&str>, Option<&str>), Option<TimeSource>); 8] = [
			((None, None), Some(TimeSource::Processing)),
			((None, Some("at")), Some(event("at"))),
			((Some("event"), None), None),
			((Some("event"), Some("at")), Some(event("at"))),
			((Some("processing"), None), Some(TimeSource::Processing)),
			((Some("processing"), Some("at")), None),
			((Some("wallclock"), None), None),
			((Some("wallclock"), Some("at")), None),
		];

		for ((time, ts), want) in source {
			match (resolve_source_time(&declaration(time, ts)), want) {
				(Ok(got), Some(want)) => assert_eq!(got, want, "source time={time:?} ts={ts:?}"),
				(Err(_), None) => {}
				(Ok(got), None) => {
					panic!("source time={time:?} ts={ts:?} must be rejected, resolved {got:?}")
				}
				(Err(err), Some(want)) => {
					panic!("source time={time:?} ts={ts:?} must resolve {want:?}, rejected: {err:?}")
				}
			}
		}

		// The inner Option is what the flow DECLARED; the outer is whether it is accepted at all.
		// An undeclared flow resolves to Ok(None), which is distinct from a flow that explicitly
		// declared processing - registration rejects the former over an event-time source and
		// accepts the latter, so collapsing the two would lose the whole point of F3.
		let flow: [((Option<&str>, Option<&str>), Option<Option<TimeDomain>>); 8] = [
			((None, None), Some(None)),
			((None, Some("at")), None),
			((Some("event"), None), Some(Some(TimeDomain::Event))),
			((Some("event"), Some("at")), None),
			((Some("processing"), None), Some(Some(TimeDomain::Processing))),
			((Some("processing"), Some("at")), None),
			((Some("wallclock"), None), None),
			((Some("wallclock"), Some("at")), None),
		];

		for ((time, ts), want) in flow {
			match (resolve_flow_time(&declaration(time, ts)), want) {
				(Ok(got), Some(want)) => assert_eq!(got, want, "flow time={time:?} ts={ts:?}"),
				(Err(_), None) => {}
				(Ok(got), None) => {
					panic!("flow time={time:?} ts={ts:?} must be rejected, resolved {got:?}")
				}
				(Err(err), Some(want)) => {
					panic!("flow time={time:?} ts={ts:?} must resolve {want:?}, rejected: {err:?}")
				}
			}
		}
	}
}

