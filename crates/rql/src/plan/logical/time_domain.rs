// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{TimeDomain, TimeSource};
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{Result, ast::ast::AstTimeDeclaration, diagnostic::AstError, error::RqlError};

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

pub fn resolve_declared_source_time<'a>(
	declaration: &TimeDeclaration,
	columns: impl IntoIterator<Item = (&'a str, ValueType)>,
) -> Result<TimeSource> {
	let time = resolve_source_time(declaration)?;

	let TimeSource::Event {
		ts,
	} = &time
	else {
		return Ok(time);
	};

	let fragment = declaration.ts.clone().unwrap_or(Fragment::None);
	let mut available = Vec::new();
	for (name, value_type) in columns {
		if name == ts.as_str() {
			if value_type != ValueType::DateTime {
				return Err(RqlError::TimePopulatorNotDateTime {
					column: ts.clone(),
					found: value_type,
					fragment,
				}
				.into());
			}
			return Ok(time);
		}
		available.push(name.to_string());
	}

	Err(RqlError::TimePopulatorUnknownColumn {
		column: ts.clone(),
		available,
		fragment,
	}
	.into())
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
	fn a_source_declaring_event_without_ts_is_rejected_at_the_time_key() {
		// `time: event` with no populator column describes nothing the engine can act on, and the incomplete
		// half of the declaration is the `time` key the author has to edit.
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
	fn a_source_declaring_processing_with_ts_is_rejected_at_the_ts_key() {
		// A populator the engine would discard reads to the author as event time and runs as processing, so
		// the span has to name the stray `ts` rather than `time`.
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
	fn a_bare_source_is_processing() {
		// Silence on a source is a legitimate declaration, not an omission to fault.
		assert_eq!(resolve_source_time(&declaration(None, None)).unwrap(), TimeSource::Processing);
	}

	#[test]
	fn a_flow_may_never_name_a_ts_column() {
		// A flow's rows can come from several sources with different stamp names, and none survives a
		// projection or aggregate, so a populator only ever names a column on the source. Accepting a
		// flow-level `ts` and ignoring it is the silent trap the two levels exist to keep apart.
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
	fn an_unknown_time_value_is_rejected_at_both_levels() {
		// An unrecognised value must fault at the text the author typed rather than fall back to a default.
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
	fn the_time_value_is_matched_case_insensitively_at_both_levels() {
		// `time: Event` must not be legal in one declaration form and a hard error in the other.
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
	fn the_full_declaration_matrix_is_pinned_for_both_levels() {
		// The two levels have different legal cells - ts is required for an event source and forbidden on
		// every flow - so writing them as two tables side by side is what makes a convergence visible.
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
					panic!(
						"source time={time:?} ts={ts:?} must resolve {want:?}, rejected: {err:?}"
					)
				}
			}
		}

		// The inner Option is what the flow declared, the outer whether it is accepted at all. Registration
		// rejects an undeclared flow over an event-time source and accepts an explicitly processing one.
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
