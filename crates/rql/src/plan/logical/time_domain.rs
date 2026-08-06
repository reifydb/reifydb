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
	fn an_unknown_time_value_is_rejected() {
		// An unrecognised value must fault at the text the author typed rather than fall back to a
		// default, because the default is Processing and silently bucketing by ingest time is the
		// failure this whole declaration exists to prevent.
		let unknown = TimeDeclaration {
			time: Some(Fragment::statement("wallclock", 9, 2)),
			ts: None,
		};

		let err = resolve_source_time(&unknown).unwrap_err();
		assert_eq!(err.fragment.text(), "wallclock");
		assert_eq!(err.fragment.line().0, 9);
	}

	#[test]
	fn the_time_value_is_matched_case_insensitively() {
		// Case is not part of the declaration's meaning, so `time: Event` and `time: event` must
		// resolve identically rather than one of them falling through to the Processing default.
		assert_eq!(resolve_source_time(&declaration(Some("EVENT"), Some("at"))).unwrap(), event("at"));
		assert_eq!(
			resolve_source_time(&declaration(Some("Processing"), None)).unwrap(),
			TimeSource::Processing
		);
	}

	#[test]
	fn the_full_source_declaration_matrix_is_pinned() {
		// Every cell matters because the accepted ones decide what #time a row gets, and #time is now
		// the only clock anything downstream reads. A cell that quietly flips from rejected to
		// Processing is a whole pipeline bucketing by ingest time with nothing to notice it.
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

	}
}
