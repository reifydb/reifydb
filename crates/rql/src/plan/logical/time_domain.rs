// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::TimeDomain;
use reifydb_value::fragment::Fragment;

use crate::{Result, ast::ast::AstTimeDeclaration, diagnostic::AstError};

#[derive(Debug, Default, Clone)]
pub struct TimeDeclaration {
	pub time: Option<Fragment>,
	pub ts: Option<Fragment>,
}

impl TimeDeclaration {
	pub fn ts_column(&self) -> Option<String> {
		self.ts.as_ref().map(|ts| ts.text().to_string())
	}
}

impl From<&AstTimeDeclaration<'_>> for TimeDeclaration {
	fn from(ast: &AstTimeDeclaration<'_>) -> Self {
		Self {
			time: ast.time.as_ref().map(|token| token.fragment.to_owned()),
			ts: ast.ts.as_ref().map(|token| token.fragment.to_owned()),
		}
	}
}

pub fn resolve_time_domain(declaration: &TimeDeclaration) -> Result<TimeDomain> {
	let Some(time) = declaration.time.as_ref() else {
		return Ok(match declaration.ts.as_ref() {
			Some(ts) => TimeDomain::Event {
				ts: ts.text().to_string(),
			},
			None => TimeDomain::Processing,
		});
	};

	match time.text().to_ascii_lowercase().as_str() {
		"event" => {
			let Some(ts) = declaration.ts.as_ref() else {
				return Err(AstError::UnexpectedToken {
					expected: "time: event requires a ts column".to_string(),
					fragment: time.clone(),
				}
				.into());
			};
			Ok(TimeDomain::Event {
				ts: ts.text().to_string(),
			})
		}
		"processing" => {
			if let Some(ts) = declaration.ts.as_ref() {
				return Err(AstError::UnexpectedToken {
					expected: "time: processing must not declare a ts column".to_string(),
					fragment: ts.clone(),
				}
				.into());
			}
			Ok(TimeDomain::Processing)
		}
		_ => Err(AstError::UnexpectedToken {
			expected: "\"event\" or \"processing\"".to_string(),
			fragment: time.clone(),
		}
		.into()),
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

	#[test]
	// Intent: omitting `time` must keep today's implicit behavior - a ts column means event-time.
	fn default_resolves_event_when_ts_present() {
		assert_eq!(
			resolve_time_domain(&declaration(None, Some("window_start"))).unwrap(),
			TimeDomain::Event {
				ts: "window_start".to_string()
			}
		);
	}

	#[test]
	// Intent: no ts and no explicit time means processing-time (wall clock), as before.
	fn default_resolves_processing_without_ts() {
		assert_eq!(resolve_time_domain(&declaration(None, None)).unwrap(), TimeDomain::Processing);
	}

	#[test]
	// Intent: `time: event` is a user error without a ts column, never silently processing.
	fn explicit_event_requires_ts() {
		assert!(resolve_time_domain(&declaration(Some("event"), None)).is_err());
		assert_eq!(
			resolve_time_domain(&declaration(Some("event"), Some("window_start"))).unwrap(),
			TimeDomain::Event {
				ts: "window_start".to_string()
			}
		);
	}

	#[test]
	// Intent: no ts and `time: processing` is the plain processing-time declaration, unchanged.
	fn explicit_processing_without_ts_resolves_processing() {
		assert_eq!(
			resolve_time_domain(&declaration(Some("processing"), None)).unwrap(),
			TimeDomain::Processing
		);
	}

	#[test]
	// Intent: a ts the engine would discard is a silent trap - the author believes they declared
	// event time and got processing. Declaring nothing is a legitimate default; declaring
	// something the engine ignores is not.
	fn explicit_processing_with_ts_is_rejected() {
		assert!(resolve_time_domain(&declaration(Some("processing"), Some("window_start"))).is_err());
	}

	#[test]
	// Intent: an unrecognized time value is rejected, not silently defaulted.
	fn unknown_time_value_is_rejected() {
		assert!(resolve_time_domain(&declaration(Some("wallclock"), None)).is_err());
	}

	#[test]
	// Intent: the declaration is matched case-insensitively, and identically at both levels. The
	// window path lowercased its value before comparing and the flow path did not, which would
	// have made `time: Event` legal in one declaration form and a hard error in the other for no
	// reason the author could see.
	fn the_time_value_is_matched_case_insensitively() {
		assert_eq!(
			resolve_time_domain(&declaration(Some("EVENT"), Some("window_start"))).unwrap(),
			TimeDomain::Event {
				ts: "window_start".to_string()
			}
		);
		assert_eq!(
			resolve_time_domain(&declaration(Some("Processing"), None)).unwrap(),
			TimeDomain::Processing
		);
	}

	#[test]
	// Intent: every rejection must point at the span the author has to edit, and at the RIGHT one
	// of the two keys. A diagnostic carrying Fragment::None renders as "got " with an empty value
	// and leaves the author hunting for which key is wrong - which is what this resolver produced
	// while it took bare strings instead of fragments.
	fn each_rejection_points_at_the_key_the_author_must_edit() {
		let event_without_ts = TimeDeclaration {
			time: Some(Fragment::statement("event", 3, 11)),
			ts: None,
		};
		let err = resolve_time_domain(&event_without_ts).unwrap_err();
		assert_eq!(err.fragment.text(), "event", "the incomplete `time` key is the one to edit");
		assert_eq!(err.fragment.line().0, 3);
		assert_eq!(err.fragment.column().0, 11);

		let processing_with_ts = TimeDeclaration {
			time: Some(Fragment::statement("processing", 3, 11)),
			ts: Some(Fragment::statement("block_time", 4, 7)),
		};
		let err = resolve_time_domain(&processing_with_ts).unwrap_err();
		assert_eq!(err.fragment.text(), "block_time", "the stray `ts` is the one to remove, not `time`");
		assert_eq!(err.fragment.line().0, 4);
		assert_eq!(err.fragment.column().0, 7);

		let unknown = TimeDeclaration {
			time: Some(Fragment::statement("wallclock", 9, 2)),
			ts: None,
		};
		let err = resolve_time_domain(&unknown).unwrap_err();
		assert_eq!(err.fragment.text(), "wallclock");
		assert_eq!(err.fragment.line().0, 9);
	}

	#[test]
	// Intent: pin the whole input space, not just the interesting corners. This resolution is
	// shared by the flow level and the window level precisely so the two cannot drift, which
	// makes an accidental change to any single cell a change to both levels at once. The table
	// is the contract; a new arm added without a decision here shows up as an unhandled case.
	fn the_full_declaration_matrix_is_pinned() {
		let expected: [((Option<&str>, Option<&str>), Option<TimeDomain>); 8] = [
			((None, None), Some(TimeDomain::Processing)),
			(
				(None, Some("window_start")),
				Some(TimeDomain::Event {
					ts: "window_start".to_string(),
				}),
			),
			((Some("event"), None), None),
			(
				(Some("event"), Some("window_start")),
				Some(TimeDomain::Event {
					ts: "window_start".to_string(),
				}),
			),
			((Some("processing"), None), Some(TimeDomain::Processing)),
			((Some("processing"), Some("window_start")), None),
			((Some("wallclock"), None), None),
			((Some("wallclock"), Some("window_start")), None),
		];

		for ((time, ts), want) in expected {
			match (resolve_time_domain(&declaration(time, ts)), want) {
				(Ok(got), Some(want)) => assert_eq!(got, want, "time={time:?} ts={ts:?}"),
				(Err(_), None) => {}
				(Ok(got), None) => {
					panic!("time={time:?} ts={ts:?} must be rejected, resolved {got:?}")
				}
				(Err(err), Some(want)) => {
					panic!("time={time:?} ts={ts:?} must resolve {want:?}, rejected: {err:?}")
				}
			}
		}
	}
}
