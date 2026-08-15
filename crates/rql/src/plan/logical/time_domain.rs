// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::TimeSource;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{Result, ast::ast::AstTimeDeclaration, error::RqlError};

#[derive(Debug, Default, Clone)]
pub enum TimeDeclaration {
	#[default]
	Undeclared,
	None,
	Event {
		column: Fragment,
	},
	Processing,
}

impl From<&AstTimeDeclaration<'_>> for TimeDeclaration {
	fn from(ast: &AstTimeDeclaration<'_>) -> Self {
		match ast {
			AstTimeDeclaration::Undeclared => TimeDeclaration::Undeclared,
			AstTimeDeclaration::None(_) => TimeDeclaration::None,
			AstTimeDeclaration::Event {
				column,
				..
			} => TimeDeclaration::Event {
				column: column.fragment.to_owned(),
			},
			AstTimeDeclaration::Processing(_) => TimeDeclaration::Processing,
		}
	}
}

pub fn resolve_declared_source_time<'a>(
	declaration: &TimeDeclaration,
	columns: impl IntoIterator<Item = (&'a str, ValueType)>,
	row_ttl: Option<Fragment>,
) -> Result<TimeSource> {
	match declaration {
		TimeDeclaration::Undeclared | TimeDeclaration::None => match row_ttl {
			Some(fragment) => Err(RqlError::RowTtlWithoutTimeDomain {
				fragment,
			}
			.into()),
			None => Ok(TimeSource::None),
		},
		TimeDeclaration::Processing => Ok(TimeSource::Processing),
		TimeDeclaration::Event {
			column,
		} => resolve_event_column(column, columns),
	}
}

fn resolve_event_column<'a>(
	column: &Fragment,
	columns: impl IntoIterator<Item = (&'a str, ValueType)>,
) -> Result<TimeSource> {
	let name = column.text();
	let mut available = Vec::new();

	for (candidate, value_type) in columns {
		if candidate == name {
			if value_type != ValueType::DateTime {
				return Err(RqlError::TimePopulatorNotDateTime {
					column: name.to_string(),
					found: value_type,
					fragment: column.clone(),
				}
				.into());
			}
			return Ok(TimeSource::Event {
				ts: name.to_string(),
			});
		}
		available.push(candidate.to_string());
	}

	Err(RqlError::TimePopulatorUnknownColumn {
		column: name.to_string(),
		available,
		fragment: column.clone(),
	}
	.into())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn event(column: &str) -> TimeDeclaration {
		TimeDeclaration::Event {
			column: Fragment::internal(column),
		}
	}

	fn columns() -> Vec<(&'static str, ValueType)> {
		vec![("id", ValueType::Int4), ("block_time", ValueType::DateTime)]
	}

	fn resolve(declaration: &TimeDeclaration) -> Result<TimeSource> {
		resolve_declared_source_time(declaration, columns(), None)
	}

	#[test]
	fn an_undeclared_object_carries_no_time() {
		// This is the whole point of the redesign. The old default was Processing, which stamped the
		// wall clock onto reference rows that had no temporal column, and a July trade joined to
		// such a row came out August. Defaulting to None can only withhold time, never corrupt it.
		assert_eq!(resolve(&TimeDeclaration::Undeclared).unwrap(), TimeSource::None);
	}

	#[test]
	fn declaring_none_explicitly_resolves_the_same_as_silence() {
		// `time: none` exists so an author can state the intent that silence also expresses. If the
		// two ever diverged, reading a declaration would no longer tell you what the object does.
		assert_eq!(resolve(&TimeDeclaration::None).unwrap(), TimeSource::None);
		assert_eq!(resolve(&TimeDeclaration::None).unwrap(), resolve(&TimeDeclaration::Undeclared).unwrap());
	}

	#[test]
	fn processing_still_resolves_to_the_arrival_clock() {
		// Processing is now an opt-in rather than the default, so it has to keep working for the
		// objects that genuinely want ingest time.
		assert_eq!(resolve(&TimeDeclaration::Processing).unwrap(), TimeSource::Processing);
	}

	#[test]
	fn an_event_declaration_carries_its_column_through() {
		assert_eq!(
			resolve(&event("block_time")).unwrap(),
			TimeSource::Event {
				ts: "block_time".to_string()
			}
		);
	}

	#[test]
	fn an_event_column_that_does_not_exist_is_rejected_and_lists_the_candidates() {
		// A populator naming a missing column would stamp nothing, and the author cannot tell which
		// name they fat-fingered without seeing what was actually available.
		let err = resolve(&event("blok_time")).unwrap_err();
		let message = format!("{:?}", err.diagnostic());

		assert!(message.contains("blok_time"), "the diagnostic must name the missing column: {message}");
		assert!(message.contains("block_time"), "the diagnostic must list the candidates: {message}");
	}

	#[test]
	fn an_event_column_of_the_wrong_type_is_rejected() {
		// #time is a timestamp. Accepting an Int4 populator would either panic at the write boundary
		// or silently coerce a row number into an instant.
		let err = resolve(&event("id")).unwrap_err();
		let message = format!("{:?}", err.diagnostic());

		assert!(message.contains("id"), "the diagnostic must name the offending column: {message}");
	}

	#[test]
	fn the_event_error_points_at_the_column_the_author_typed() {
		// The column inside `event(..)` is the token to edit, so the span has to land on it rather
		// than on the `time` key or the whole WITH block.
		let declaration = TimeDeclaration::Event {
			column: Fragment::statement("blok_time", 4, 13),
		};

		let err = resolve_declared_source_time(&declaration, columns(), None).unwrap_err();

		assert_eq!(err.fragment.text(), "blok_time");
		assert_eq!(err.fragment.line().0, 4);
		assert_eq!(err.fragment.column().0, 13);
	}

	#[test]
	fn a_time_less_object_may_not_declare_a_row_ttl() {
		// An object worth a retention policy is worth an explicit clock. Both silence and an explicit
		// `time: none` are rejected, because under the new default they mean the same thing and
		// letting silence through would leave exactly the hole the default was changed to close.
		let ttl = || Some(Fragment::statement("1m", 2, 20));

		for declaration in [TimeDeclaration::Undeclared, TimeDeclaration::None] {
			let err = resolve_declared_source_time(&declaration, columns(), ttl()).unwrap_err();

			assert_eq!(err.fragment.text(), "1m", "the span must land on the ttl to remove");
			assert_eq!(err.fragment.line().0, 2);
		}
	}

	#[test]
	fn an_object_that_carries_time_may_declare_a_row_ttl() {
		// The rejection above must be scoped to time-less objects only. Rejecting every row ttl would
		// break every declaration that pairs retention with a real clock.
		let ttl = || Some(Fragment::statement("1m", 2, 20));

		assert_eq!(
			resolve_declared_source_time(&TimeDeclaration::Processing, columns(), ttl()).unwrap(),
			TimeSource::Processing
		);
		assert_eq!(
			resolve_declared_source_time(&event("block_time"), columns(), ttl()).unwrap(),
			TimeSource::Event {
				ts: "block_time".to_string()
			}
		);
	}
}
