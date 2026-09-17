// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::operator_with::{ApplyWith, WithSpan};
use reifydb_value::value::duration::Duration;

use crate::{
	Result,
	ast::ast::{AstOperatorWith, AstOperatorWithEntry},
	diagnostic::AstError,
	duration::{DurationBound, compile_duration},
	plan::logical::{
		Compiler,
		operator_with::{
			Declared, declared_count, declared_duration, entries, is_duration, is_number, literal,
			literal_boolean, reject_immutable_not_smaller_than_lateness, unknown_key,
		},
	},
	token::token::Token,
};

const APPLY_WITH_KEYS: &str = "lateness, immutable, or retention";

enum Immutable {
	Zero(Declared<()>),
	Span(Declared<WithSpan>),
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_apply_with(with: Option<&AstOperatorWith<'bump>>) -> Result<ApplyWith> {
		let mut lateness: Option<Declared<WithSpan>> = None;
		let mut immutable: Option<Immutable> = None;
		let mut retention: Option<Duration> = None;

		for entry in entries(with) {
			match entry.key.word() {
				Some("lateness") => lateness = Some(declared_span(literal(entry)?, "'lateness'")?),
				Some("immutable") => immutable = declared_immutable(entry)?,
				Some("retention") => {
					retention = Some(compile_duration(
						literal(entry)?,
						DurationBound::Positive,
						"a retention",
					)?);
				}
				_ => return Err(unknown_key(entry, APPLY_WITH_KEYS)),
			}
		}

		let immutable = immutable.map(|immutable| match immutable {
			Immutable::Span(span) => span,
			Immutable::Zero(zero) => Declared {
				value: match lateness.as_ref().map(|lateness| lateness.value) {
					Some(WithSpan::Count(_)) => WithSpan::Count(0),
					_ => WithSpan::Duration(Duration::zero()),
				},
				fragment: zero.fragment,
			},
		});

		reject_immutable_not_smaller_than_lateness_in_its_unit(immutable.as_ref(), lateness.as_ref())?;

		Ok(ApplyWith {
			lateness: Declared::value_of(&lateness),
			immutable: Declared::value_of(&immutable),
			retention,
		})
	}
}

fn declared_span(token: &Token<'_>, key: &str) -> Result<Declared<WithSpan>> {
	if is_duration(token) {
		let declared = declared_duration(token, key, DurationBound::AllowZero)?;
		return Ok(Declared {
			value: WithSpan::Duration(declared.value),
			fragment: declared.fragment,
		});
	}
	if is_number(token) {
		let declared = declared_count(token)?;
		return Ok(Declared {
			value: WithSpan::Count(declared.value),
			fragment: declared.fragment,
		});
	}
	Err(AstError::UnexpectedToken {
		expected: format!("a duration such as `30s` or a row count such as `150` for {}", key),
		fragment: token.fragment.to_owned(),
	}
	.into())
}

fn declared_immutable(entry: &AstOperatorWithEntry<'_>) -> Result<Option<Immutable>> {
	let token = literal(entry)?;
	Ok(match literal_boolean(token) {
		Some(true) => Some(Immutable::Zero(Declared {
			value: (),
			fragment: token.fragment.to_owned(),
		})),
		Some(false) => None,
		None => Some(Immutable::Span(declared_span(token, "'immutable'")?)),
	})
}

fn reject_immutable_not_smaller_than_lateness_in_its_unit(
	immutable: Option<&Declared<WithSpan>>,
	lateness: Option<&Declared<WithSpan>>,
) -> Result<()> {
	let (Some(immutable), Some(lateness)) = (immutable, lateness) else {
		return Ok(());
	};
	match (immutable.value, lateness.value) {
		(WithSpan::Duration(immutable_value), WithSpan::Duration(lateness_value)) => {
			reject_immutable_not_smaller_than_lateness(
				Some(&retag(immutable, immutable_value)),
				Some(&retag(lateness, lateness_value)),
			)
		}
		(WithSpan::Count(immutable_value), WithSpan::Count(lateness_value)) => {
			reject_immutable_not_smaller_than_lateness(
				Some(&retag(immutable, immutable_value)),
				Some(&retag(lateness, lateness_value)),
			)
		}
		_ => Err(AstError::UnexpectedToken {
			expected: "immutable in the same unit as lateness".to_string(),
			fragment: immutable.fragment.clone(),
		}
		.into()),
	}
}

fn retag<T>(declared: &Declared<WithSpan>, value: T) -> Declared<T> {
	Declared {
		value,
		fragment: declared.fragment.clone(),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::operator_with::{ApplyWith, WithSpan};
	use reifydb_value::value::duration::Duration;

	use crate::{Result, ast::parse_str, bump::Bump, plan::logical::Compiler};

	fn apply_with(source: &str) -> Result<ApplyWith> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source)?;
		Compiler::compile_apply_with(statements[0].first_unchecked().as_apply().with.as_ref())
	}

	fn seconds(n: i64) -> Option<WithSpan> {
		Some(WithSpan::Duration(Duration::from_seconds(n).unwrap()))
	}

	#[test]
	fn lateness_and_immutable_parse_as_durations() {
		assert_eq!(
			apply_with("apply op { } with { lateness: 30s, immutable: 10s }").unwrap(),
			ApplyWith {
				lateness: seconds(30),
				immutable: seconds(10),
				retention: None,
			}
		);
	}

	#[test]
	fn an_integer_lateness_parses_as_a_row_count() {
		// A count read as a duration would seal after 150 nanoseconds instead of 150 rows.
		assert_eq!(
			apply_with("apply op { } with { lateness: 150 }").unwrap().lateness,
			Some(WithSpan::Count(150))
		);
	}

	#[test]
	fn retention_parses_as_a_duration() {
		assert_eq!(
			apply_with("apply op { } with { retention: 1h }").unwrap().retention,
			Some(Duration::from_hours(1).unwrap())
		);
	}

	#[test]
	fn keys_nothing_reads_are_rejected() {
		// An accepted key that nothing reads is a setting the author believes is in force.
		for source in [
			"apply op { } with { ttl: 1h }",
			"apply op { } with { duration: 1m }",
			"apply op { } with { window: 1m }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn a_zero_retention_is_rejected() {
		// A zero retention frees state on the write that created it.
		assert!(apply_with("apply op { } with { retention: 0s }").is_err());
	}

	#[test]
	fn immutable_not_smaller_than_lateness_is_rejected() {
		for source in [
			"apply op { } with { lateness: 30s, immutable: 30s }",
			"apply op { } with { lateness: 30s, immutable: 40s }",
			"apply op { } with { lateness: 150, immutable: 150 }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn immutable_in_a_different_unit_than_lateness_is_rejected() {
		// A duration and a count cannot be ordered, so the immutable bound could never be checked.
		for source in [
			"apply op { } with { lateness: 30s, immutable: 10 }",
			"apply op { } with { lateness: 150, immutable: 10s }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn immutable_true_reads_as_zero_in_the_unit_of_lateness() {
		// A zero duration next to a count lateness would fail the unit check for a knob the author wrote as a
		// flag.
		assert_eq!(
			apply_with("apply op { } with { lateness: 30s, immutable: true }").unwrap().immutable,
			seconds(0)
		);
		assert_eq!(
			apply_with("apply op { } with { lateness: 150, immutable: true }").unwrap().immutable,
			Some(WithSpan::Count(0))
		);
		assert_eq!(apply_with("apply op { } with { immutable: true }").unwrap().immutable, seconds(0));
	}

	#[test]
	fn immutable_false_reads_as_absent() {
		assert_eq!(
			apply_with("apply op { } with { lateness: 30s, immutable: false }").unwrap().immutable,
			None
		);
	}

	#[test]
	fn no_with_block_leaves_every_setting_absent() {
		assert_eq!(apply_with("apply op { }").unwrap(), ApplyWith::default());
	}
}
