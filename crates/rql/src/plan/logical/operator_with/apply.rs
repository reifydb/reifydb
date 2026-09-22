// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::WindowSize,
	operator_with::{ApplyWith, WithSpan},
};
use reifydb_value::{fragment::Fragment, value::duration::Duration};

use crate::{
	Result,
	ast::ast::{AstOperatorWith, AstOperatorWithEntry, AstOperatorWithValue, AstWindowKind},
	diagnostic::AstError,
	duration::DurationBound,
	error::RqlError,
	plan::logical::{
		Compiler,
		operator_with::{
			Declared, declared_count, declared_duration, entries, is_duration, is_number, literal,
			literal_boolean, reject_immutable_not_smaller_than_lateness, unknown_key, window::ParsedConfig,
		},
	},
	token::token::Token,
};

const APPLY_WITH_KEYS: &str = "window, duration, slots, slide, gap, lag, pane, lateness, retention, or immutable";

enum Immutable {
	Zero(Declared<()>),
	Span(Declared<WithSpan>),
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_apply_with(with: Option<&AstOperatorWith<'bump>>) -> Result<ApplyWith> {
		let mut lateness: Option<Declared<WithSpan>> = None;
		let mut retention: Option<Declared<Duration>> = None;
		let mut immutable: Option<Immutable> = None;
		let mut window_kind: Option<AstWindowKind> = None;
		let mut parsed = ParsedConfig::default();
		let mut size_keys_seen: Vec<(&'static str, Fragment)> = Vec::new();
		let mut immutable_key: Option<Fragment> = None;

		for entry in entries(with) {
			match entry.key.word() {
				Some("window") => {
					let (kind, fragment) = match entry.value.as_ref() {
						Some(AstOperatorWithValue::Word(token)) => {
							match token.fragment.text().to_lowercase().as_str() {
								"tumbling" => (
									AstWindowKind::Tumbling,
									token.fragment.to_owned(),
								),
								"sliding" => (
									AstWindowKind::Sliding,
									token.fragment.to_owned(),
								),
								"rolling" => (
									AstWindowKind::Rolling,
									token.fragment.to_owned(),
								),
								"session" => (
									AstWindowKind::Session,
									token.fragment.to_owned(),
								),
								_ => {
									return Err(AstError::UnexpectedToken {
										expected: "tumbling, sliding, rolling or session"
											.to_string(),
										fragment: token.fragment.to_owned(),
									}
									.into());
								}
							}
						}
						Some(AstOperatorWithValue::Literal(token)) => {
							return Err(AstError::UnexpectedToken {
								expected: "tumbling, sliding, rolling or session"
									.to_string(),
								fragment: token.fragment.to_owned(),
							}
							.into());
						}
						Some(AstOperatorWithValue::Block(_)) | None => {
							return Err(AstError::UnexpectedToken {
								expected: "tumbling, sliding, rolling or session"
									.to_string(),
								fragment: entry.key.fragment(),
							}
							.into());
						}
					};
					window_kind = Some(kind);
					parsed.window = fragment;
				}
				Some("slots") => {
					parsed.count = Some(declared_count(literal(entry)?)?);
					size_keys_seen.push(("slots", entry.key.fragment()));
				}
				Some("duration") => {
					Self::parse_config_item(entry, &mut parsed)?;
					size_keys_seen.push(("duration", entry.key.fragment()));
				}
				Some("slide") => {
					Self::parse_config_item(entry, &mut parsed)?;
					size_keys_seen.push(("slide", entry.key.fragment()));
				}
				Some("gap") => {
					Self::parse_config_item(entry, &mut parsed)?;
					size_keys_seen.push(("gap", entry.key.fragment()));
				}
				Some("lag") => {
					Self::parse_config_item(entry, &mut parsed)?;
					size_keys_seen.push(("lag", entry.key.fragment()));
				}
				Some("pane") => {
					parsed.pane = Some(declared_duration(
						literal(entry)?,
						"'pane'",
						DurationBound::Positive,
					)?);
					size_keys_seen.push(("pane", entry.key.fragment()));
				}
				Some("lateness") => lateness = Some(declared_span(literal(entry)?, "'lateness'")?),
				Some("retention") => {
					retention = Some(declared_duration(
						literal(entry)?,
						"'retention'",
						DurationBound::AllowZero,
					)?)
				}
				Some("immutable") => {
					immutable_key = Some(entry.key.fragment());
					immutable = declared_immutable(entry)?;
				}
				_ => return Err(unknown_key(entry, APPLY_WITH_KEYS)),
			}
		}

		let immutable = immutable.map(|immutable| match immutable {
			Immutable::Span(span) => span,
			Immutable::Zero(zero) => Declared {
				value: if parsed.count.is_some() {
					WithSpan::Count(0)
				} else if parsed.duration.is_some() {
					WithSpan::Duration(Duration::zero())
				} else {
					match lateness.as_ref().map(|lateness| lateness.value) {
						Some(WithSpan::Count(_)) => WithSpan::Count(0),
						_ => WithSpan::Duration(Duration::zero()),
					}
				},
				fragment: zero.fragment,
			},
		});

		reject_immutable_not_smaller_than_lateness_in_its_unit(immutable.as_ref(), lateness.as_ref())?;
		reject_retention_below_lateness(retention.as_ref(), lateness.as_ref())?;

		if window_kind.is_none() {
			if let Some((_, fragment)) = size_keys_seen.first() {
				return Err(AstError::UnexpectedToken {
					expected: "window before duration, slots, slide, gap or lag".to_string(),
					fragment: fragment.clone(),
				}
				.into());
			}
		}

		let kind = match window_kind {
			None => None,
			Some(ast_kind) => {
				let kind_name = match ast_kind {
					AstWindowKind::Tumbling => "tumbling",
					AstWindowKind::Sliding => "sliding",
					AstWindowKind::Rolling => "rolling",
					AstWindowKind::Session => "session",
				};
				for (key_name, fragment) in &size_keys_seen {
					if !window_reads(ast_kind, key_name) {
						return Err(AstError::UnexpectedToken {
							expected: format!("a key the {} window reads", kind_name),
							fragment: fragment.clone(),
						}
						.into());
					}
				}
				if matches!(ast_kind, AstWindowKind::Rolling)
					&& let Some(fragment) = &immutable_key
				{
					return Err(AstError::UnexpectedToken {
						expected: format!("a key the {} window reads", kind_name),
						fragment: fragment.clone(),
					}
					.into());
				}

				let kind = Self::build_window_kind(ast_kind, &parsed)?;

				let size_is_count = matches!(kind.size(), Some(WindowSize::Count(_)));
				if let Some(lateness) = &lateness
					&& matches!(lateness.value, WithSpan::Count(_)) != size_is_count
				{
					return Err(AstError::UnexpectedToken {
						expected: "lateness in the unit of the window size".to_string(),
						fragment: lateness.fragment.clone(),
					}
					.into());
				}
				if let Some(immutable) = &immutable
					&& matches!(immutable.value, WithSpan::Count(_)) != size_is_count
				{
					return Err(AstError::UnexpectedToken {
						expected: "lateness in the unit of the window size".to_string(),
						fragment: immutable.fragment.clone(),
					}
					.into());
				}

				match kind.size() {
					Some(WindowSize::Count(slots)) => {
						if let Some(immutable) = &immutable
							&& let WithSpan::Count(imm_count) = immutable.value
							&& imm_count >= *slots
						{
							return Err(RqlError::WindowImmutableNotSmallerThanWindow {
								immutable_value: immutable.fragment.text().to_string(),
								window_value: parsed
									.count
									.as_ref()
									.map(|declared| {
										declared.fragment.text().to_string()
									})
									.unwrap_or_default(),
								fragment: immutable.fragment.clone(),
							}
							.into());
						}
					}
					_ => {
						if let Some(immutable) = &immutable
							&& let WithSpan::Duration(duration) = immutable.value
						{
							parsed.immutable = Some(Declared {
								value: duration,
								fragment: immutable.fragment.clone(),
							});
						}
						Self::reject_immutable_not_smaller_than_window(&parsed, &kind)?;
					}
				}

				Some(kind)
			}
		};

		Ok(ApplyWith {
			window: kind,
			lateness: Declared::value_of(&lateness),
			immutable: Declared::value_of(&immutable),
			retention: Declared::value_of(&retention),
		})
	}
}

fn window_reads(kind: AstWindowKind, key: &str) -> bool {
	match kind {
		AstWindowKind::Tumbling => matches!(key, "duration" | "slots"),
		AstWindowKind::Sliding => matches!(key, "duration" | "slots" | "slide"),
		AstWindowKind::Rolling => matches!(key, "duration" | "slots" | "pane"),
		AstWindowKind::Session => matches!(key, "gap"),
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

fn reject_retention_below_lateness(
	retention: Option<&Declared<Duration>>,
	lateness: Option<&Declared<WithSpan>>,
) -> Result<()> {
	let (Some(retention), Some(lateness)) = (retention, lateness) else {
		return Ok(());
	};
	match lateness.value {
		WithSpan::Duration(lateness_value) if retention.value < lateness_value => {
			Err(AstError::UnexpectedToken {
				expected: "a retention of at least the lateness".to_string(),
				fragment: retention.fragment.clone(),
			}
			.into())
		}
		_ => Ok(()),
	}
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
	use reifydb_core::{
		common::{WindowKind, WindowSize},
		operator_with::{ApplyWith, WithSpan},
	};
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
				window: None,
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
		// A retention read as a count would bound managed state by rows, which the reclaim cannot measure.
		assert_eq!(
			apply_with("apply op { } with { lateness: 30s, retention: 1h }").unwrap(),
			ApplyWith {
				window: None,
				lateness: seconds(30),
				immutable: None,
				retention: Some(Duration::from_hours(1).unwrap()),
			}
		);
		assert_eq!(
			apply_with("apply op { } with { retention: 0s }").unwrap().retention,
			Some(Duration::zero())
		);
	}

	#[test]
	fn a_count_retention_is_rejected() {
		// A bare number has no unit the reclaim can add to a write time.
		assert!(apply_with("apply op { } with { retention: 150 }").is_err());
	}

	#[test]
	fn retention_below_lateness_is_rejected() {
		// A retention under the lateness frees a group while a timer inside the hold can still fire for it.
		assert!(apply_with("apply op { } with { lateness: 30s, retention: 10s }").is_err());
		assert!(apply_with("apply op { } with { lateness: 30s, retention: 30s }").is_ok());
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
		// A zero duration next to a count lateness would fail the unit check for a boolean flag.
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

	#[test]
	fn a_tumbling_window_parses_into_apply_with() {
		assert_eq!(
			apply_with("apply op { } with { window: tumbling, duration: 1m, lateness: 30s }")
				.unwrap()
				.window,
			Some(WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_minutes(1).unwrap()),
			})
		);
	}

	#[test]
	fn a_window_key_without_window_fails() {
		// A size key with no window has nothing to build a WindowKind against.
		for source in [
			"apply op { } with { duration: 1m }",
			"apply op { } with { slots: 1 }",
			"apply op { } with { slide: 30s }",
			"apply op { } with { gap: 1m }",
			"apply op { } with { lag: 30s }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn a_unit_mix_fails() {
		for source in [
			"apply op { } with { window: tumbling, slots: 1, lateness: 30s }",
			"apply op { } with { window: tumbling, duration: 1m, lateness: 150 }",
			"apply op { } with { window: tumbling, duration: 1m, immutable: 150 }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn immutable_not_smaller_than_the_window_fails() {
		assert!(apply_with("apply op { } with { window: tumbling, duration: 1m, immutable: 1m }").is_err());
	}

	#[test]
	fn slot_immutable_not_smaller_than_the_window_fails() {
		assert!(apply_with("apply op { } with { window: tumbling, slots: 4, immutable: 4 }").is_err());
	}

	#[test]
	fn a_key_the_kind_ignores_fails() {
		for source in [
			"apply op { } with { window: tumbling, duration: 1m, slide: 30s }",
			"apply op { } with { window: tumbling, duration: 1m, gap: 30s }",
			"apply op { } with { window: session, gap: 1m, duration: 30s }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn immutable_true_takes_the_window_size_unit() {
		assert_eq!(
			apply_with("apply op { } with { window: tumbling, duration: 1m, immutable: true }")
				.unwrap()
				.immutable,
			seconds(0)
		);
		assert_eq!(
			apply_with("apply op { } with { window: tumbling, slots: 4, immutable: true }")
				.unwrap()
				.immutable,
			Some(WithSpan::Count(0))
		);
	}

	#[test]
	fn pane_parses_on_a_rolling_window() {
		assert_eq!(
			apply_with("apply op { } with { window: rolling, duration: 1h, pane: 1s }").unwrap().window,
			Some(WindowKind::Rolling {
				size: WindowSize::Duration(Duration::from_hours(1).unwrap()),
				lag: None,
				pane: Some(Duration::from_seconds(1).unwrap()),
			})
		);
	}

	#[test]
	fn pane_on_a_window_that_is_not_rolling_fails() {
		// A pane the kind never reads is a setting the author believes is in force.
		for source in [
			"apply op { } with { window: tumbling, duration: 1m, pane: 1s }",
			"apply op { } with { window: sliding, duration: 1m, slide: 30s, pane: 1s }",
			"apply op { } with { window: session, gap: 1m, pane: 1s }",
			"apply op { } with { pane: 1s }",
		] {
			assert!(apply_with(source).is_err(), "must be rejected: {source}");
		}
	}

	#[test]
	fn pane_on_a_slot_sized_rolling_window_fails() {
		// A pane is a time width; a slot count has no width to divide.
		assert!(apply_with("apply op { } with { window: rolling, slots: 4, pane: 1s }").is_err());
	}

	#[test]
	fn a_zero_pane_fails() {
		// A zero-width pane would need unbounded panes to cover the window.
		assert!(apply_with("apply op { } with { window: rolling, duration: 1h, pane: 0s }").is_err());
	}

	#[test]
	fn pane_wider_than_immutable_fails() {
		// No rolling driver reads immutable, so accepting it leaves a setting that silently does nothing.
		let err = apply_with("apply op { } with { window: rolling, duration: 1h, pane: 10s, immutable: 1s }")
			.expect_err("must be rejected")
			.to_string();
		assert!(err.contains("expected a key the rolling window reads, got immutable"), "got: {err}");
	}

	#[test]
	fn lag_on_a_rolling_window_fails() {
		// No rolling driver shifts its output by lag, so accepting it leaves a setting that silently does
		// nothing.
		let err = apply_with("apply op { } with { window: rolling, duration: 1h, pane: 1s, lag: 30s }")
			.expect_err("must be rejected")
			.to_string();
		assert!(err.contains("expected a key the rolling window reads, got lag"), "got: {err}");
	}
}
