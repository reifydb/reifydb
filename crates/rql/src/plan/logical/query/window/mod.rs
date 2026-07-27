// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{TimeDomain, WindowKind, WindowSize};
use reifydb_value::{
	fragment::Fragment,
	value::{duration::Duration, number::parse::parse_primitive_int, temporal::parse::duration::parse_duration},
};

use crate::{
	Result,
	ast::ast::{
		Ast,
		Ast::Literal,
		AstLiteral::{Number, Text},
		AstWindow, AstWindowConfig, AstWindowKind,
	},
	bump::BumpFragment,
	diagnostic::AstError,
	expression::{Expression, ExpressionCompiler},
	plan::logical::{
		Compiler, LogicalPlan,
		time_domain::{TimeDeclaration, resolve_time_domain},
	},
};

#[derive(Debug, Clone)]
struct Declared<T> {
	pub value: T,
	pub fragment: Fragment,
}

impl<T: Copy> Declared<T> {
	fn value_of(declared: &Option<Self>) -> Option<T> {
		declared.as_ref().map(|declared| declared.value)
	}
}

#[derive(Debug, Default)]
struct ParsedConfig {
	pub interval: Option<Declared<Duration>>,
	pub count: Option<Declared<u64>>,
	pub slide_duration: Option<Declared<Duration>>,
	pub slide_count: Option<Declared<u64>>,
	pub gap: Option<Declared<Duration>>,
	pub lag: Option<Declared<Duration>>,
	pub grace: Option<Declared<Duration>>,
	pub lateness: Option<Declared<Duration>>,
	pub time_declaration: TimeDeclaration,
	pub window: Fragment,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub grace: Duration,
	pub lateness: Duration,
	pub rql: String,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_window(&self, ast: AstWindow<'bump>) -> Result<LogicalPlan<'bump>> {
		let rql = ast.rql.to_string();

		let parsed = Self::parse_config(&ast.config, ast.token.fragment.to_owned())?;
		let group_by = Self::compile_expressions(ast.group_by)?;
		let aggregations = Self::compile_expressions(ast.aggregations)?;
		let kind = Self::build_window_kind(ast.kind, &parsed)?;
		Self::reject_time_only_config(&parsed, &kind)?;

		Ok(LogicalPlan::Window(WindowNode {
			kind,
			group_by,
			aggregations,
			ts: parsed.time_declaration.ts_column(),
			grace: Declared::value_of(&parsed.grace).unwrap_or_default(),
			lateness: Declared::value_of(&parsed.lateness).unwrap_or_default(),
			rql,
		}))
	}

	fn reject_time_only_config(parsed: &ParsedConfig, kind: &WindowKind) -> Result<()> {
		if !kind.size().is_some_and(|size| size.is_count()) {
			return Ok(());
		}
		for (declared, message) in [
			(&parsed.grace, "no grace on count-based windows (grace needs a time domain)"),
			(&parsed.lateness, "no lateness on count-based windows (lateness needs a time domain)"),
		] {
			if let Some(declared) = declared {
				return Err(AstError::UnexpectedToken {
					expected: message.to_string(),
					fragment: declared.fragment.clone(),
				}
				.into());
			}
		}
		Ok(())
	}

	#[inline]
	fn parse_config(config: &[AstWindowConfig<'bump>], window: Fragment) -> Result<ParsedConfig> {
		let mut parsed = ParsedConfig {
			window,
			..Default::default()
		};
		for config_item in config {
			Self::parse_config_item(config_item, &mut parsed)?;
		}
		Ok(parsed)
	}

	fn compile_expressions(asts: Vec<Ast<'bump>>) -> Result<Vec<Expression>> {
		let mut expressions = Vec::new();
		for ast in asts {
			expressions.push(ExpressionCompiler::compile(ast)?);
		}
		Ok(expressions)
	}

	#[inline]
	fn build_window_kind(kind: AstWindowKind, parsed: &ParsedConfig) -> Result<WindowKind> {
		if let Some(lag) = parsed.lag.as_ref()
			&& !matches!(kind, AstWindowKind::Rolling)
		{
			return Err(AstError::UnexpectedToken {
				expected: "lag is only supported for rolling windows".to_string(),
				fragment: lag.fragment.clone(),
			}
			.into());
		}

		let time = Self::resolve_time_domain(parsed)?;

		match kind {
			AstWindowKind::Tumbling => {
				let size = Self::build_measure(parsed)?;
				Ok(WindowKind::Tumbling {
					size,
					time,
				})
			}
			AstWindowKind::Sliding => {
				let size = Self::build_measure(parsed)?;
				let slide = if let Some(d) = parsed.slide_duration.as_ref() {
					WindowSize::Duration(d.value)
				} else if let Some(c) = parsed.slide_count.as_ref() {
					WindowSize::Count(c.value)
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "slide parameter is required for sliding windows".to_string(),
						fragment: parsed.window.clone(),
					}
					.into());
				};
				Ok(WindowKind::Sliding {
					size,
					slide,
					time,
				})
			}
			AstWindowKind::Rolling => {
				let size = Self::build_measure(parsed)?;
				if let Some(lag) = parsed.lag.as_ref()
					&& !matches!(size, WindowSize::Duration(_))
				{
					return Err(AstError::UnexpectedToken {
						expected: "lag is only supported with a duration interval".to_string(),
						fragment: lag.fragment.clone(),
					}
					.into());
				}
				if let Some(lag) = parsed.lag.as_ref()
					&& !time.is_event()
				{
					return Err(AstError::UnexpectedToken {
						expected: "lag is only supported for event-time rolling windows"
							.to_string(),
						fragment: lag.fragment.clone(),
					}
					.into());
				}
				Ok(WindowKind::Rolling {
					size,
					lag: Declared::value_of(&parsed.lag),
					time,
				})
			}
			AstWindowKind::Session => {
				let gap = parsed.gap.as_ref().ok_or_else(|| AstError::UnexpectedToken {
					expected: "gap parameter is required for session windows".to_string(),
					fragment: parsed.window.clone(),
				})?;
				Ok(WindowKind::Session {
					gap: gap.value,
					time,
				})
			}
		}
	}

	fn resolve_time_domain(parsed: &ParsedConfig) -> Result<TimeDomain> {
		resolve_time_domain(&parsed.time_declaration)
	}

	fn build_measure(parsed: &ParsedConfig) -> Result<WindowSize> {
		if let Some(d) = parsed.interval.as_ref() {
			Ok(WindowSize::Duration(d.value))
		} else if let Some(c) = parsed.count.as_ref() {
			Ok(WindowSize::Count(c.value))
		} else {
			Err(AstError::UnexpectedToken {
				expected: "interval or count must be specified".to_string(),
				fragment: parsed.window.clone(),
			}
			.into())
		}
	}

	fn parse_config_item(config_item: &AstWindowConfig<'bump>, config: &mut ParsedConfig) -> Result<()> {
		match config_item.key.text() {
			"interval" | "duration" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.interval = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"count" => {
				if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.count = Some(Declared {
						value: count_val as u64,
						fragment: config_item.value.token().fragment.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "number".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"slide" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.slide_duration = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.slide_count = Some(Declared {
						value: count_val as u64,
						fragment: config_item.value.token().fragment.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string or number".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"gap" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.gap = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"lag" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.lag = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"grace" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.grace = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"lateness" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.lateness = Some(Declared {
						value: parse_duration(frag.to_owned())?,
						fragment: frag.to_owned(),
					});
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"ts" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.time_declaration.ts = Some(frag.to_owned());
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "column name string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"time" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.time_declaration.time = Some(frag.to_owned());
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "\"event\" or \"processing\"".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			_ => {
				return Err(AstError::UnexpectedToken {
					expected: "interval, count, slide, gap, lag, grace, lateness, ts, or time"
						.to_string(),
					fragment: config_item.key.token.fragment.to_owned(),
				}
				.into());
			}
		}
		Ok(())
	}

	pub fn extract_text_fragment(ast: &Ast<'bump>) -> Option<BumpFragment<'bump>> {
		if let Literal(literal) = ast
			&& let Text(text) = literal
		{
			Some(text.0.fragment)
		} else {
			None
		}
	}

	pub fn extract_literal_number(ast: &Ast) -> Option<i64> {
		if let Literal(literal) = ast
			&& let Number(number) = literal
		{
			parse_primitive_int::<i64>(number.0.fragment.to_owned()).ok()
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{ast::parse_str, bump::Bump};

	fn declared<T>(value: T) -> Option<Declared<T>> {
		Some(Declared {
			value,
			fragment: Fragment::internal("test"),
		})
	}

	fn ts_only(column: &str) -> TimeDeclaration {
		TimeDeclaration {
			time: None,
			ts: Some(Fragment::internal(column)),
		}
	}

	fn rolling_with_ts() -> ParsedConfig {
		ParsedConfig {
			interval: declared(Duration::from_seconds(60).unwrap()),
			time_declaration: ts_only("window_start"),
			..Default::default()
		}
	}

	#[test]
	// Intent: omitting `time` must keep today's implicit behavior - a ts column means event-time.
	fn default_resolves_event_when_ts_present() {
		let mut c = ParsedConfig::default();
		c.time_declaration.ts = Some(Fragment::internal("window_start"));
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Event);
	}

	#[test]
	// Intent: no ts and no explicit time means processing-time (wall clock), as before.
	fn default_resolves_processing_without_ts() {
		let c = ParsedConfig::default();
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Processing);
	}

	#[test]
	// Intent: `time: event` is a user error without a ts column, never silently processing.
	fn explicit_event_requires_ts() {
		let mut c = ParsedConfig::default();
		c.time_declaration.time = Some(Fragment::internal("event"));
		assert!(Compiler::<'static>::resolve_time_domain(&c).is_err());
		c.time_declaration.ts = Some(Fragment::internal("window_start"));
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Event);
	}

	#[test]
	// Intent: no ts and `time: processing` is the plain processing-time declaration, unchanged.
	fn explicit_processing_without_ts_resolves_processing() {
		let mut c = ParsedConfig::default();
		c.time_declaration.time = Some(Fragment::internal("processing"));
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Processing);
	}

	#[test]
	// Intent: a ts the engine would discard is a silent trap - the author believes they declared
	// event time and got processing. This assertion is duplicated from the shared resolver on
	// purpose: it proves the WINDOW level actually routes through it. A window compiler that
	// reimplemented the lenient rule locally would pass the shared test and fail this one, which
	// is the divergence the extraction exists to prevent.
	fn explicit_processing_with_ts_is_rejected() {
		let mut c = ParsedConfig::default();
		c.time_declaration.time = Some(Fragment::internal("processing"));
		c.time_declaration.ts = Some(Fragment::internal("window_start"));
		assert!(Compiler::<'static>::resolve_time_domain(&c).is_err());
	}

	#[test]
	// Intent: an unrecognized time value is rejected, not silently defaulted.
	fn unknown_time_value_is_rejected() {
		let mut c = ParsedConfig::default();
		c.time_declaration.time = Some(Fragment::internal("wallclock"));
		assert!(Compiler::<'static>::resolve_time_domain(&c).is_err());
	}

	#[test]
	// Intent: the resolved domain is carried on the constructed WindowKind (back-compat event-time rolling).
	fn rolling_carries_resolved_event_domain() {
		let c = rolling_with_ts();
		let kind = Compiler::<'static>::build_window_kind(AstWindowKind::Rolling, &c).unwrap();
		assert!(matches!(
			kind,
			WindowKind::Rolling {
				time: TimeDomain::Event,
				..
			}
		));
	}

	#[test]
	// Intent: lag is meaningless under processing-time and must be rejected even when a ts exists.
	fn rolling_lag_rejected_under_processing() {
		let mut c = rolling_with_ts();
		c.lag = declared(Duration::from_seconds(60).unwrap());
		c.time_declaration.time = Some(Fragment::internal("processing"));
		assert!(Compiler::<'static>::build_window_kind(AstWindowKind::Rolling, &c).is_err());
	}

	#[test]
	// Intent: every kind receives a resolved time; a no-ts tumbling window is processing-time.
	fn tumbling_without_ts_is_processing() {
		let mut c = ParsedConfig::default();
		c.interval = declared(Duration::from_seconds(5).unwrap());
		let kind = Compiler::<'static>::build_window_kind(AstWindowKind::Tumbling, &c).unwrap();
		assert!(matches!(
			kind,
			WindowKind::Tumbling {
				time: TimeDomain::Processing,
				..
			}
		));
	}

	#[test]
	// Intent: session windows also carry the resolved domain uniformly.
	fn session_carries_resolved_event_domain() {
		let mut c = ParsedConfig::default();
		c.gap = declared(Duration::from_seconds(30).unwrap());
		c.time_declaration.ts = Some(Fragment::internal("window_start"));
		let kind = Compiler::<'static>::build_window_kind(AstWindowKind::Session, &c).unwrap();
		assert!(matches!(
			kind,
			WindowKind::Session {
				time: TimeDomain::Event,
				..
			}
		));
	}

	fn parse_window_config(source: &str) -> Result<ParsedConfig> {
		let bump = Bump::new();
		let statements = parse_str(&bump, source).unwrap();
		let window = statements[0].first_unchecked().as_window();
		Compiler::parse_config(&window.config, window.token.fragment.to_owned())
	}

	#[test]
	// Intent: a rejected window config must point at the key the author has to change. Every
	// diagnostic here used to carry Fragment::None, which renders as "found ``" with an empty
	// value and gives the author a message with no location in a `with { }` block that may hold a
	// dozen keys. Retaining the fragment alongside the parsed value is the only thing keeping the
	// span alive from the token to the diagnostic, so this asserts the span, not just the failure.
	fn a_rejected_key_points_at_that_key() {
		let parsed = parse_window_config(
			r#"window tumbling { count(*) } with { interval: "5m", lag: "30s", ts: "at" }"#,
		)
		.unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Tumbling, &parsed).unwrap_err();
		assert_eq!(err.fragment.text(), "30s", "the offending lag value is what the author must remove");
	}

	#[test]
	// Intent: an error about something ABSENT has no key to point at, so it must fall back to the
	// window itself rather than to nothing. Fragment::None here would leave the author with a
	// message about a missing interval and no indication of which window in the statement lacks
	// it.
	fn a_missing_measure_points_at_the_window() {
		let parsed = parse_window_config(r#"window tumbling { count(*) } with { ts: "at" }"#).unwrap();

		let err = Compiler::<'static>::build_window_kind(AstWindowKind::Tumbling, &parsed).unwrap_err();
		assert_eq!(err.fragment.text(), "window", "the window token is the fallback span");
	}

	#[test]
	// Intent: `with { }` accepts only the keys the engine actually reads. The two cache-size knobs
	// were removed once the operator state cache moved to a global byte budget, so they must now be
	// rejected like any other unknown key rather than silently accepted and ignored.
	fn unknown_with_key_still_rejected() {
		assert!(parse_window_config(r#"window tumbling { count(*) } with { bogus: 1 }"#).is_err());
		assert!(
			parse_window_config(
				r#"window tumbling { count(*) } with { interval: "5m", state_cache_size: 4096 }"#
			)
			.is_err(),
			"state_cache_size was removed and must not be silently accepted"
		);
		assert!(
			parse_window_config(
				r#"window tumbling { count(*) } with { interval: "5m", internal_state_cache_size: 512 }"#
			)
			.is_err(),
			"internal_state_cache_size was removed and must not be silently accepted"
		);
	}

	#[test]
	// Intent: lateness is the operator's allowance for out-of-order events, and the retention plane
	// derives a windowed node's seal horizon as window + grace + lateness. If the parsed value were
	// dropped on the way to the plan, the horizon would silently shrink to window + grace and
	// reclamation would delete state that late events still need.
	fn lateness_is_parsed_and_reaches_the_plan() {
		let parsed =
			parse_window_config(r#"window tumbling { count(*) } with { interval: "5m", lateness: "30s" }"#)
				.unwrap();

		assert_eq!(Declared::value_of(&parsed.lateness), Some(Duration::from_seconds(30).unwrap()));
	}

	#[test]
	// Intent: lateness must be independent of grace, not an alias for it. They bound different
	// things (grace defers emission, lateness bounds how long state is kept for stragglers), so a
	// window declaring both must carry both values distinctly.
	fn lateness_and_grace_are_independent() {
		let parsed = parse_window_config(
			r#"window tumbling { count(*) } with { interval: "5m", grace: "10s", lateness: "45s" }"#,
		)
		.unwrap();

		assert_eq!(Declared::value_of(&parsed.grace), Some(Duration::from_seconds(10).unwrap()));
		assert_eq!(Declared::value_of(&parsed.lateness), Some(Duration::from_seconds(45).unwrap()));
	}

	#[test]
	// Intent: an omitted lateness must default to zero rather than to some implicit allowance, so a
	// window that declares nothing gets a seal horizon of exactly window + grace.
	fn omitted_lateness_defaults_to_zero() {
		let parsed = parse_window_config(r#"window tumbling { count(*) } with { interval: "5m" }"#).unwrap();

		assert_eq!(
			Declared::value_of(&parsed.lateness),
			None,
			"an absent key must stay absent, not become a zero default"
		);
		assert_eq!(
			Declared::value_of(&parsed.lateness).unwrap_or_default(),
			Duration::default(),
			"and it must materialise as a zero allowance in the plan"
		);
	}

	#[test]
	// Intent: lateness is an event-time allowance, so it is meaningless on a count-based window that
	// has no time domain at all. Accepting it there would let a user write a declaration that reads
	// as bounding state but cannot possibly do so - the same reason grace is rejected.
	fn lateness_is_rejected_on_count_based_windows() {
		let parsed =
			parse_window_config(r#"window tumbling { count(*) } with { count: 100, lateness: "30s" }"#)
				.unwrap();
		let kind = Compiler::<'static>::build_window_kind(AstWindowKind::Tumbling, &parsed).unwrap();

		let error = Compiler::<'static>::reject_time_only_config(&parsed, &kind).unwrap_err();

		assert!(
			format!("{error:?}").contains("lateness"),
			"the error must name lateness so the user knows which key to remove: {error:?}"
		);
	}

	#[test]
	// Intent: a malformed duration must fail at compile time. A silently ignored lateness would
	// leave the node with a zero allowance, so late events would be dropped by a horizon the user
	// believed they had widened.
	fn a_malformed_lateness_is_rejected() {
		assert!(
			parse_window_config(r#"window tumbling { count(*) } with { interval: "5m", lateness: 30 }"#)
				.is_err(),
			"a bare number is not a duration and must not be accepted"
		);
		assert!(
			parse_window_config(
				r#"window tumbling { count(*) } with { interval: "5m", lateness: "banana" }"#
			)
			.is_err(),
			"an unparseable duration string must be rejected"
		);
	}
}
