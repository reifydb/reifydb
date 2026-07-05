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
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Default)]
struct ParsedConfig {
	pub interval: Option<Duration>,
	pub count: Option<u64>,
	pub slide_duration: Option<Duration>,
	pub slide_count: Option<u64>,
	pub gap: Option<Duration>,
	pub lag: Option<Duration>,
	pub grace: Option<Duration>,
	pub ts: Option<String>,
	pub time: Option<String>,
	pub state_cache_size: Option<usize>,
	pub internal_state_cache_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub grace: Duration,
	pub state_cache_size: Option<usize>,
	pub internal_state_cache_size: Option<usize>,
	pub rql: String,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_window(&self, ast: AstWindow<'bump>) -> Result<LogicalPlan<'bump>> {
		let rql = ast.rql.to_string();

		let parsed = Self::parse_config(&ast.config)?;
		let group_by = Self::compile_expressions(ast.group_by)?;
		let aggregations = Self::compile_expressions(ast.aggregations)?;
		let kind = Self::build_window_kind(ast.kind, &parsed)?;
		if parsed.grace.is_some() && kind.size().is_some_and(|size| size.is_count()) {
			return Err(AstError::UnexpectedToken {
				expected: "no grace on count-based windows (grace needs a time domain)".to_string(),
				fragment: Fragment::None,
			}
			.into());
		}

		Ok(LogicalPlan::Window(WindowNode {
			kind,
			group_by,
			aggregations,
			ts: parsed.ts,
			grace: parsed.grace.unwrap_or_default(),
			state_cache_size: parsed.state_cache_size,
			internal_state_cache_size: parsed.internal_state_cache_size,
			rql,
		}))
	}

	#[inline]
	fn parse_config(config: &[AstWindowConfig<'bump>]) -> Result<ParsedConfig> {
		let mut parsed = ParsedConfig::default();
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
		if parsed.lag.is_some() && !matches!(kind, AstWindowKind::Rolling) {
			return Err(AstError::UnexpectedToken {
				expected: "lag is only supported for rolling windows".to_string(),
				fragment: Fragment::None,
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
				let slide = if let Some(d) = parsed.slide_duration {
					WindowSize::Duration(d)
				} else if let Some(c) = parsed.slide_count {
					WindowSize::Count(c)
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "slide parameter is required for sliding windows".to_string(),
						fragment: Fragment::None,
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
				if parsed.lag.is_some() && !matches!(size, WindowSize::Duration(_)) {
					return Err(AstError::UnexpectedToken {
						expected: "lag is only supported with a duration interval".to_string(),
						fragment: Fragment::None,
					}
					.into());
				}
				if parsed.lag.is_some() && time != TimeDomain::Event {
					return Err(AstError::UnexpectedToken {
						expected: "lag is only supported for event-time rolling windows"
							.to_string(),
						fragment: Fragment::None,
					}
					.into());
				}
				Ok(WindowKind::Rolling {
					size,
					lag: parsed.lag,
					time,
				})
			}
			AstWindowKind::Session => {
				let gap = parsed.gap.ok_or_else(|| AstError::UnexpectedToken {
					expected: "gap parameter is required for session windows".to_string(),
					fragment: Fragment::None,
				})?;
				Ok(WindowKind::Session {
					gap,
					time,
				})
			}
		}
	}

	fn resolve_time_domain(parsed: &ParsedConfig) -> Result<TimeDomain> {
		match parsed.time.as_deref() {
			Some("event") => {
				if parsed.ts.is_none() {
					return Err(AstError::UnexpectedToken {
						expected: "time: event requires a ts column".to_string(),
						fragment: Fragment::None,
					}
					.into());
				}
				Ok(TimeDomain::Event)
			}
			Some("processing") => Ok(TimeDomain::Processing),
			Some(_) => Err(AstError::UnexpectedToken {
				expected: "\"event\" or \"processing\"".to_string(),
				fragment: Fragment::None,
			}
			.into()),
			None => Ok(if parsed.ts.is_some() {
				TimeDomain::Event
			} else {
				TimeDomain::Processing
			}),
		}
	}

	fn build_measure(parsed: &ParsedConfig) -> Result<WindowSize> {
		if let Some(d) = parsed.interval {
			Ok(WindowSize::Duration(d))
		} else if let Some(c) = parsed.count {
			Ok(WindowSize::Count(c))
		} else {
			Err(AstError::UnexpectedToken {
				expected: "interval or count must be specified".to_string(),
				fragment: Fragment::None,
			}
			.into())
		}
	}

	fn parse_config_item(config_item: &AstWindowConfig<'bump>, config: &mut ParsedConfig) -> Result<()> {
		match config_item.key.text() {
			"interval" | "duration" => {
				if let Some(frag) = Self::extract_text_fragment(&config_item.value) {
					config.interval = Some(parse_duration(frag.to_owned())?);
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
					config.count = Some(count_val as u64);
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
					config.slide_duration = Some(parse_duration(frag.to_owned())?);
				} else if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.slide_count = Some(count_val as u64);
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
					config.gap = Some(parse_duration(frag.to_owned())?);
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
					config.lag = Some(parse_duration(frag.to_owned())?);
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
					config.grace = Some(parse_duration(frag.to_owned())?);
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
					config.ts = Some(frag.text().to_string());
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
					config.time = Some(frag.text().to_ascii_lowercase());
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "\"event\" or \"processing\"".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"state_cache_size" => {
				if let Some(value) = Self::extract_literal_number(&config_item.value) {
					config.state_cache_size = Some(value as usize);
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "number".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"internal_state_cache_size" => {
				if let Some(value) = Self::extract_literal_number(&config_item.value) {
					config.internal_state_cache_size = Some(value as usize);
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "number".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			_ => {
				return Err(AstError::UnexpectedToken {
					expected:
						"interval, count, slide, gap, lag, grace, ts, time, state_cache_size, \
					           or internal_state_cache_size"
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

	fn rolling_with_ts() -> ParsedConfig {
		ParsedConfig {
			interval: Some(Duration::from_seconds(60).unwrap()),
			ts: Some("window_start".to_string()),
			..Default::default()
		}
	}

	#[test]
	// Intent: omitting `time` must keep today's implicit behavior - a ts column means event-time.
	fn default_resolves_event_when_ts_present() {
		let mut c = ParsedConfig::default();
		c.ts = Some("window_start".to_string());
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
		c.time = Some("event".to_string());
		assert!(Compiler::<'static>::resolve_time_domain(&c).is_err());
		c.ts = Some("window_start".to_string());
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Event);
	}

	#[test]
	// Intent: `time: processing` is legal regardless of a ts column (ts is ignored for bucketing).
	fn explicit_processing_allowed_with_or_without_ts() {
		let mut c = ParsedConfig::default();
		c.time = Some("processing".to_string());
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Processing);
		c.ts = Some("window_start".to_string());
		assert_eq!(Compiler::<'static>::resolve_time_domain(&c).unwrap(), TimeDomain::Processing);
	}

	#[test]
	// Intent: an unrecognized time value is rejected, not silently defaulted.
	fn unknown_time_value_is_rejected() {
		let mut c = ParsedConfig::default();
		c.time = Some("wallclock".to_string());
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
		c.lag = Some(Duration::from_seconds(60).unwrap());
		c.time = Some("processing".to_string());
		assert!(Compiler::<'static>::build_window_kind(AstWindowKind::Rolling, &c).is_err());
	}

	#[test]
	// Intent: every kind receives a resolved time; a no-ts tumbling window is processing-time.
	fn tumbling_without_ts_is_processing() {
		let mut c = ParsedConfig::default();
		c.interval = Some(Duration::from_seconds(5).unwrap());
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
		c.gap = Some(Duration::from_seconds(30).unwrap());
		c.ts = Some("window_start".to_string());
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
		Compiler::parse_config(&window.config)
	}

	#[test]
	// Intent: the optional cache-size knobs in `with { }` parse as counts and land on ParsedConfig.
	fn parses_optional_cache_sizes() {
		let parsed = parse_window_config(
			r#"window tumbling { count(*) } with { interval: "5m", state_cache_size: 4096, internal_state_cache_size: 512 }"#,
		)
		.unwrap();
		assert_eq!(parsed.state_cache_size, Some(4096));
		assert_eq!(parsed.internal_state_cache_size, Some(512));
	}

	#[test]
	// Intent: when omitted the knobs stay None, so the engine keeps its built-in default capacities.
	fn cache_sizes_absent_stay_none() {
		let parsed = parse_window_config(r#"window tumbling { count(*) } with { interval: "5m" }"#).unwrap();
		assert_eq!(parsed.state_cache_size, None);
		assert_eq!(parsed.internal_state_cache_size, None);
	}

	#[test]
	// Intent: adding the two keys must not open `with { }` to arbitrary keys - unknown keys still error.
	fn unknown_with_key_still_rejected() {
		assert!(parse_window_config(r#"window tumbling { count(*) } with { bogus: 1 }"#).is_err());
	}
}
