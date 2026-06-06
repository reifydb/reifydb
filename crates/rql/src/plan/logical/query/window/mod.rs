// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{TimeDomain, WindowKind, WindowSize},
	internal_error,
};
use reifydb_value::{fragment::Fragment, value::duration::Duration};

use crate::{
	Result,
	ast::ast::{
		Ast,
		Ast::Literal,
		AstLiteral::{Number, Text},
		AstWindow, AstWindowConfig, AstWindowKind,
	},
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
	pub ts: Option<String>,
	pub time: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
	pub rql: String,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_window(&self, ast: AstWindow<'bump>) -> Result<LogicalPlan<'bump>> {
		let rql = ast.rql.to_string();

		let parsed = Self::parse_config(&ast.config)?;
		let group_by = Self::compile_expressions(ast.group_by)?;
		let aggregations = Self::compile_expressions(ast.aggregations)?;
		let kind = Self::build_window_kind(ast.kind, &parsed)?;

		Ok(LogicalPlan::Window(WindowNode {
			kind,
			group_by,
			aggregations,
			ts: parsed.ts,
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

	fn parse_config_item(config_item: &AstWindowConfig, config: &mut ParsedConfig) -> Result<()> {
		match config_item.key.text() {
			"interval" | "duration" => {
				if let Some(duration_str) = Self::extract_literal_string(&config_item.value) {
					config.interval = Some(Self::parse_duration(&duration_str)?);
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
				if let Some(duration_str) = Self::extract_literal_string(&config_item.value) {
					config.slide_duration = Some(Self::parse_duration(&duration_str)?);
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
				if let Some(duration_str) = Self::extract_literal_string(&config_item.value) {
					config.gap = Some(Self::parse_duration(&duration_str)?);
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"lag" => {
				if let Some(duration_str) = Self::extract_literal_string(&config_item.value) {
					config.lag = Some(Self::parse_duration(&duration_str)?);
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "duration string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"ts" => {
				if let Some(ts_str) = Self::extract_literal_string(&config_item.value) {
					config.ts = Some(ts_str);
				} else {
					return Err(AstError::UnexpectedToken {
						expected: "column name string".to_string(),
						fragment: config_item.value.token().fragment.to_owned(),
					}
					.into());
				}
			}
			"time" => {
				if let Some(time_str) = Self::extract_literal_string(&config_item.value) {
					config.time = Some(time_str.to_ascii_lowercase());
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
					expected: "interval, count, slide, gap, lag, ts, or time".to_string(),
					fragment: config_item.key.token.fragment.to_owned(),
				}
				.into());
			}
		}
		Ok(())
	}

	pub fn parse_duration(duration_str: &str) -> Result<Duration> {
		let duration_str = duration_str.trim_matches('"');

		if let Some(number_part) = duration_str.strip_suffix("ms") {
			let number: u64 =
				number_part.parse().map_err(|_| internal_error!("Invalid duration number"))?;
			return Ok(Duration::from_milliseconds(number as i64).unwrap());
		}

		if let Some(suffix) = duration_str.chars().last() {
			let number_part = &duration_str[..duration_str.len() - 1];
			let number: u64 =
				number_part.parse().map_err(|_| internal_error!("Invalid duration number"))?;

			let duration = match suffix {
				's' => Duration::from_seconds(number as i64).unwrap(),
				'm' => Duration::from_seconds((number * 60) as i64).unwrap(),
				'h' => Duration::from_seconds((number * 3600) as i64).unwrap(),
				'd' => Duration::from_seconds((number * 86400) as i64).unwrap(),
				_ => {
					return Err(internal_error!("Invalid duration suffix"));
				}
			};

			Ok(duration)
		} else {
			Err(internal_error!("Invalid duration format"))
		}
	}

	pub fn extract_literal_string(ast: &Ast) -> Option<String> {
		if let Literal(literal) = ast
			&& let Text(text) = literal
		{
			Some(text.0.fragment.text().to_string())
		} else {
			None
		}
	}

	pub fn extract_literal_number(ast: &Ast) -> Option<i64> {
		if let Literal(literal) = ast
			&& let Number(number) = literal
		{
			number.0.fragment.text().parse().ok()
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

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
}
