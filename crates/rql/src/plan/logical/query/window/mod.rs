// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
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

		match kind {
			AstWindowKind::Tumbling => {
				let size = Self::build_measure(parsed)?;
				Ok(WindowKind::Tumbling {
					size,
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
				if parsed.lag.is_some() && parsed.ts.is_none() {
					return Err(AstError::UnexpectedToken {
						expected: "lag requires a ts column (event-time rolling)".to_string(),
						fragment: Fragment::None,
					}
					.into());
				}
				Ok(WindowKind::Rolling {
					size,
					lag: parsed.lag,
				})
			}
			AstWindowKind::Session => {
				let gap = parsed.gap.ok_or_else(|| AstError::UnexpectedToken {
					expected: "gap parameter is required for session windows".to_string(),
					fragment: Fragment::None,
				})?;
				Ok(WindowKind::Session {
					gap,
				})
			}
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
			_ => {
				return Err(AstError::UnexpectedToken {
					expected: "interval, count, slide, gap, lag, or ts".to_string(),
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
