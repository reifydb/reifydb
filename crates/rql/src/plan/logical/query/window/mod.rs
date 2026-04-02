// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	internal_error,
};
use reifydb_type::fragment::Fragment;

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

/// Raw parsed config values from WITH clause (before constructing WindowKind)
#[derive(Debug, Default)]
struct ParsedConfig {
	pub interval: Option<Duration>,
	pub count: Option<u64>,
	pub slide_duration: Option<Duration>,
	pub slide_count: Option<u64>,
	pub gap: Option<Duration>,
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
		let mut parsed = ParsedConfig::default();
		let mut group_by = Vec::new();

		// Parse configuration parameters
		for config_item in &ast.config {
			Self::parse_config_item(config_item, &mut parsed)?;
		}

		// Compile group by expressions from AST
		for group_ast in ast.group_by {
			let group_expr = ExpressionCompiler::compile(group_ast)?;
			group_by.push(group_expr);
		}

		// Compile aggregation expressions
		let mut aggregations = Vec::new();
		for agg_ast in ast.aggregations {
			let agg_expr = ExpressionCompiler::compile(agg_ast)?;
			aggregations.push(agg_expr);
		}

		// Determine WindowKind from explicit kind keyword
		let kind = match ast.kind {
			AstWindowKind::Tumbling => {
				let size = Self::build_measure(&parsed)?;
				WindowKind::Tumbling {
					size,
				}
			}
			AstWindowKind::Sliding => {
				let size = Self::build_measure(&parsed)?;
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
				WindowKind::Sliding {
					size,
					slide,
				}
			}
			AstWindowKind::Rolling => {
				let size = Self::build_measure(&parsed)?;
				WindowKind::Rolling {
					size,
				}
			}
			AstWindowKind::Session => {
				let gap = parsed.gap.ok_or_else(|| AstError::UnexpectedToken {
					expected: "gap parameter is required for session windows".to_string(),
					fragment: Fragment::None,
				})?;
				WindowKind::Session {
					gap,
				}
			}
		};

		let window_node = WindowNode {
			kind,
			group_by,
			aggregations,
			ts: parsed.ts,
			rql,
		};

		Ok(LogicalPlan::Window(window_node))
	}

	/// Build a WindowSize from parsed config (interval or count)
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
					expected: "interval, count, slide, or gap".to_string(),
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
			return Ok(Duration::from_millis(number));
		}

		if let Some(suffix) = duration_str.chars().last() {
			let number_part = &duration_str[..duration_str.len() - 1];
			let number: u64 =
				number_part.parse().map_err(|_| internal_error!("Invalid duration number"))?;

			let duration = match suffix {
				's' => Duration::from_secs(number),
				'm' => Duration::from_secs(number * 60),
				'h' => Duration::from_secs(number * 3600),
				'd' => Duration::from_secs(number * 86400),
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
