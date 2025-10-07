// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{WindowSize, WindowSlide, WindowTimeMode, WindowType, interface::expression::Expression};
use reifydb_type::{diagnostic::ast::unexpected_token_error, return_error};

use crate::{
	Result,
	ast::{
		Ast::Literal,
		AstLiteral::{Number, Text},
		AstWindow,
	},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, LogicalPlan},
};

mod sliding;
mod tumbling;

pub use sliding::*;
pub use tumbling::*;

#[derive(Debug, Clone)]
pub struct WindowNode<'a> {
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression<'a>>,
	pub aggregations: Vec<Expression<'a>>,
}

/// Configuration parameters parsed from WITH clause
#[derive(Debug, Default)]
pub struct WindowConfig {
	pub window_type: Option<WindowType>,
	pub size: Option<WindowSize>,
	pub slide: Option<WindowSlide>,
	pub timestamp_column: Option<String>,
}

impl Compiler {
	pub(crate) fn compile_window<'a, T: CatalogQueryTransaction>(
		ast: AstWindow<'a>,
		_tx: &mut T,
	) -> Result<LogicalPlan<'a>> {
		let mut config = WindowConfig::default();
		let mut group_by = Vec::new();

		// Parse configuration parameters
		for config_item in &ast.config {
			Self::parse_config_item(config_item, &mut config)?;
		}

		// Compile group by expressions from AST
		for group_ast in &ast.group_by {
			let group_expr = ExpressionCompiler::compile(group_ast.clone())?;
			group_by.push(group_expr);
		}

		// Compile aggregation expressions
		let mut aggregations = Vec::new();
		for agg_ast in &ast.aggregations {
			let agg_expr = ExpressionCompiler::compile(agg_ast.clone())?;
			aggregations.push(agg_expr);
		}

		// Determine window type based on configuration
		let window_node = if config.slide.is_some() {
			// Sliding window
			sliding::create_sliding_window(config, group_by, aggregations)?
		} else {
			// Tumbling window
			tumbling::create_tumbling_window(config, group_by, aggregations)?
		};

		Ok(LogicalPlan::Window(window_node))
	}

	fn parse_config_item(config_item: &crate::ast::AstWindowConfig, config: &mut WindowConfig) -> Result<()> {
		match config_item.key.text() {
			"interval" => {
				config.window_type = Some(WindowType::Time(WindowTimeMode::Processing));
				if let Some(duration_str) = Self::extract_literal_string(&config_item.value) {
					config.size = Some(WindowSize::Duration(Self::parse_duration(&duration_str)?));
				} else {
					return_error!(unexpected_token_error(
						"duration string",
						config_item.value.token().fragment.clone()
					));
				}
			}
			"count" => {
				config.window_type = Some(WindowType::Count);
				if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.size = Some(WindowSize::Count(count_val as u64));
				} else {
					return_error!(unexpected_token_error(
						"number",
						config_item.value.token().fragment.clone()
					));
				}
			}
			"slide" => {
				if let Some(duration_str) = Self::extract_literal_string(&config_item.value) {
					config.slide =
						Some(WindowSlide::Duration(Self::parse_duration(&duration_str)?));
				} else if let Some(count_val) = Self::extract_literal_number(&config_item.value) {
					config.slide = Some(WindowSlide::Count(count_val as u64));
				} else {
					return_error!(unexpected_token_error(
						"duration string or number",
						config_item.value.token().fragment.clone()
					));
				}
			}
			"timestamp_column" => {
				if let Some(column_name) = Self::extract_literal_string(&config_item.value) {
					config.timestamp_column = Some(column_name.clone());
					// Update window_type to use EventTime mode if timestamp_column is specified
					if let Some(WindowType::Time(_)) = config.window_type {
						config.window_type =
							Some(WindowType::Time(WindowTimeMode::EventTime(column_name)));
					}
				} else {
					return_error!(unexpected_token_error(
						"column name string",
						config_item.value.token().fragment.clone()
					));
				}
			}
			_ => {
				return_error!(unexpected_token_error(
					"interval, count, slide, or timestamp_column",
					config_item.key.token.fragment.clone()
				));
			}
		}
		Ok(())
	}

	pub fn parse_duration(duration_str: &str) -> Result<Duration> {
		// Parse duration strings like "5m", "1h", "30s", "100ms"
		let duration_str = duration_str.trim_matches('"');

		// Handle milliseconds suffix "ms"
		if duration_str.ends_with("ms") {
			let number_part = &duration_str[..duration_str.len() - 2];
			let number: u64 = number_part.parse().map_err(|_| {
				reifydb_type::Error(reifydb_core::diagnostic::internal::internal(
					"Invalid duration number",
				))
			})?;
			return Ok(Duration::from_millis(number));
		}

		// Handle single character suffixes
		if let Some(suffix) = duration_str.chars().last() {
			let number_part = &duration_str[..duration_str.len() - 1];
			let number: u64 = number_part.parse().map_err(|_| {
				reifydb_type::Error(reifydb_core::diagnostic::internal::internal(
					"Invalid duration number",
				))
			})?;

			let duration = match suffix {
				's' => Duration::from_secs(number),
				'm' => Duration::from_secs(number * 60),
				'h' => Duration::from_secs(number * 3600),
				'd' => Duration::from_secs(number * 86400),
				_ => {
					return Err(reifydb_type::Error(reifydb_core::diagnostic::internal::internal(
						"Invalid duration suffix",
					)));
				}
			};

			Ok(duration)
		} else {
			Err(reifydb_type::Error(reifydb_core::diagnostic::internal::internal(
				"Invalid duration format",
			)))
		}
	}

	pub fn extract_literal_string(ast: &crate::ast::Ast) -> Option<String> {
		if let Literal(literal) = ast {
			if let Text(text) = literal {
				Some(text.0.fragment.text().to_string())
			} else {
				None
			}
		} else {
			None
		}
	}

	pub fn extract_literal_number(ast: &crate::ast::Ast) -> Option<i64> {
		if let Literal(literal) = ast {
			if let Number(number) = literal {
				number.0.fragment.text().parse().ok()
			} else {
				None
			}
		} else {
			None
		}
	}
}
