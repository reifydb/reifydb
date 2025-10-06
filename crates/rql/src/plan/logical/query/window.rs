// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{WindowSize, WindowSlide, WindowType, interface::expression::Expression};
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

#[derive(Debug, Clone)]
pub struct WindowNode<'a> {
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression<'a>>,
	pub aggregations: Vec<Expression<'a>>,
}

impl Compiler {
	pub(crate) fn compile_window<'a, T: CatalogQueryTransaction>(
		ast: AstWindow<'a>,
		_tx: &mut T,
	) -> Result<LogicalPlan<'a>> {
		let mut window_type = None;
		let mut size = None;
		let mut slide = None;
		let mut group_by = Vec::new();

		// Parse configuration parameters
		for config in &ast.config {
			match config.key.text() {
				"interval" => {
					window_type = Some(WindowType::Time);
					if let Some(duration_str) = Self::extract_literal_string(&config.value) {
						size = Some(WindowSize::Duration(Self::parse_duration(&duration_str)?));
					} else {
						return_error!(unexpected_token_error(
							"duration string",
							config.value.token().fragment.clone()
						));
					}
				}
				"count" => {
					window_type = Some(WindowType::Count);
					if let Some(count_val) = Self::extract_literal_number(&config.value) {
						size = Some(WindowSize::Count(count_val as u64));
					} else {
						return_error!(unexpected_token_error(
							"number",
							config.value.token().fragment.clone()
						));
					}
				}
				"slide" => {
					if let Some(duration_str) = Self::extract_literal_string(&config.value) {
						slide = Some(WindowSlide::Duration(Self::parse_duration(
							&duration_str,
						)?));
					} else if let Some(count_val) = Self::extract_literal_number(&config.value) {
						slide = Some(WindowSlide::Count(count_val as u64));
					} else {
						return_error!(unexpected_token_error(
							"duration string or number",
							config.value.token().fragment.clone()
						));
					}
				}
				_ => {
					return_error!(unexpected_token_error(
						"interval, count, or slide",
						config.key.token.fragment.clone()
					));
				}
			}
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

		let window_type = window_type.ok_or_else(|| {
			reifydb_type::Error(reifydb_core::diagnostic::internal::internal("Window type not specified"))
		})?;

		let size = size.ok_or_else(|| {
			reifydb_type::Error(reifydb_core::diagnostic::internal::internal("Window size not specified"))
		})?;

		Ok(LogicalPlan::Window(WindowNode {
			window_type,
			size,
			slide,
			group_by,
			aggregations,
		}))
	}

	fn parse_duration(duration_str: &str) -> Result<Duration> {
		// Parse duration strings like "5m", "1h", "30s"
		let duration_str = duration_str.trim_matches('"');

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

	fn extract_literal_string(ast: &crate::ast::Ast) -> Option<String> {
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

	fn extract_literal_number(ast: &crate::ast::Ast) -> Option<i64> {
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
