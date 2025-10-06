// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{diagnostic::ast::unexpected_token_error, return_error};

use crate::ast::{
	AstWindow, AstWindowConfig,
	parse::{Parser, Precedence},
	tokenize::{
		Keyword::{By, Window, With},
		Operator::{CloseCurly, Colon, OpenCurly},
		Separator::Comma,
	},
};

impl<'a> Parser<'a> {
	pub(crate) fn parse_window(&mut self) -> crate::Result<AstWindow<'a>> {
		let token = self.consume_keyword(Window)?;

		// Parse computation block
		self.consume_operator(OpenCurly)?;
		let mut aggregations = Vec::new();

		// Parse aggregation expressions in main window block
		loop {
			if self.is_eof() {
				return_error!(unexpected_token_error("}", self.current()?.fragment.clone()));
			}

			if self.current()?.is_operator(CloseCurly) {
				break;
			}

			// Parse aggregation expression
			aggregations.push(self.parse_node(Precedence::None)?);

			// Handle comma separation
			if self.current()?.is_separator(Comma) {
				let _ = self.advance()?;
			} else if self.current()?.is_operator(CloseCurly) {
				break;
			} else {
				return_error!(unexpected_token_error(", or }", self.current()?.fragment.clone()));
			}
		}

		self.consume_operator(CloseCurly)?;

		// Parse optional WITH and BY clauses in any order
		let mut config = Vec::new();
		let mut group_by = Vec::new();

		// Keep parsing WITH and BY clauses until we don't find any more
		loop {
			if self.is_eof() {
				break;
			}

			let current = self.current()?;
			if current.is_keyword(With) {
				let _ = self.advance()?; // consume 'with'
				let with_config = self.parse_with_clause()?;
				config.extend(with_config);
			} else if current.is_keyword(By) {
				let _ = self.advance()?; // consume 'by'
				let by_exprs = self.parse_by_clause()?;
				group_by.extend(by_exprs);
			} else {
				// No more WITH or BY clauses
				break;
			}
		}

		Ok(AstWindow {
			token,
			config,
			aggregations,
			group_by,
		})
	}

	/// Parse WITH { interval: "5m", slide: "1m" } clause
	fn parse_with_clause(&mut self) -> crate::Result<Vec<AstWindowConfig<'a>>> {
		self.consume_operator(OpenCurly)?;

		let mut config = Vec::new();

		loop {
			if self.is_eof() {
				return_error!(unexpected_token_error("}", self.current()?.fragment.clone()));
			}

			if self.current()?.is_operator(CloseCurly) {
				break;
			}

			// Parse configuration parameter (identifier: value)
			if !self.current()?.is_identifier() {
				return_error!(unexpected_token_error(
					"configuration parameter name",
					self.current()?.fragment.clone()
				));
			}

			let key = self.parse_as_identifier()?;
			self.consume_operator(Colon)?;
			let value = self.parse_node(Precedence::None)?;

			config.push(AstWindowConfig {
				key,
				value,
			});

			// Handle comma separation
			if self.current()?.is_separator(Comma) {
				let _ = self.advance()?;
			} else if self.current()?.is_operator(CloseCurly) {
				break;
			} else {
				return_error!(unexpected_token_error(", or }", self.current()?.fragment.clone()));
			}
		}

		self.consume_operator(CloseCurly)?;
		Ok(config)
	}

	/// Parse BY { field1, field2 } clause
	fn parse_by_clause(&mut self) -> crate::Result<Vec<crate::ast::Ast<'a>>> {
		self.consume_operator(OpenCurly)?;

		let mut group_by = Vec::new();

		loop {
			if self.is_eof() {
				return_error!(unexpected_token_error("}", self.current()?.fragment.clone()));
			}

			if self.current()?.is_operator(CloseCurly) {
				break;
			}

			// Parse grouping expression
			group_by.push(self.parse_node(Precedence::None)?);

			// Handle comma separation
			if self.current()?.is_separator(Comma) {
				let _ = self.advance()?;
			} else if self.current()?.is_operator(CloseCurly) {
				break;
			} else {
				return_error!(unexpected_token_error(", or }", self.current()?.fragment.clone()));
			}
		}

		self.consume_operator(CloseCurly)?;
		Ok(group_by)
	}
}

#[cfg(test)]
mod tests {
	use crate::ast::{parse::Parser, tokenize::tokenize};

	#[test]
	fn test_parse_time_window() {
		let tokens = tokenize(r#"window { count(*) } with { interval: "5m" }"#).unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "interval");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_count_window() {
		let tokens = tokenize(r#"window { sum(value) } with { count: 100 }"#).unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "count");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_sliding_window() {
		let tokens =
			tokenize(r#"window { count(*), avg(value) } with { interval: "5m", slide: "1m" }"#).unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 2);
		assert_eq!(window.aggregations.len(), 2);
	}

	#[test]
	fn test_parse_grouped_window() {
		let tokens = tokenize(r#"window { count(*) } with { interval: "1h" } by { user_id }"#).unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.group_by.len(), 1);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_window_by_then_with() {
		let tokens = tokenize(r#"window { count(*) } by { user_id, region } with { interval: "1h" }"#).unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.group_by.len(), 2);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_window_multiple_aggregations_and_grouping() {
		let tokens = tokenize(r#"window { count(*), sum(amount), avg(price) } with { interval: "30m", slide: "5m" } by { customer_id, product_category }"#).unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 2);
		assert_eq!(window.group_by.len(), 2);
		assert_eq!(window.aggregations.len(), 3);
	}
}
