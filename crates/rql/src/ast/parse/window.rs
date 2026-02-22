// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::{
		ast::{AstWindow, AstWindowConfig},
		parse::{Parser, Precedence},
	},
	diagnostic::AstError,
	token::{
		keyword::Keyword::{By, Window, With},
		operator::Operator::{CloseCurly, Colon, OpenCurly},
		separator::Separator::Comma,
	},
};

impl<'bump> Parser<'bump> {
	pub(crate) fn parse_window(&mut self) -> crate::Result<AstWindow<'bump>> {
		let token = self.consume_keyword(Window)?;

		// Parse computation block
		self.consume_operator(OpenCurly)?;
		let mut aggregations = Vec::new();

		// Parse aggregation expressions in main window block
		loop {
			if self.is_eof() {
				return Err(AstError::UnexpectedToken {
					expected: "}".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
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
				return Err(AstError::UnexpectedToken {
					expected: ", or }".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
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
	fn parse_with_clause(&mut self) -> crate::Result<Vec<AstWindowConfig<'bump>>> {
		self.consume_operator(OpenCurly)?;

		let mut config = Vec::new();

		loop {
			if self.is_eof() {
				return Err(AstError::UnexpectedToken {
					expected: "}".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
			}

			if self.current()?.is_operator(CloseCurly) {
				break;
			}

			// Parse configuration parameter (identifier: value)
			if !self.current()?.is_identifier() {
				return Err(AstError::UnexpectedToken {
					expected: "configuration parameter name".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
			}

			let key = self.parse_identifier_with_hyphens()?;
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
				return Err(AstError::UnexpectedToken {
					expected: ", or }".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
			}
		}

		self.consume_operator(CloseCurly)?;
		Ok(config)
	}

	/// Parse BY { field1, field2 } clause
	fn parse_by_clause(&mut self) -> crate::Result<Vec<crate::ast::ast::Ast<'bump>>> {
		self.consume_operator(OpenCurly)?;

		let mut group_by = Vec::new();

		loop {
			if self.is_eof() {
				return Err(AstError::UnexpectedToken {
					expected: "}".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
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
				return Err(AstError::UnexpectedToken {
					expected: ", or }".to_string(),
					fragment: self.current()?.fragment.to_owned(),
				}
				.into());
			}
		}

		self.consume_operator(CloseCurly)?;
		Ok(group_by)
	}
}

#[cfg(test)]
pub mod tests {
	use crate::{ast::parse::Parser, bump::Bump, token::tokenize};

	#[test]
	fn test_parse_time_window() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"window { count(*) } with { interval: "5m" }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "interval");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_count_window() {
		let bump = Bump::new();
		let tokens =
			tokenize(&bump, r#"window { sum(value) } with { count: 100 }"#).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "count");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_sliding_window() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"window { count(*), avg(value) } with { interval: "5m", slide: "1m" }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 2);
		assert_eq!(window.aggregations.len(), 2);
	}

	#[test]
	fn test_parse_grouped_window() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"window { count(*) } with { interval: "1h" } by { user_id }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.group_by.len(), 1);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_window_by_then_with() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"window { count(*) } by { user_id, region } with { interval: "1h" }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 1);
		assert_eq!(window.group_by.len(), 2);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_window_multiple_aggregations_and_grouping() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"window { count(*), sum(amount), avg(price) } with { interval: "30m", slide: "5m" } by { customer_id, product_category }"#).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 2);
		assert_eq!(window.group_by.len(), 2);
		assert_eq!(window.aggregations.len(), 3);
	}

	#[test]
	fn test_parse_rolling_count_window() {
		let bump = Bump::new();
		let tokens = tokenize(
			&bump,
			r#"window { count(*), avg(value) } with { count: 10, rolling: true } by { user_id }"#,
		)
		.unwrap()
		.into_iter()
		.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 2);
		assert_eq!(window.config[0].key.text(), "count");
		assert_eq!(window.config[1].key.text(), "rolling");
		assert_eq!(window.group_by.len(), 1);
		assert_eq!(window.aggregations.len(), 2);
	}

	#[test]
	fn test_parse_rolling_time_window() {
		let bump = Bump::new();
		let tokens = tokenize(&bump, r#"window { sum(amount) } with { interval: "5m", rolling: true }"#)
			.unwrap()
			.into_iter()
			.collect();
		let mut parser = Parser::new(&bump, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.config.len(), 2);
		assert_eq!(window.config[0].key.text(), "interval");
		assert_eq!(window.config[1].key.text(), "rolling");
		assert_eq!(window.aggregations.len(), 1);
	}
}
