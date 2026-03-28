// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::{
		ast::{Ast, AstWindow, AstWindowConfig, AstWindowKind},
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
	pub(crate) fn parse_window(&mut self) -> Result<AstWindow<'bump>> {
		let start = self.current()?.fragment.offset();
		let token = self.consume_keyword(Window)?;

		// Parse mandatory window kind (tumbling, sliding, rolling, session)
		let kind = if !self.is_eof() {
			match self.current()?.fragment.text().to_lowercase().as_str() {
				"tumbling" => {
					let _ = self.advance()?;
					AstWindowKind::Tumbling
				}
				"sliding" => {
					let _ = self.advance()?;
					AstWindowKind::Sliding
				}
				"rolling" => {
					let _ = self.advance()?;
					AstWindowKind::Rolling
				}
				"session" => {
					let _ = self.advance()?;
					AstWindowKind::Session
				}
				_ => {
					return Err(AstError::UnexpectedToken {
						expected: "window kind (tumbling, sliding, rolling, session)"
							.to_string(),
						fragment: self.current()?.fragment.to_owned(),
					}
					.into());
				}
			}
		} else {
			return Err(AstError::UnexpectedToken {
				expected: "window kind (tumbling, sliding, rolling, session)".to_string(),
				fragment: self.current()?.fragment.to_owned(),
			}
			.into());
		};

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
			kind,
			config,
			aggregations,
			group_by,
			rql: self.source_since(start),
		})
	}

	/// Parse WITH { interval: "5m", slide: "1m" } clause
	fn parse_with_clause(&mut self) -> Result<Vec<AstWindowConfig<'bump>>> {
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
	fn parse_by_clause(&mut self) -> Result<Vec<Ast<'bump>>> {
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
	use crate::{
		ast::{ast::AstWindowKind, parse::Parser},
		bump::Bump,
		token::tokenize,
	};

	#[test]
	fn test_parse_tumbling_time_window() {
		let bump = Bump::new();
		let source = r#"window tumbling { count(*) } with { interval: "5m" }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Tumbling);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "interval");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_tumbling_count_window() {
		let bump = Bump::new();
		let source = r#"window tumbling { sum(value) } with { count: 100 }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Tumbling);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "count");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_sliding_window() {
		let bump = Bump::new();
		let source = r#"window sliding { count(*), avg(value) } with { interval: "5m", slide: "1m" }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Sliding);
		assert_eq!(window.config.len(), 2);
		assert_eq!(window.aggregations.len(), 2);
	}

	#[test]
	fn test_parse_tumbling_grouped_window() {
		let bump = Bump::new();
		let source = r#"window tumbling { count(*) } with { interval: "1h" } by { user_id }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Tumbling);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.group_by.len(), 1);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_window_by_then_with() {
		let bump = Bump::new();
		let source = r#"window tumbling { count(*) } by { user_id, region } with { interval: "1h" }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Tumbling);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.group_by.len(), 2);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_sliding_multiple_aggregations_and_grouping() {
		let bump = Bump::new();
		let source = r#"window sliding { count(*), sum(amount), avg(price) } with { interval: "30m", slide: "5m" } by { customer_id, product_category }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Sliding);
		assert_eq!(window.config.len(), 2);
		assert_eq!(window.group_by.len(), 2);
		assert_eq!(window.aggregations.len(), 3);
	}

	#[test]
	fn test_parse_rolling_count_window() {
		let bump = Bump::new();
		let source = r#"window rolling { count(*), avg(value) } with { count: 10 } by { user_id }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Rolling);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "count");
		assert_eq!(window.group_by.len(), 1);
		assert_eq!(window.aggregations.len(), 2);
	}

	#[test]
	fn test_parse_rolling_time_window() {
		let bump = Bump::new();
		let source = r#"window rolling { sum(amount) } with { interval: "5m" }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Rolling);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "interval");
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_session_window() {
		let bump = Bump::new();
		let source = r#"window session { count(*) } with { gap: "10m" } by { user_id }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let window = result[0].first_unchecked().as_window();

		assert_eq!(window.kind, AstWindowKind::Session);
		assert_eq!(window.config.len(), 1);
		assert_eq!(window.config[0].key.text(), "gap");
		assert_eq!(window.group_by.len(), 1);
		assert_eq!(window.aggregations.len(), 1);
	}

	#[test]
	fn test_parse_bare_window_is_error() {
		let bump = Bump::new();
		let source = r#"window { count(*) } with { interval: "5m" }"#;
		let tokens = tokenize(&bump, source).unwrap().into_iter().collect();
		let mut parser = Parser::new(&bump, source, tokens);
		let err = parser.parse().unwrap_err();
		let msg = err.to_string();

		assert!(
			msg.contains("window kind (tumbling, sliding, rolling, session)"),
			"expected error about missing window kind, got: {msg}"
		);
	}
}
