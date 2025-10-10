// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod aggregate;
mod alter;
mod apply;
mod call;
mod cast;
mod conditional;
mod create;
mod create_index;
mod delete;
mod describe;
mod distinct;
mod extend;
mod filter;
mod from;
mod identifier;
mod infix;
mod inline;
mod insert;
mod join;
mod r#let;
mod list;
mod literal;
mod map;
mod policy;
mod prefix;
mod primary;
mod select;
mod sort;
pub mod sub_query;
mod take;
mod tuple;
mod update;
mod window;

use std::cmp::PartialOrd;

use reifydb_core::return_error;
use reifydb_type::diagnostic::ast;

use crate::ast::{
	Ast, AstInfix, AstStatement, InfixOperator,
	tokenize::{Keyword, Literal, Operator, Separator, Separator::NewLine, Token, TokenKind},
};

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) enum Precedence {
	None,
	Assignment,
	LogicOr,
	LogicAnd,
	Comparison,
	Term,
	Factor,
	Prefix,
	Call,
	Primary,
}

// Compile-time precedence lookup using const evaluation
const fn get_precedence_for_operator(op: Operator) -> Precedence {
	use Operator::*;
	use Precedence::*;

	match op {
		As => Assignment,
		ColonEqual => Assignment,
		Equal | DoubleEqual | BangEqual | LeftAngle | LeftAngleEqual | RightAngle | RightAngleEqual => {
			Comparison
		}
		Plus | Minus => Term,
		Asterisk | Slash | Percent => Factor,
		OpenParen => Call,
		Dot | DoubleColon | Arrow => Primary,
		Colon => Assignment,
		Or | Xor => LogicOr,
		And => LogicAnd,
		_ => None,
	}
}

pub fn parse<'a>(tokens: Vec<Token<'a>>) -> crate::Result<Vec<AstStatement<'a>>> {
	let mut parser = Parser::new(tokens);
	parser.parse()
}

pub(crate) struct Parser<'a> {
	tokens: Vec<Token<'a>>,
	position: usize,
}

impl<'a> Parser<'a> {
	fn new(tokens: Vec<Token<'a>>) -> Self {
		Self {
			tokens,
			position: 0,
		}
	}

	fn parse(&mut self) -> crate::Result<Vec<AstStatement<'a>>> {
		let mut result = Vec::with_capacity(4);
		loop {
			if self.is_eof() {
				break;
			}

			result.push(self.parse_statement()?);
		}
		Ok(result)
	}

	/// Parse a single statement (possibly with pipes)
	pub(crate) fn parse_statement(&mut self) -> crate::Result<AstStatement<'a>> {
		let mut nodes = Vec::with_capacity(8);
		let mut has_pipes = false;
		loop {
			if self.is_eof() || self.consume_if(TokenKind::Separator(Separator::Semicolon))?.is_some() {
				break;
			}
			nodes.push(self.parse_node(Precedence::None)?);
			if !self.is_eof() {
				// Check for pipe operator or newline as
				// separator
				if self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					self.consume_if(TokenKind::Separator(NewLine))?;
				}
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
		})
	}

	/// Parse statement content without handling termination (for use within other constructs)
	pub(crate) fn parse_statement_content(&mut self) -> crate::Result<AstStatement<'a>> {
		let mut nodes = Vec::with_capacity(8);
		let mut has_pipes = false;
		loop {
			// Don't check for semicolon termination - that's handled by the outer context
			if self.is_eof() {
				break;
			}

			// Check if we hit a semicolon - if so, stop but don't consume it
			if let Ok(current) = self.current() {
				if current.is_separator(Separator::Semicolon) {
					break;
				}
			}

			nodes.push(self.parse_node(Precedence::None)?);
			if !self.is_eof() {
				// Check for pipe operator or newline as separator
				if self.current()?.is_operator(Operator::Pipe) {
					self.advance()?; // consume the pipe
					has_pipes = true;
				} else {
					self.consume_if(TokenKind::Separator(NewLine))?;
				}
			}
		}

		Ok(AstStatement {
			nodes,
			has_pipes,
		})
	}

	pub(crate) fn parse_node(&mut self, precedence: Precedence) -> crate::Result<Ast<'a>> {
		let mut left = self.parse_primary()?;

		while !self.is_eof() {
			if precedence >= self.current_precedence()? {
				break;
			}

			// Check token type before consuming
			let is_between = if let Ok(current) = self.current() {
				matches!(current.kind, TokenKind::Keyword(Keyword::Between))
			} else {
				break;
			};

			if is_between {
				left = Ast::Between(self.parse_between(left)?);
			} else {
				left = Ast::Infix(self.parse_infix(left)?);
			}
		}
		Ok(left)
	}

	pub(crate) fn advance(&mut self) -> crate::Result<Token<'a>> {
		if self.position >= self.tokens.len() {
			return Err(reifydb_type::Error(ast::unexpected_eof_error()));
		}
		let token = self.tokens[self.position].clone();
		self.position += 1;
		Ok(token)
	}

	pub(crate) fn consume(&mut self, expected: TokenKind) -> crate::Result<Token<'a>> {
		self.current_expect(expected)?;
		self.advance()
	}

	pub(crate) fn consume_if(&mut self, expected: TokenKind) -> crate::Result<Option<Token<'a>>> {
		if self.is_eof() || self.current()?.kind != expected {
			return Ok(None);
		}

		Ok(Some(self.consume(expected)?))
	}

	pub(crate) fn consume_while(&mut self, expected: TokenKind) -> crate::Result<()> {
		loop {
			if self.is_eof() || self.current()?.kind != expected {
				return Ok(());
			}
			self.advance()?;
		}
	}

	pub(crate) fn consume_literal(&mut self, expected: Literal) -> crate::Result<Token<'a>> {
		self.current_expect_literal(expected)?;
		self.advance()
	}

	pub(crate) fn consume_operator(&mut self, expected: Operator) -> crate::Result<Token<'a>> {
		self.current_expect_operator(expected)?;
		self.advance()
	}

	pub(crate) fn consume_keyword(&mut self, expected: Keyword) -> crate::Result<Token<'a>> {
		self.current_expect_keyword(expected)?;
		self.advance()
	}

	pub(crate) fn current(&self) -> crate::Result<&Token<'a>> {
		if self.position >= self.tokens.len() {
			return Err(reifydb_type::Error(ast::unexpected_eof_error()));
		}
		Ok(&self.tokens[self.position])
	}

	pub(crate) fn current_expect(&self, expected: TokenKind) -> crate::Result<()> {
		let got = self.current()?;
		if got.kind == expected {
			Ok(())
		} else {
			// Use specific error for identifier expectations to
			// match test format
			if let TokenKind::Identifier = expected {
				return_error!(ast::expected_identifier_error(got.clone().fragment))
			} else {
				return_error!(ast::unexpected_token_error(
					&format!("{:?}", expected),
					got.fragment.clone()
				))
			}
		}
	}

	pub(crate) fn current_expect_literal(&self, literal: Literal) -> crate::Result<()> {
		self.current_expect(TokenKind::Literal(literal))
	}

	pub(crate) fn current_expect_operator(&self, operator: Operator) -> crate::Result<()> {
		self.current_expect(TokenKind::Operator(operator))
	}

	pub(crate) fn current_expect_keyword(&self, keyword: Keyword) -> crate::Result<()> {
		self.current_expect(TokenKind::Keyword(keyword))
	}

	pub(crate) fn current_precedence(&self) -> crate::Result<Precedence> {
		if self.is_eof() {
			return Ok(Precedence::None);
		};

		let current = self.current()?;
		match current.kind {
			TokenKind::Operator(operator) => Ok(get_precedence_for_operator(operator)),
			TokenKind::Keyword(Keyword::Between) => Ok(Precedence::Comparison),
			_ => Ok(Precedence::None),
		}
	}

	pub(crate) fn is_eof(&self) -> bool {
		self.position >= self.tokens.len()
	}

	/// Look ahead from current position to find a pipe operator (|)
	/// Returns true if pipe found before semicolon or EOF
	/// Returns false if semicolon or EOF found first
	pub(crate) fn has_pipe_ahead(&self) -> bool {
		let mut pos = self.position;

		while pos < self.tokens.len() {
			let token = &self.tokens[pos];
			match token.kind {
				TokenKind::Operator(Operator::Pipe) => return true,
				TokenKind::Separator(Separator::Semicolon) => return false,
				_ => pos += 1,
			}
		}

		// Reached EOF without finding pipe or semicolon
		false
	}

	pub(crate) fn skip_new_line(&mut self) -> crate::Result<()> {
		self.consume_while(TokenKind::Separator(NewLine))?;
		Ok(())
	}

	pub(crate) fn parse_between(&mut self, value: Ast<'a>) -> crate::Result<crate::ast::AstBetween<'a>> {
		let token = self.consume_keyword(Keyword::Between)?;
		let lower = Box::new(self.parse_node(Precedence::Comparison)?);
		self.consume_operator(Operator::And)?;
		let upper = Box::new(self.parse_node(Precedence::Comparison)?);

		Ok(crate::ast::AstBetween {
			token,
			value: Box::new(value),
			lower,
			upper,
		})
	}

	/// Parse a comma-separated list of expressions with optional braces
	/// Returns (nodes, had_braces) tuple
	pub(crate) fn parse_expressions(&mut self, allow_colon_alias: bool) -> crate::Result<(Vec<Ast<'a>>, bool)> {
		let has_braces = self.current()?.is_operator(Operator::OpenCurly);

		if has_braces {
			self.advance()?; // consume opening brace
		}

		let mut nodes = Vec::with_capacity(4);
		loop {
			if allow_colon_alias {
				if let Ok(alias_expr) = self.try_parse_colon_alias() {
					nodes.push(alias_expr);
				} else {
					nodes.push(self.parse_node(Precedence::None)?);
				}
			} else {
				nodes.push(self.parse_node(Precedence::None)?);
			}

			if self.is_eof() {
				break;
			}

			// consume comma and continue
			if self.current()?.is_separator(Separator::Comma) {
				self.advance()?;
			} else if has_braces && self.current()?.is_operator(Operator::CloseCurly) {
				// If we have braces, look for closing brace
				self.advance()?; // consume closing brace
				break;
			} else {
				break;
			}
		}

		Ok((nodes, has_braces))
	}

	/// Try to parse "identifier: expression" syntax and convert it to
	/// "expression AS identifier"
	pub(crate) fn try_parse_colon_alias(&mut self) -> crate::Result<Ast<'a>> {
		// Check if we have enough tokens from current position
		if self.position + 1 >= self.tokens.len() {
			return_error!(ast::unsupported_token_error(self.current()?.clone().fragment));
		}

		// Check if current token is identifier
		if !self.tokens[self.position].is_identifier() {
			return_error!(ast::unsupported_token_error(self.current()?.clone().fragment));
		}

		// Check if next token is colon
		if !self.tokens[self.position + 1].is_operator(Operator::Colon) {
			return_error!(ast::unsupported_token_error(self.current()?.clone().fragment));
		}

		// Parse the identifier and consume colon
		let identifier = self.parse_as_identifier()?;
		let colon_token = self.advance()?; // consume colon

		// Parse the expression
		let expression = self.parse_node(Precedence::None)?;

		// Return as "expression AS identifier"
		Ok(Ast::Infix(AstInfix {
			token: expression.token().clone(),
			left: Box::new(expression),
			operator: InfixOperator::As(colon_token),
			right: Box::new(Ast::Identifier(identifier)),
		}))
	}
}

#[cfg(test)]
mod tests {
	use diagnostic::ast;
	use reifydb_core::Error;
	use reifydb_type::{diagnostic, err};

	use crate::ast::{
		parse::{Parser, Precedence, Precedence::Term},
		tokenize::{
			Literal::{False, Number, True},
			Operator::Plus,
			Separator::Semicolon,
			TokenKind,
			TokenKind::{Identifier, Literal, Separator},
			tokenize,
		},
	};

	#[test]
	fn test_advance_but_eof() {
		let mut parser = Parser::new(vec![]);
		let result = parser.advance();
		assert_eq!(result, err!(ast::unexpected_eof_error()))
	}

	#[test]
	fn test_advance() {
		let tokens = tokenize("1 + 2").unwrap();
		let mut parser = Parser::new(tokens);

		let one = parser.advance().unwrap();
		assert_eq!(one.kind, Literal(Number));
		assert_eq!(one.fragment.text(), "1");

		let plus = parser.advance().unwrap();
		assert_eq!(plus.kind, TokenKind::Operator(Plus));
		assert_eq!(plus.fragment.text(), "+");

		let two = parser.advance().unwrap();
		assert_eq!(two.kind, Literal(Number));
		assert_eq!(two.fragment.text(), "2");
	}

	#[test]
	fn test_consume_but_eof() {
		let tokens = tokenize("").unwrap();
		let mut parser = Parser::new(tokens);
		let err = parser.consume(Identifier).err().unwrap();
		assert_eq!(err, Error(ast::unexpected_eof_error()))
	}

	#[test]
	fn test_consume_but_unexpected_token() {
		let tokens = tokenize("false").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.consume(Literal(True));
		assert!(result.is_err());

		// Pattern matching no longer works with unified error system
		// Just verify it's an error for now
		assert!(result.is_err());
	}

	#[test]
	fn test_consume() {
		let tokens = tokenize("true 99").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.consume(Literal(True)).unwrap();
		assert_eq!(result.kind, Literal(True));

		let result = parser.consume(Literal(Number)).unwrap();
		assert_eq!(result.kind, Literal(Number));
	}

	#[test]
	fn test_consume_if_but_eof() {
		let tokens = tokenize("").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.consume_if(Literal(True));
		assert_eq!(result, Ok(None))
	}

	#[test]
	fn test_consume_if_but_unexpected_token() {
		let tokens = tokenize("false").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.consume_if(Literal(True));
		assert_eq!(result, Ok(None));
	}

	#[test]
	fn test_consume_if() {
		let tokens = tokenize("true 0x99").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.consume_if(Literal(True)).unwrap().unwrap();
		assert_eq!(result.kind, Literal(True));

		let result = parser.consume_if(Literal(Number)).unwrap().unwrap();
		assert_eq!(result.kind, Literal(Number));
	}

	#[test]
	fn test_current_but_eof() {
		let tokens = tokenize("").unwrap();
		let parser = Parser::new(tokens);
		let result = parser.current();
		assert_eq!(result, err!(ast::unexpected_eof_error()))
	}

	#[test]
	fn test_semicolon_statement_separation() {
		use crate::ast::tokenize::tokenize;
		let tokens = tokenize("let $x := 1; FROM users").unwrap();
		let mut parser = Parser::new(tokens);
		let statements = parser.parse().unwrap();
		assert_eq!(statements.len(), 2, "Should parse two separate statements");

		// First statement should be the let assignment
		let first_stmt = &statements[0];
		assert_eq!(first_stmt.nodes.len(), 1);
		assert!(matches!(first_stmt.nodes[0], crate::ast::Ast::Let(_)));

		// Second statement should be the FROM
		let second_stmt = &statements[1];
		assert_eq!(second_stmt.nodes.len(), 1);
		assert!(matches!(second_stmt.nodes[0], crate::ast::Ast::From(_)));
	}

	#[test]
	fn test_variable_multiline_separation() {
		use crate::ast::tokenize::tokenize;
		let sql = r#"
		let $user_data := FROM [{ name: "Alice", age: 25 }, { name: "Bob", age: 17 }, { name: "Carol", age: 30 }] | FILTER age > 21;
		FROM $user_data
		"#;
		let tokens = tokenize(sql).unwrap();
		let mut parser = Parser::new(tokens);
		let statements = parser.parse().unwrap();
		assert_eq!(statements.len(), 2, "Should parse two separate statements from multiline input");

		// First statement should be the let assignment
		let first_stmt = &statements[0];
		assert_eq!(first_stmt.nodes.len(), 1);
		assert!(matches!(first_stmt.nodes[0], crate::ast::Ast::Let(_)));

		// Second statement should be the FROM with variable
		let second_stmt = &statements[1];
		assert_eq!(second_stmt.nodes.len(), 1);
		if let crate::ast::Ast::From(from_ast) = &second_stmt.nodes[0] {
			assert!(matches!(from_ast, crate::ast::AstFrom::Variable { .. }));
		} else {
			panic!("Expected FROM statement with variable");
		}
	}

	#[test]
	fn test_current() {
		let tokens = tokenize("true false").unwrap();
		let mut parser = Parser::new(tokens);

		let true_token = parser.current().unwrap().clone();
		assert_eq!(true_token.kind, Literal(True));

		parser.advance().unwrap();

		let false_token = parser.current().unwrap().clone();
		assert_eq!(false_token.kind, Literal(False));
	}

	#[test]
	fn test_current_expect_but_eof() {
		let tokens = tokenize("").unwrap();
		let parser = Parser::new(tokens);
		let result = parser.current_expect(Separator(Semicolon));
		assert_eq!(result, err!(ast::unexpected_eof_error()))
	}

	#[test]
	fn test_current_expect() {
		let tokens = tokenize("true false").unwrap();
		let mut parser = Parser::new(tokens);

		let result = parser.current_expect(Literal(True));
		assert!(result.is_ok());

		parser.advance().unwrap();

		let result = parser.current_expect(Literal(False));
		assert!(result.is_ok());
	}

	#[test]
	fn test_current_expect_but_different() {
		let tokens = tokenize("true").unwrap();
		let parser = Parser::new(tokens);

		let result = parser.current_expect(Literal(False));
		assert!(result.is_err());

		// Pattern matching no longer works with unified error system
		// Just verify it's an error for now
		assert!(result.is_err());
	}

	#[test]
	fn test_current_precedence_but_eof() {
		let tokens = tokenize("").unwrap();
		let parser = Parser::new(tokens);
		let result = parser.current_precedence();
		assert_eq!(result, Ok(Precedence::None))
	}

	#[test]
	fn test_current_precedence() {
		let tokens = tokenize("+").unwrap();
		let parser = Parser::new(tokens);
		let result = parser.current_precedence();
		assert_eq!(result, Ok(Term))
	}

	#[test]
	fn test_between_precedence() {
		let tokens = tokenize("BETWEEN").unwrap();
		let parser = Parser::new(tokens);
		let result = parser.current_precedence();
		assert_eq!(result, Ok(Precedence::Comparison))
	}

	#[test]
	fn test_parse_between_expression() {
		let tokens = tokenize("x BETWEEN 1 AND 10").unwrap();
		let result = crate::ast::parse::parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let between = result[0].first_unchecked().as_between();
		assert_eq!(between.value.as_identifier().text(), "x");
		assert_eq!(between.lower.as_literal_number().value(), "1");
		assert_eq!(between.upper.as_literal_number().value(), "10");
	}

	#[test]
	fn test_pipe_operator_simple() {
		use crate::ast::Ast;
		let tokens = tokenize("from users | sort name").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 2);

		// First operation should be From
		assert!(matches!(statement[0], Ast::From(_)));
		// Second operation should be Sort
		assert!(matches!(statement[1], Ast::Sort(_)));
	}

	#[test]
	fn test_pipe_operator_multiple() {
		use crate::ast::Ast;
		let tokens = tokenize("from users | filter age > 18 | sort name | take 10").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 4);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Filter(_)));
		assert!(matches!(statement[2], Ast::Sort(_)));
		assert!(matches!(statement[3], Ast::Take(_)));
	}

	#[test]
	fn test_pipe_with_system_tables() {
		use crate::ast::Ast;
		let tokens = tokenize("from system.tables | sort id").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 2);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Sort(_)));
	}

	#[test]
	fn test_newline_still_works() {
		use crate::ast::Ast;
		let tokens = tokenize("from users\nsort name").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 2);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Sort(_)));
	}

	#[test]
	fn test_mixed_pipe_and_newline() {
		use crate::ast::Ast;
		let tokens = tokenize("from users | filter age > 18\nsort name | take 10").unwrap();
		let mut parser = Parser::new(tokens);
		let result = parser.parse().unwrap();

		assert_eq!(result.len(), 1);
		let statement = &result[0];
		assert_eq!(statement.len(), 4);

		assert!(matches!(statement[0], Ast::From(_)));
		assert!(matches!(statement[1], Ast::Filter(_)));
		assert!(matches!(statement[2], Ast::Sort(_)));
		assert!(matches!(statement[3], Ast::Take(_)));
	}
}
