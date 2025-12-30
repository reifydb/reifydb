// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use thiserror::Error;

use super::{
	ast::*,
	token::{Span, Token, TokenKind},
};
use crate::expr::{BinaryOp, UnaryOp};

/// Parser error types.
#[derive(Debug, Clone, Error)]
pub enum ParseError {
	#[error("unexpected token: expected {expected}, found '{found}' at {line}:{column}")]
	UnexpectedToken {
		expected: String,
		found: String,
		line: u32,
		column: u32,
	},

	#[error("unexpected end of input: expected {expected}")]
	UnexpectedEof {
		expected: String,
	},

	#[error("invalid pipeline: must start with 'scan'")]
	InvalidPipelineStart {
		span: Span,
	},

	#[error("empty column list in select")]
	EmptyColumnList {
		span: Span,
	},

	#[error("invalid take limit: expected positive integer")]
	InvalidTakeLimit {
		span: Span,
	},
}

/// Operator precedence levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
	None = 0,
	Or = 1,       // or, ||
	And = 2,      // and, &&
	Equality = 3, // ==, !=
	Compare = 4,  // <, <=, >, >=
	Term = 5,     // +, -
	Factor = 6,   // *, /
	Unary = 7,    // not, -
	Primary = 8,
}

impl Precedence {
	fn for_binary_op(token: &TokenKind) -> Option<Precedence> {
		match token {
			TokenKind::Or => Some(Precedence::Or),
			TokenKind::And => Some(Precedence::And),
			TokenKind::Eq | TokenKind::Ne => Some(Precedence::Equality),
			TokenKind::Lt | TokenKind::Le | TokenKind::Gt | TokenKind::Ge => Some(Precedence::Compare),
			TokenKind::Plus | TokenKind::Minus => Some(Precedence::Term),
			TokenKind::Star | TokenKind::Slash => Some(Precedence::Factor),
			_ => None,
		}
	}

	fn next(self) -> Precedence {
		match self {
			Precedence::None => Precedence::Or,
			Precedence::Or => Precedence::And,
			Precedence::And => Precedence::Equality,
			Precedence::Equality => Precedence::Compare,
			Precedence::Compare => Precedence::Term,
			Precedence::Term => Precedence::Factor,
			Precedence::Factor => Precedence::Unary,
			Precedence::Unary => Precedence::Primary,
			Precedence::Primary => Precedence::Primary,
		}
	}
}

/// Recursive descent parser for the DSL.
pub struct Parser {
	tokens: Vec<Token>,
	position: usize,
}

impl Parser {
	/// Create a new parser.
	pub fn new(tokens: Vec<Token>) -> Self {
		Self {
			tokens,
			position: 0,
		}
	}

	/// Parse the token stream into a DSL AST.
	/// For backwards compatibility, this parses a single pipeline.
	pub fn parse(&mut self) -> Result<DslAst, ParseError> {
		self.parse_program()
	}

	/// Parse a complete program (multiple statements).
	pub fn parse_program(&mut self) -> Result<DslAst, ParseError> {
		let start_span = self.current().span;
		let mut statements = Vec::new();
		let mut end_span = start_span;

		while !self.at_end() {
			let stmt = self.parse_statement()?;
			end_span = stmt.span();
			statements.push(stmt);
		}

		Ok(DslAst {
			statements,
			span: start_span.merge(&end_span),
		})
	}

	/// Parse a single statement.
	fn parse_statement(&mut self) -> Result<StatementAst, ParseError> {
		let token = self.current().clone();

		match &token.kind {
			TokenKind::Let => self.parse_let(),
			TokenKind::Fn => self.parse_fn(),
			TokenKind::If => self.parse_if(),
			TokenKind::Loop => self.parse_loop(),
			TokenKind::Break => self.parse_break(),
			TokenKind::Continue => self.parse_continue(),
			TokenKind::For => self.parse_for(),
			TokenKind::Scan | TokenKind::Inline => {
				// Pipeline starting with scan or inline
				let pipeline = self.parse_pipeline()?;
				Ok(StatementAst::Pipeline(pipeline))
			}
			TokenKind::Dollar => {
				// Could be assignment ($var = expr), expression ($var * 2), or pipeline ($var | ...)
				if self.is_assignment_lookahead() {
					self.parse_assign()
				} else if self.is_variable_expression_lookahead() {
					// Variable in expression context: $x * 2, $x.field, etc.
					let expr = self.parse_expr()?;
					Ok(StatementAst::Expression(ExpressionAst {
						span: expr.span(),
						expr,
					}))
				} else {
					// Pipeline starting with a variable reference
					let pipeline = self.parse_pipeline_from_variable()?;
					Ok(StatementAst::Pipeline(pipeline))
				}
			}
			TokenKind::Ident => {
				// Could be module call (console::log), function call, or pipeline
				if self.peek_is(&TokenKind::ColonColon) {
					self.parse_module_call()
				} else if self.peek_is(&TokenKind::LParen) {
					self.parse_call()
				} else {
					// Treat as pipeline (could be a stage keyword or identifier)
					let pipeline = self.parse_pipeline()?;
					Ok(StatementAst::Pipeline(pipeline))
				}
			}
			_ => {
				// Check if this looks like a bare expression (literal, parenthesized)
				if self.is_bare_expression_start() {
					let expr = self.parse_expr()?;
					return Ok(StatementAst::Expression(ExpressionAst {
						span: expr.span(),
						expr,
					}));
				}
				// Try to parse as pipeline
				let pipeline = self.parse_pipeline()?;
				Ok(StatementAst::Pipeline(pipeline))
			}
		}
	}

	/// Parse a let statement: "let" "$" IDENT "=" value
	/// Value can be an expression (e.g., `$user.id`, `42`) or a pipeline (e.g., `scan users`)
	fn parse_let(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Let)?;
		self.expect(&TokenKind::Dollar)?;

		let name_token = self.expect_ident()?;
		let name = name_token.text.clone();

		self.expect(&TokenKind::Assign)?;

		// Determine if value is expression or pipeline
		let value = if self.is_expression_start() {
			let expr = self.parse_expr()?;
			LetValue::Expr(expr)
		} else {
			let stmt = self.parse_statement()?;
			match stmt {
				StatementAst::Pipeline(p) => LetValue::Pipeline(Box::new(p)),
				_ => {
					let span = stmt.span();
					return Err(ParseError::UnexpectedToken {
						expected: "pipeline or expression".to_string(),
						found: "statement".to_string(),
						line: span.line,
						column: span.column,
					});
				}
			}
		};

		let end_span = value.span();

		Ok(StatementAst::Let(LetAst {
			name,
			value,
			span: start.merge(&end_span),
		}))
	}

	/// Check if current position starts a bare expression that can be a statement.
	/// Returns true for literals and parenthesized expressions (not pipelines).
	fn is_bare_expression_start(&self) -> bool {
		match &self.current().kind {
			// Literals are always expressions - can't be pipeline
			TokenKind::Int(_)
			| TokenKind::Float(_)
			| TokenKind::String(_)
			| TokenKind::Bool(_)
			| TokenKind::Null => true,
			// Parenthesized expression - can't be pipeline start
			TokenKind::LParen => true,
			// Unary operators
			TokenKind::Minus | TokenKind::Not => true,
			_ => false,
		}
	}

	/// Check if current position starts an expression (not a pipeline).
	/// Returns true for literals, parenthesized expressions, and variable field access.
	fn is_expression_start(&self) -> bool {
		match &self.current().kind {
			TokenKind::Dollar => {
				// $var.field or $var + 1 is expression
				// $var | ... or $var alone is pipeline
				self.is_variable_expression_lookahead()
			}
			TokenKind::Int(_)
			| TokenKind::Float(_)
			| TokenKind::String(_)
			| TokenKind::Bool(_)
			| TokenKind::Null
			| TokenKind::LParen
			| TokenKind::Minus
			| TokenKind::Not => true,
			_ => false,
		}
	}

	/// Check if we're looking at a variable used in an expression context.
	/// Returns true for: $var.field, $var + 1, $var == 1, etc.
	/// Returns false for: $var | filter, $var alone (pipeline usage)
	fn is_variable_expression_lookahead(&self) -> bool {
		// We're at $ - check what follows $ IDENT
		let pos = self.position;
		if !matches!(self.tokens.get(pos).map(|t| &t.kind), Some(TokenKind::Dollar)) {
			return false;
		}
		if !matches!(self.tokens.get(pos + 1).map(|t| &t.kind), Some(TokenKind::Ident)) {
			return false;
		}
		// Check token after $ IDENT - is it an expression operator?
		match self.tokens.get(pos + 2).map(|t| &t.kind) {
			Some(TokenKind::Dot) => true,   // $var.field
			Some(TokenKind::Plus) => true,  // $var + ...
			Some(TokenKind::Minus) => true, // $var - ...
			Some(TokenKind::Star) => true,  // $var * ...
			Some(TokenKind::Slash) => true, // $var / ...
			Some(TokenKind::Eq) => true,    // $var == ...
			Some(TokenKind::Ne) => true,    // $var != ...
			Some(TokenKind::Lt) => true,    // $var < ...
			Some(TokenKind::Le) => true,    // $var <= ...
			Some(TokenKind::Gt) => true,    // $var > ...
			Some(TokenKind::Ge) => true,    // $var >= ...
			Some(TokenKind::And) => true,   // $var and ...
			Some(TokenKind::Or) => true,    // $var or ...
			_ => false,                     // $var | ... or $var alone -> pipeline
		}
	}

	/// Check if we're looking at an assignment pattern: $ IDENT =
	fn is_assignment_lookahead(&self) -> bool {
		let pos = self.position;
		if !matches!(self.tokens.get(pos).map(|t| &t.kind), Some(TokenKind::Dollar)) {
			return false;
		}
		if !matches!(self.tokens.get(pos + 1).map(|t| &t.kind), Some(TokenKind::Ident)) {
			return false;
		}
		matches!(self.tokens.get(pos + 2).map(|t| &t.kind), Some(TokenKind::Assign))
	}

	/// Check if the current token starts a pipeline (for subquery detection).
	fn is_pipeline_start(&self) -> bool {
		matches!(self.current().kind, TokenKind::Scan | TokenKind::Inline | TokenKind::Dollar)
	}

	/// Parse assignment: $var = expr
	fn parse_assign(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Dollar)?;

		let name_token = self.expect_ident()?;
		let name = name_token.text.clone();

		self.expect(&TokenKind::Assign)?;

		// Parse the expression value
		let value = self.parse_expr()?;
		let end_span = value.span();

		Ok(StatementAst::Assign(AssignAst {
			name,
			value,
			span: start.merge(&end_span),
		}))
	}

	/// Parse a function definition: "fn" IDENT "(" params ")" "{" body "}"
	fn parse_fn(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Fn)?;

		let name_token = self.expect_ident()?;
		let name = name_token.text.clone();

		// Parameters
		self.expect(&TokenKind::LParen)?;
		let mut parameters = Vec::new();

		if !self.check(&TokenKind::RParen) {
			loop {
				let param_start = self.current().span;
				let param_name_token = self.expect_ident()?;
				let param_name = param_name_token.text.clone();

				let param_type = if self.check(&TokenKind::Colon) {
					self.advance();
					let type_token = self.expect_ident()?;
					Some(type_token.text.clone())
				} else {
					None
				};

				parameters.push(ParameterAst {
					name: param_name,
					param_type,
					span: param_start.merge(&self.previous().span),
				});

				if !self.check(&TokenKind::Comma) {
					break;
				}
				self.advance();
			}
		}
		self.expect(&TokenKind::RParen)?;

		// Body
		self.expect(&TokenKind::LBrace)?;
		let mut body = Vec::new();

		while !self.check(&TokenKind::RBrace) && !self.at_end() {
			body.push(self.parse_statement()?);
		}

		let end_token = self.expect(&TokenKind::RBrace)?;

		Ok(StatementAst::Def(DefAst {
			name,
			parameters,
			body,
			span: start.merge(&end_token.span),
		}))
	}

	/// Parse an if statement: "if" expr "{" body "}" ["else" ("if" ... | "{" body "}")]
	fn parse_if(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::If)?;

		let condition = self.parse_expr()?;

		self.expect(&TokenKind::LBrace)?;
		let mut then_branch = Vec::new();
		while !self.check(&TokenKind::RBrace) && !self.at_end() {
			then_branch.push(self.parse_statement()?);
		}
		let then_end = self.expect(&TokenKind::RBrace)?;
		let mut end_span = then_end.span;

		let else_branch = if self.check(&TokenKind::Else) {
			self.advance();
			// Check for "else if" chain
			if self.check(&TokenKind::If) {
				let else_if = self.parse_if()?;
				end_span = else_if.span();
				Some(vec![else_if])
			} else {
				self.expect(&TokenKind::LBrace)?;
				let mut else_stmts = Vec::new();
				while !self.check(&TokenKind::RBrace) && !self.at_end() {
					else_stmts.push(self.parse_statement()?);
				}
				let else_end = self.expect(&TokenKind::RBrace)?;
				end_span = else_end.span;
				Some(else_stmts)
			}
		} else {
			None
		};

		Ok(StatementAst::If(IfAst {
			condition,
			then_branch,
			else_branch,
			span: start.merge(&end_span),
		}))
	}

	/// Parse a function call: IDENT "(" args ")"
	fn parse_call(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		let name_token = self.expect_ident()?;
		let function_name = name_token.text.clone();

		self.expect(&TokenKind::LParen)?;
		let mut arguments = Vec::new();

		if !self.check(&TokenKind::RParen) {
			loop {
				arguments.push(self.parse_expr()?);
				if !self.check(&TokenKind::Comma) {
					break;
				}
				self.advance();
			}
		}

		let end_token = self.expect(&TokenKind::RParen)?;

		Ok(StatementAst::Call(CallAst {
			function_name,
			arguments,
			span: start.merge(&end_token.span),
		}))
	}

	/// Parse a module function call: IDENT "::" IDENT "(" args ")"
	fn parse_module_call(&mut self) -> Result<StatementAst, ParseError> {
		use super::ast::ModuleCallAst;

		let start = self.current().span;
		let module_token = self.expect_ident()?;
		let module = module_token.text.clone();

		self.expect(&TokenKind::ColonColon)?;

		let function_token = self.expect_ident()?;
		let function = function_token.text.clone();

		self.expect(&TokenKind::LParen)?;
		let mut arguments = Vec::new();

		if !self.check(&TokenKind::RParen) {
			loop {
				arguments.push(self.parse_expr()?);
				if !self.check(&TokenKind::Comma) {
					break;
				}
				self.advance();
			}
		}

		let end_token = self.expect(&TokenKind::RParen)?;

		Ok(StatementAst::ModuleCall(ModuleCallAst {
			module,
			function,
			arguments,
			span: start.merge(&end_token.span),
		}))
	}

	/// Parse a loop statement: "loop" "{" body "}"
	fn parse_loop(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Loop)?;
		self.expect(&TokenKind::LBrace)?;

		let mut body = Vec::new();
		while !self.check(&TokenKind::RBrace) && !self.at_end() {
			body.push(self.parse_statement()?);
		}

		let end = self.expect(&TokenKind::RBrace)?;

		Ok(StatementAst::Loop(LoopAst {
			body,
			span: start.merge(&end.span),
		}))
	}

	/// Parse a break statement: "break"
	fn parse_break(&mut self) -> Result<StatementAst, ParseError> {
		let token = self.expect(&TokenKind::Break)?;
		Ok(StatementAst::Break(BreakAst {
			span: token.span,
		}))
	}

	/// Parse a continue statement: "continue"
	fn parse_continue(&mut self) -> Result<StatementAst, ParseError> {
		let token = self.expect(&TokenKind::Continue)?;
		Ok(StatementAst::Continue(ContinueAst {
			span: token.span,
		}))
	}

	/// Parse a for loop: "for" "$" IDENT "in" statement "{" body "}"
	fn parse_for(&mut self) -> Result<StatementAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::For)?;
		self.expect(&TokenKind::Dollar)?;

		let name_token = self.expect_ident()?;
		let variable = name_token.text.clone();

		self.expect(&TokenKind::In)?;

		// Parse the iterable (pipeline or expression)
		let iterable = self.parse_statement()?;

		self.expect(&TokenKind::LBrace)?;

		let mut body = Vec::new();
		while !self.check(&TokenKind::RBrace) && !self.at_end() {
			body.push(self.parse_statement()?);
		}

		let end = self.expect(&TokenKind::RBrace)?;

		Ok(StatementAst::For(ForAst {
			variable,
			iterable: Box::new(iterable),
			body,
			span: start.merge(&end.span),
		}))
	}

	/// Parse a pipeline that starts with a variable reference: "$" IDENT ("|" stage)*
	fn parse_pipeline_from_variable(&mut self) -> Result<PipelineAst, ParseError> {
		let start_span = self.current().span;

		// Parse the variable as a pseudo-stage that will be handled specially
		// For now, we'll wrap it - the compiler will handle $var as source
		let var_expr = self.parse_variable_expr()?;

		// Create a special "variable source" stage
		// We'll reuse ScanAst but mark it specially, or we need a new stage type
		// Actually, let's add this to the pipeline later during compilation
		// For now, store the variable name in a scan-like structure

		let var_name = match var_expr {
			ExprAst::Variable {
				name,
				..
			} => name,
			_ => unreachable!(),
		};

		let mut stages = vec![StageAst::Scan(ScanAst {
			table_name: format!("${}", var_name), // Prefix with $ to indicate variable
			span: start_span.merge(&self.previous().span),
		})];

		let mut end_span = stages[0].span();

		// Parse remaining stages
		while self.check(&TokenKind::Pipe) {
			self.advance();
			let stage = self.parse_stage()?;
			end_span = stage.span();
			stages.push(stage);
		}

		Ok(PipelineAst {
			stages,
			span: start_span.merge(&end_span),
		})
	}

	/// Parse a variable expression: "$" IDENT
	fn parse_variable_expr(&mut self) -> Result<ExprAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Dollar)?;

		let name_token = self.expect_ident()?;
		let name = name_token.text.clone();

		Ok(ExprAst::Variable {
			name,
			span: start.merge(&name_token.span),
		})
	}

	/// Parse a pipeline: stage ("|" stage)*
	fn parse_pipeline(&mut self) -> Result<PipelineAst, ParseError> {
		let mut stages = Vec::new();
		let start_span = self.current().span;

		// Parse first stage
		let first_stage = self.parse_stage()?;
		let mut end_span = first_stage.span();
		stages.push(first_stage);

		// Parse remaining stages
		while self.check(&TokenKind::Pipe) {
			self.advance();
			let stage = self.parse_stage()?;
			end_span = stage.span();
			stages.push(stage);
		}

		Ok(PipelineAst {
			stages,
			span: start_span.merge(&end_span),
		})
	}

	/// Parse a single stage.
	fn parse_stage(&mut self) -> Result<StageAst, ParseError> {
		let token = self.current().clone();

		match &token.kind {
			TokenKind::Scan => self.parse_scan(),
			TokenKind::Filter => self.parse_filter(),
			TokenKind::Select => self.parse_select(),
			TokenKind::Take => self.parse_take(),
			TokenKind::Extend => self.parse_extend(),
			TokenKind::Sort => self.parse_sort(),
			TokenKind::Inline => self.parse_inline(),
			_ => Err(ParseError::UnexpectedToken {
				expected: "stage keyword (scan, filter, select, take, extend, sort, inline)"
					.to_string(),
				found: token.text.clone(),
				line: token.span.line,
				column: token.span.column,
			}),
		}
	}

	/// Parse inline stage: "inline" - creates an empty pipeline
	fn parse_inline(&mut self) -> Result<StageAst, ParseError> {
		let span = self.current().span;
		self.expect(&TokenKind::Inline)?;

		// Use a special table name to indicate inline/empty
		Ok(StageAst::Scan(ScanAst {
			table_name: "$inline".to_string(),
			span,
		}))
	}

	/// Parse scan stage: "scan" IDENT or "scan" IDENT.IDENT (qualified name)
	fn parse_scan(&mut self) -> Result<StageAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Scan)?;

		let first_token = self.expect_ident()?;
		let mut table_name = first_token.text.clone();
		let mut end = first_token.span;

		// Check for qualified name (namespace.table)
		if self.check(&TokenKind::Dot) {
			self.advance();
			let second_token = self.expect_ident()?;
			table_name = format!("{}.{}", table_name, second_token.text);
			end = second_token.span;
		}

		Ok(StageAst::Scan(ScanAst {
			table_name,
			span: start.merge(&end),
		}))
	}

	/// Parse filter stage: "filter" expr
	fn parse_filter(&mut self) -> Result<StageAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Filter)?;

		let predicate = self.parse_expr()?;
		let end = predicate.span();

		Ok(StageAst::Filter(FilterAst {
			predicate,
			span: start.merge(&end),
		}))
	}

	/// Parse select stage: "select" "[" IDENT ("," IDENT)* "]"
	fn parse_select(&mut self) -> Result<StageAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Select)?;
		self.expect(&TokenKind::LBracket)?;

		let mut columns = Vec::new();

		// Handle empty list
		if !self.check(&TokenKind::RBracket) {
			loop {
				let col = self.expect_ident()?;
				columns.push(col.text.clone());

				if !self.check(&TokenKind::Comma) {
					break;
				}
				self.advance();
			}
		}

		let end_token = self.expect(&TokenKind::RBracket)?;

		if columns.is_empty() {
			return Err(ParseError::EmptyColumnList {
				span: start,
			});
		}

		Ok(StageAst::Select(SelectAst {
			columns,
			span: start.merge(&end_token.span),
		}))
	}

	/// Parse take stage: "take" INT
	fn parse_take(&mut self) -> Result<StageAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Take)?;

		let limit_token = self.current().clone();
		match &limit_token.kind {
			TokenKind::Int(n) if *n > 0 => {
				self.advance();
				Ok(StageAst::Take(TakeAst {
					limit: *n as u64,
					span: start.merge(&limit_token.span),
				}))
			}
			_ => Err(ParseError::InvalidTakeLimit {
				span: limit_token.span,
			}),
		}
	}

	/// Parse extend stage: "extend" "{" IDENT ":" expr ("," IDENT ":" expr)* "}"
	fn parse_extend(&mut self) -> Result<StageAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Extend)?;
		self.expect(&TokenKind::LBrace)?;

		let mut extensions = Vec::new();

		if !self.check(&TokenKind::RBrace) {
			loop {
				let name = self.expect_ident()?.text.clone();
				self.expect(&TokenKind::Colon)?;
				let expr = self.parse_expr()?;
				extensions.push((name, expr));

				if !self.check(&TokenKind::Comma) {
					break;
				}
				self.advance();
			}
		}

		let end_token = self.expect(&TokenKind::RBrace)?;

		Ok(StageAst::Extend(ExtendAst {
			extensions,
			span: start.merge(&end_token.span),
		}))
	}

	/// Parse sort stage: "sort" IDENT ["asc"|"desc"] ("," IDENT ["asc"|"desc"])*
	fn parse_sort(&mut self) -> Result<StageAst, ParseError> {
		let start = self.current().span;
		self.expect(&TokenKind::Sort)?;

		let mut columns = Vec::new();
		let mut end_span;

		loop {
			let col = self.expect_ident()?;
			end_span = col.span;
			let name = col.text.clone();

			// Check for asc/desc
			let order = if self.check(&TokenKind::Asc) {
				end_span = self.current().span;
				self.advance();
				SortOrder::Asc
			} else if self.check(&TokenKind::Desc) {
				end_span = self.current().span;
				self.advance();
				SortOrder::Desc
			} else {
				SortOrder::Asc // default
			};

			columns.push((name, order));

			if !self.check(&TokenKind::Comma) {
				break;
			}
			self.advance();
		}

		Ok(StageAst::Sort(SortAst {
			columns,
			span: start.merge(&end_span),
		}))
	}

	/// Parse an expression using precedence climbing.
	fn parse_expr(&mut self) -> Result<ExprAst, ParseError> {
		self.parse_expr_with_precedence(Precedence::Or)
	}

	/// Parse an expression with minimum precedence.
	fn parse_expr_with_precedence(&mut self, min_prec: Precedence) -> Result<ExprAst, ParseError> {
		let mut left = self.parse_unary()?;

		loop {
			// Check for IN / NOT IN (at Compare precedence)
			if min_prec <= Precedence::Compare {
				if let Some(in_result) = self.try_parse_in_expr(left.clone())? {
					left = in_result;
					continue;
				}
			}

			// Regular binary operators
			let Some(prec) = Precedence::for_binary_op(&self.current().kind) else {
				break;
			};
			if prec < min_prec {
				break;
			}

			let op_token = self.advance().clone();
			let op = token_to_binary_op(&op_token.kind)?;

			let right = self.parse_expr_with_precedence(prec.next())?;

			let span = left.span().merge(&right.span());
			left = ExprAst::BinaryOp {
				op,
				left: Box::new(left),
				right: Box::new(right),
				span,
			};
		}

		Ok(left)
	}

	/// Try to parse an IN or NOT IN expression. Returns None if not an IN expression.
	fn try_parse_in_expr(&mut self, left: ExprAst) -> Result<Option<ExprAst>, ParseError> {
		let start = left.span();
		let negated;

		// Check for "in" or "not in"
		if self.check(&TokenKind::In) {
			negated = false;
			self.advance();
		} else if self.check(&TokenKind::Not) && self.peek_is(&TokenKind::In) {
			negated = true;
			self.advance(); // consume 'not'
			self.advance(); // consume 'in'
		} else {
			return Ok(None);
		}

		// Expect (
		self.expect(&TokenKind::LParen)?;

		// Check if this is a subquery or inline list
		if self.is_pipeline_start() {
			// IN with subquery: expr in (scan t | ...)
			let pipeline = self.parse_pipeline()?;
			let end_token = self.expect(&TokenKind::RParen)?;
			Ok(Some(ExprAst::InSubquery {
				expr: Box::new(left),
				pipeline: Box::new(pipeline),
				negated,
				span: start.merge(&end_token.span),
			}))
		} else {
			// IN with inline list: expr in (val1, val2, ...)
			let mut values = Vec::new();
			if !self.check(&TokenKind::RParen) {
				loop {
					values.push(self.parse_expr()?);
					if !self.check(&TokenKind::Comma) {
						break;
					}
					self.advance();
				}
			}
			let end_token = self.expect(&TokenKind::RParen)?;
			Ok(Some(ExprAst::InList {
				expr: Box::new(left),
				values,
				negated,
				span: start.merge(&end_token.span),
			}))
		}
	}

	/// Parse a unary expression.
	fn parse_unary(&mut self) -> Result<ExprAst, ParseError> {
		let token = self.current().clone();

		match &token.kind {
			TokenKind::Not => {
				let start = token.span;
				self.advance();

				// Check for "not exists(...)" or "not in"
				if self.check(&TokenKind::Exists) {
					// not exists(pipeline)
					self.advance();
					self.expect(&TokenKind::LParen)?;
					let pipeline = self.parse_pipeline()?;
					let end_token = self.expect(&TokenKind::RParen)?;
					return Ok(ExprAst::Subquery {
						kind: SubqueryKind::NotExists,
						pipeline: Box::new(pipeline),
						span: start.merge(&end_token.span),
					});
				}

				// Regular unary not
				let operand = self.parse_unary()?;
				let span = start.merge(&operand.span());
				Ok(ExprAst::UnaryOp {
					op: UnaryOp::Not,
					operand: Box::new(operand),
					span,
				})
			}
			TokenKind::Minus => {
				self.advance();
				let operand = self.parse_unary()?;
				let span = token.span.merge(&operand.span());
				Ok(ExprAst::UnaryOp {
					op: UnaryOp::Neg,
					operand: Box::new(operand),
					span,
				})
			}
			_ => self.parse_postfix(),
		}
	}

	/// Parse a postfix expression (field access).
	fn parse_postfix(&mut self) -> Result<ExprAst, ParseError> {
		let mut expr = self.parse_primary()?;

		// Handle postfix operators like .field
		while self.check(&TokenKind::Dot) {
			self.advance(); // consume the dot
			let field_token = self.expect_ident()?;
			let span = expr.span().merge(&field_token.span);
			expr = ExprAst::FieldAccess {
				object: Box::new(expr),
				field: field_token.text.clone(),
				span,
			};
		}

		Ok(expr)
	}

	/// Parse a primary expression (literals, identifiers, parenthesized).
	fn parse_primary(&mut self) -> Result<ExprAst, ParseError> {
		let token = self.current().clone();

		match &token.kind {
			TokenKind::Ident => {
				let start = token.span;
				let name = token.text.clone();
				self.advance();

				// Check if this is a function call (identifier followed by parentheses)
				if self.check(&TokenKind::LParen) {
					self.advance(); // consume (
					let mut arguments = Vec::new();

					if !self.check(&TokenKind::RParen) {
						loop {
							arguments.push(self.parse_expr()?);
							if !self.check(&TokenKind::Comma) {
								break;
							}
							self.advance();
						}
					}

					let end_token = self.expect(&TokenKind::RParen)?;
					Ok(ExprAst::Call {
						function_name: name,
						arguments,
						span: start.merge(&end_token.span),
					})
				} else {
					// Regular column reference
					Ok(ExprAst::Column {
						name,
						span: start,
					})
				}
			}

			TokenKind::Int(n) => {
				self.advance();
				Ok(ExprAst::Int {
					value: *n,
					span: token.span,
				})
			}

			TokenKind::Float(f) => {
				self.advance();
				Ok(ExprAst::Float {
					value: *f,
					span: token.span,
				})
			}

			TokenKind::String(s) => {
				self.advance();
				Ok(ExprAst::String {
					value: s.clone(),
					span: token.span,
				})
			}

			TokenKind::Bool(b) => {
				self.advance();
				Ok(ExprAst::Bool {
					value: *b,
					span: token.span,
				})
			}

			TokenKind::Null => {
				self.advance();
				Ok(ExprAst::Null {
					span: token.span,
				})
			}

			TokenKind::LParen => {
				let start = token.span;
				self.advance();

				// Check if this is a scalar subquery: (scan ... | ...)
				if self.is_pipeline_start() {
					let pipeline = self.parse_pipeline()?;
					let end_token = self.expect(&TokenKind::RParen)?;
					Ok(ExprAst::Subquery {
						kind: SubqueryKind::Scalar,
						pipeline: Box::new(pipeline),
						span: start.merge(&end_token.span),
					})
				} else {
					// Regular parenthesized expression
					let inner = self.parse_expr()?;
					let end_token = self.expect(&TokenKind::RParen)?;
					Ok(ExprAst::Paren {
						inner: Box::new(inner),
						span: start.merge(&end_token.span),
					})
				}
			}

			TokenKind::Exists => {
				// exists(pipeline)
				let start = token.span;
				self.advance();
				self.expect(&TokenKind::LParen)?;
				let pipeline = self.parse_pipeline()?;
				let end_token = self.expect(&TokenKind::RParen)?;
				Ok(ExprAst::Subquery {
					kind: SubqueryKind::Exists,
					pipeline: Box::new(pipeline),
					span: start.merge(&end_token.span),
				})
			}

			TokenKind::Dollar => {
				// Variable reference: $name
				let start = token.span;
				self.advance();
				let name_token = self.expect_ident()?;
				Ok(ExprAst::Variable {
					name: name_token.text.clone(),
					span: start.merge(&name_token.span),
				})
			}

			_ => Err(ParseError::UnexpectedToken {
				expected: "expression".to_string(),
				found: token.text.clone(),
				line: token.span.line,
				column: token.span.column,
			}),
		}
	}

	// --- Utility methods ---

	/// Get the current token.
	fn current(&self) -> &Token {
		self.tokens
			.get(self.position)
			.unwrap_or_else(|| self.tokens.last().expect("token list should have at least EOF"))
	}

	/// Get the previous token.
	fn previous(&self) -> &Token {
		self.tokens
			.get(self.position.saturating_sub(1))
			.unwrap_or_else(|| self.tokens.first().expect("token list should not be empty"))
	}

	/// Check if we're at the end of the token stream.
	fn at_end(&self) -> bool {
		self.current().kind == TokenKind::Eof
	}

	/// Peek at the next token and check if it matches the expected kind.
	fn peek_is(&self, kind: &TokenKind) -> bool {
		self.tokens
			.get(self.position + 1)
			.map(|t| std::mem::discriminant(&t.kind) == std::mem::discriminant(kind))
			.unwrap_or(false)
	}

	/// Advance to the next token and return the current one.
	fn advance(&mut self) -> &Token {
		let token = self.current();
		if token.kind != TokenKind::Eof {
			self.position += 1;
		}
		self.tokens.get(self.position - 1).unwrap()
	}

	/// Check if the current token matches the expected kind.
	fn check(&self, kind: &TokenKind) -> bool {
		std::mem::discriminant(&self.current().kind) == std::mem::discriminant(kind)
	}

	/// Expect a specific token kind, or return an error.
	fn expect(&mut self, kind: &TokenKind) -> Result<&Token, ParseError> {
		if self.check(kind) {
			Ok(self.advance())
		} else if self.current().kind == TokenKind::Eof {
			Err(ParseError::UnexpectedEof {
				expected: kind.to_string(),
			})
		} else {
			let token = self.current().clone();
			Err(ParseError::UnexpectedToken {
				expected: kind.to_string(),
				found: token.text.clone(),
				line: token.span.line,
				column: token.span.column,
			})
		}
	}

	/// Expect an identifier token.
	fn expect_ident(&mut self) -> Result<&Token, ParseError> {
		if self.check(&TokenKind::Ident) {
			Ok(self.advance())
		} else if self.current().kind == TokenKind::Eof {
			Err(ParseError::UnexpectedEof {
				expected: "identifier".to_string(),
			})
		} else {
			let token = self.current().clone();
			Err(ParseError::UnexpectedToken {
				expected: "identifier".to_string(),
				found: token.text.clone(),
				line: token.span.line,
				column: token.span.column,
			})
		}
	}
}

/// Convert a token kind to a binary operator.
fn token_to_binary_op(kind: &TokenKind) -> Result<BinaryOp, ParseError> {
	match kind {
		TokenKind::Eq => Ok(BinaryOp::Eq),
		TokenKind::Ne => Ok(BinaryOp::Ne),
		TokenKind::Lt => Ok(BinaryOp::Lt),
		TokenKind::Le => Ok(BinaryOp::Le),
		TokenKind::Gt => Ok(BinaryOp::Gt),
		TokenKind::Ge => Ok(BinaryOp::Ge),
		TokenKind::And => Ok(BinaryOp::And),
		TokenKind::Or => Ok(BinaryOp::Or),
		TokenKind::Plus => Ok(BinaryOp::Add),
		TokenKind::Minus => Ok(BinaryOp::Sub),
		TokenKind::Star => Ok(BinaryOp::Mul),
		TokenKind::Slash => Ok(BinaryOp::Div),
		_ => unreachable!("called with non-binary-op token"),
	}
}

#[cfg(test)]
mod tests {
	use super::{super::lexer::Lexer, *};

	fn parse(source: &str) -> Result<DslAst, ParseError> {
		let tokens = Lexer::new(source).tokenize().expect("lex error");
		Parser::new(tokens).parse()
	}

	/// Helper to get the pipeline from a DslAst
	fn get_pipeline(ast: &DslAst) -> &PipelineAst {
		match &ast.statements[0] {
			StatementAst::Pipeline(p) => p,
			_ => panic!("expected pipeline statement"),
		}
	}

	#[test]
	fn test_parse_simple_pipeline() {
		let ast = parse("scan users | filter age > 21 | take 10").unwrap();
		let pipeline = get_pipeline(&ast);
		assert_eq!(pipeline.stages.len(), 3);
		assert!(matches!(&pipeline.stages[0], StageAst::Scan(_)));
		assert!(matches!(&pipeline.stages[1], StageAst::Filter(_)));
		assert!(matches!(&pipeline.stages[2], StageAst::Take(_)));
	}

	#[test]
	fn test_parse_select_columns() {
		let ast = parse("scan t | select [a, b, c]").unwrap();
		let pipeline = get_pipeline(&ast);
		if let StageAst::Select(s) = &pipeline.stages[1] {
			assert_eq!(s.columns, vec!["a", "b", "c"]);
		} else {
			panic!("expected select stage");
		}
	}

	#[test]
	fn test_parse_complex_filter() {
		let ast = parse("scan t | filter (age > 21 and score >= 80) or vip == true").unwrap();
		let pipeline = get_pipeline(&ast);
		if let StageAst::Filter(f) = &pipeline.stages[1] {
			// Root should be Or
			assert!(matches!(
				&f.predicate,
				ExprAst::BinaryOp {
					op: BinaryOp::Or,
					..
				}
			));
		} else {
			panic!("expected filter stage");
		}
	}

	#[test]
	fn test_parse_extend() {
		let ast = parse("scan t | extend { total: price * qty, doubled: x * 2 }").unwrap();
		let pipeline = get_pipeline(&ast);
		if let StageAst::Extend(e) = &pipeline.stages[1] {
			assert_eq!(e.extensions.len(), 2);
			assert_eq!(e.extensions[0].0, "total");
			assert_eq!(e.extensions[1].0, "doubled");
		} else {
			panic!("expected extend stage");
		}
	}

	#[test]
	fn test_parse_sort() {
		let ast = parse("scan t | sort age desc, name asc").unwrap();
		let pipeline = get_pipeline(&ast);
		if let StageAst::Sort(s) = &pipeline.stages[1] {
			assert_eq!(s.columns.len(), 2);
			assert_eq!(s.columns[0], ("age".to_string(), SortOrder::Desc));
			assert_eq!(s.columns[1], ("name".to_string(), SortOrder::Asc));
		} else {
			panic!("expected sort stage");
		}
	}

	#[test]
	fn test_parse_error_missing_table() {
		let result = parse("scan");
		assert!(result.is_err());
	}

	#[test]
	fn test_parse_error_empty_select() {
		let result = parse("scan t | select []");
		assert!(matches!(result, Err(ParseError::EmptyColumnList { .. })));
	}
}
