// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	Error,
	ast::*,
	token::{Keyword, Token},
};

pub struct Parser {
	tokens: Vec<Token>,
	pos: usize,
}

impl Parser {
	pub fn new(tokens: Vec<Token>) -> Self {
		Self {
			tokens,
			pos: 0,
		}
	}

	pub fn parse(&mut self) -> Result<Statement, Error> {
		let stmt = match self.peek()? {
			Token::Keyword(Keyword::With) => self.parse_select()?,
			Token::Keyword(Keyword::Select) => self.parse_select()?,
			Token::Keyword(Keyword::Insert) => self.parse_insert()?,
			Token::Keyword(Keyword::Update) => self.parse_update()?,
			Token::Keyword(Keyword::Delete) => self.parse_delete()?,
			Token::Keyword(Keyword::Create) => self.parse_create()?,
			other => return Err(Error(format!("unexpected token: {other:?}"))),
		};
		// Skip optional trailing semicolon
		if self.pos < self.tokens.len() && self.tokens[self.pos] == Token::Semicolon {
			self.pos += 1;
		}
		Ok(stmt)
	}

	// ── Helpers ──────────────────────────────────────────────────────────

	fn peek(&self) -> Result<&Token, Error> {
		self.tokens.get(self.pos).ok_or_else(|| Error("unexpected end of input".into()))
	}

	fn advance(&mut self) -> Result<Token, Error> {
		if self.pos < self.tokens.len() {
			let tok = self.tokens[self.pos].clone();
			self.pos += 1;
			Ok(tok)
		} else {
			Err(Error("unexpected end of input".into()))
		}
	}

	fn expect_keyword(&mut self, kw: Keyword) -> Result<(), Error> {
		let tok = self.advance()?;
		if tok == Token::Keyword(kw.clone()) {
			Ok(())
		} else {
			Err(Error(format!("expected {kw:?}, got {tok:?}")))
		}
	}

	fn expect_token(&mut self, expected: Token) -> Result<(), Error> {
		let tok = self.advance()?;
		if tok == expected {
			Ok(())
		} else {
			Err(Error(format!("expected {expected:?}, got {tok:?}")))
		}
	}

	fn at_keyword(&self, kw: &Keyword) -> bool {
		matches!(self.tokens.get(self.pos), Some(Token::Keyword(k)) if k == kw)
	}

	fn at_token(&self, t: &Token) -> bool {
		matches!(self.tokens.get(self.pos), Some(tok) if tok == t)
	}

	fn is_eof(&self) -> bool {
		self.pos >= self.tokens.len()
	}

	fn expect_ident(&mut self) -> Result<String, Error> {
		let tok = self.advance()?;
		match tok {
			Token::Ident(name) => Ok(name),
			// Allow certain keywords to be used as identifiers (common in SQL)
			Token::Keyword(kw) => Ok(keyword_to_string(&kw)),
			_ => Err(Error(format!("expected identifier, got {tok:?}"))),
		}
	}

	// ── SELECT ──────────────────────────────────────────────────────────

	fn parse_select(&mut self) -> Result<Statement, Error> {
		let ctes = if self.at_keyword(&Keyword::With) {
			self.parse_cte_list()?
		} else {
			vec![]
		};

		self.expect_keyword(Keyword::Select)?;

		let distinct = if self.at_keyword(&Keyword::Distinct) {
			self.advance()?;
			true
		} else {
			false
		};

		let columns = self.parse_select_columns()?;

		let from = if self.at_keyword(&Keyword::From) {
			self.advance()?;
			Some(self.parse_from_clause()?)
		} else {
			None
		};

		let mut joins = Vec::new();
		while self.parse_join_if_present()? {
			let join = self.finish_parse_join()?;
			joins.push(join);
		}

		let where_clause = if self.at_keyword(&Keyword::Where) {
			self.advance()?;
			Some(self.parse_expr()?)
		} else {
			None
		};

		let group_by = if self.at_keyword(&Keyword::Group) {
			self.advance()?;
			self.expect_keyword(Keyword::By)?;
			self.parse_expr_list()?
		} else {
			vec![]
		};

		let having = if self.at_keyword(&Keyword::Having) {
			self.advance()?;
			Some(self.parse_expr()?)
		} else {
			None
		};

		let order_by = if self.at_keyword(&Keyword::Order) {
			self.advance()?;
			self.expect_keyword(Keyword::By)?;
			self.parse_order_by_list()?
		} else {
			vec![]
		};

		let limit = if self.at_keyword(&Keyword::Limit) {
			self.advance()?;
			Some(self.parse_u64()?)
		} else {
			None
		};

		let offset = if self.at_keyword(&Keyword::Offset) {
			self.advance()?;
			Some(self.parse_u64()?)
		} else {
			None
		};

		Ok(Statement::Select(SelectStatement {
			ctes,
			distinct,
			columns,
			from,
			joins,
			where_clause,
			group_by,
			having,
			order_by,
			limit,
			offset,
		}))
	}

	fn parse_cte_list(&mut self) -> Result<Vec<CteDefinition>, Error> {
		self.expect_keyword(Keyword::With)?;
		if self.at_keyword(&Keyword::Recursive) {
			return Err(Error("recursive CTEs are not supported".into()));
		}
		let mut ctes = Vec::new();
		loop {
			let name = self.expect_ident()?;
			self.expect_keyword(Keyword::As)?;
			self.expect_token(Token::OpenParen)?;
			let stmt = self.parse_select()?;
			let query = match stmt {
				Statement::Select(sel) => sel,
				_ => return Err(Error("expected SELECT in CTE definition".into())),
			};
			self.expect_token(Token::CloseParen)?;
			ctes.push(CteDefinition {
				name,
				query,
			});
			if self.at_token(&Token::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}
		Ok(ctes)
	}

	fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, Error> {
		let mut cols = Vec::new();
		loop {
			if self.at_token(&Token::Asterisk) {
				self.advance()?;
				cols.push(SelectColumn::AllColumns);
			} else if self.is_eof()
				|| self.at_keyword(&Keyword::From)
				|| self.at_keyword(&Keyword::Where)
				|| self.at_keyword(&Keyword::Order)
				|| self.at_keyword(&Keyword::Limit)
				|| self.at_keyword(&Keyword::Group)
				|| self.at_keyword(&Keyword::Having)
				|| self.at_token(&Token::Semicolon)
			{
				break;
			} else {
				let expr = self.parse_expr()?;
				let alias = if self.at_keyword(&Keyword::As) {
					self.advance()?;
					Some(self.expect_ident()?)
				} else {
					None
				};
				cols.push(SelectColumn::Expr {
					expr,
					alias,
				});
			}

			if self.at_token(&Token::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}
		Ok(cols)
	}

	fn parse_from_clause(&mut self) -> Result<FromClause, Error> {
		if self.at_token(&Token::OpenParen) {
			// Could be a subquery
			self.advance()?;
			if self.at_keyword(&Keyword::Select) {
				let stmt = self.parse_select()?;
				self.expect_token(Token::CloseParen)?;
				if let Statement::Select(sel) = stmt {
					return Ok(FromClause::Subquery(Box::new(sel)));
				}
			}
			return Err(Error("expected subquery after '('".into()));
		}

		let name = self.expect_ident()?;
		if self.at_token(&Token::Dot) {
			self.advance()?;
			let table = self.expect_ident()?;
			Ok(FromClause::Table {
				schema: Some(name),
				name: table,
			})
		} else {
			Ok(FromClause::Table {
				name,
				schema: None,
			})
		}
	}

	// ── JOIN ────────────────────────────────────────────────────────────

	/// Check if the next tokens form a JOIN clause.  Returns true and
	/// consumes the join-type keywords (INNER/LEFT and JOIN) if present.
	fn parse_join_if_present(&mut self) -> Result<bool, Error> {
		if self.is_eof() {
			return Ok(false);
		}
		// Bare JOIN
		if self.at_keyword(&Keyword::Join) {
			return Ok(true);
		}
		// INNER JOIN / LEFT JOIN
		if self.at_keyword(&Keyword::Inner) || self.at_keyword(&Keyword::Left) {
			if let Some(Token::Keyword(Keyword::Join)) = self.tokens.get(self.pos + 1) {
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn finish_parse_join(&mut self) -> Result<JoinClause, Error> {
		let join_type = if self.at_keyword(&Keyword::Left) {
			self.advance()?;
			self.expect_keyword(Keyword::Join)?;
			JoinType::Left
		} else if self.at_keyword(&Keyword::Inner) {
			self.advance()?;
			self.expect_keyword(Keyword::Join)?;
			JoinType::Inner
		} else {
			self.expect_keyword(Keyword::Join)?;
			JoinType::Inner
		};

		let table = self.parse_from_clause()?;
		let table_alias = if self.at_keyword(&Keyword::As) {
			self.advance()?;
			Some(self.expect_ident()?)
		} else if !self.is_eof() && matches!(self.peek()?, Token::Ident(_)) && !self.at_keyword(&Keyword::On) {
			Some(self.expect_ident()?)
		} else {
			None
		};

		self.expect_keyword(Keyword::On)?;
		let on = self.parse_expr()?;

		Ok(JoinClause {
			join_type,
			table,
			table_alias,
			on,
		})
	}

	// ── INSERT ──────────────────────────────────────────────────────────

	fn parse_insert(&mut self) -> Result<Statement, Error> {
		self.expect_keyword(Keyword::Insert)?;
		self.expect_keyword(Keyword::Into)?;

		let (schema, table) = self.parse_table_name()?;

		let columns = if self.at_token(&Token::OpenParen) {
			self.advance()?;
			let cols = self.parse_ident_list()?;
			self.expect_token(Token::CloseParen)?;
			cols
		} else {
			vec![]
		};

		self.expect_keyword(Keyword::Values)?;

		let mut values = Vec::new();
		loop {
			self.expect_token(Token::OpenParen)?;
			let row = self.parse_expr_list()?;
			self.expect_token(Token::CloseParen)?;
			values.push(row);
			if self.at_token(&Token::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		Ok(Statement::Insert(InsertStatement {
			table,
			schema,
			columns,
			values,
		}))
	}

	// ── UPDATE ──────────────────────────────────────────────────────────

	fn parse_update(&mut self) -> Result<Statement, Error> {
		self.expect_keyword(Keyword::Update)?;
		let (schema, table) = self.parse_table_name()?;
		self.expect_keyword(Keyword::Set)?;

		let mut assignments = Vec::new();
		loop {
			let col = self.expect_ident()?;
			self.expect_token(Token::Eq)?;
			let val = self.parse_expr()?;
			assignments.push((col, val));
			if self.at_token(&Token::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}

		let where_clause = if self.at_keyword(&Keyword::Where) {
			self.advance()?;
			Some(self.parse_expr()?)
		} else {
			None
		};

		Ok(Statement::Update(UpdateStatement {
			table,
			schema,
			assignments,
			where_clause,
		}))
	}

	// ── DELETE ──────────────────────────────────────────────────────────

	fn parse_delete(&mut self) -> Result<Statement, Error> {
		self.expect_keyword(Keyword::Delete)?;
		self.expect_keyword(Keyword::From)?;
		let (schema, table) = self.parse_table_name()?;

		let where_clause = if self.at_keyword(&Keyword::Where) {
			self.advance()?;
			Some(self.parse_expr()?)
		} else {
			None
		};

		Ok(Statement::Delete(DeleteStatement {
			table,
			schema,
			where_clause,
		}))
	}

	// ── CREATE TABLE ────────────────────────────────────────────────────

	fn parse_create(&mut self) -> Result<Statement, Error> {
		self.expect_keyword(Keyword::Create)?;
		self.expect_keyword(Keyword::Table)?;
		let (schema, table) = self.parse_table_name()?;

		self.expect_token(Token::OpenParen)?;
		let mut columns = Vec::new();
		let mut primary_key = Vec::new();
		loop {
			if self.at_token(&Token::CloseParen) {
				break;
			}
			// Check for PRIMARY KEY(...) table constraint
			if self.at_keyword(&Keyword::Primary) {
				self.advance()?;
				self.expect_keyword(Keyword::Key)?;
				self.expect_token(Token::OpenParen)?;
				primary_key = self.parse_ident_list()?;
				self.expect_token(Token::CloseParen)?;
				if self.at_token(&Token::Comma) {
					self.advance()?;
				}
				continue;
			}
			let name = self.expect_ident()?;
			let data_type = self.parse_sql_type()?;
			let nullable = if self.at_keyword(&Keyword::Not) {
				self.advance()?;
				self.expect_keyword(Keyword::Null)?;
				false
			} else if self.at_keyword(&Keyword::Null) {
				self.advance()?;
				true
			} else {
				true
			};
			columns.push(ColumnDef {
				name,
				data_type,
				nullable,
			});
			if self.at_token(&Token::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}
		self.expect_token(Token::CloseParen)?;

		Ok(Statement::CreateTable(CreateTableStatement {
			table,
			schema,
			columns,
			primary_key,
		}))
	}

	// ── Type parsing ────────────────────────────────────────────────────

	fn parse_sql_type(&mut self) -> Result<SqlType, Error> {
		let tok = self.advance()?;
		match tok {
			Token::Keyword(Keyword::Int) => Ok(SqlType::Int),
			Token::Keyword(Keyword::Int2) => Ok(SqlType::Int2),
			Token::Keyword(Keyword::Int4) => Ok(SqlType::Int4),
			Token::Keyword(Keyword::Int8) => Ok(SqlType::Int8),
			Token::Keyword(Keyword::Smallint) => Ok(SqlType::Smallint),
			Token::Keyword(Keyword::Integer) => Ok(SqlType::Integer),
			Token::Keyword(Keyword::Bigint) => Ok(SqlType::Bigint),
			Token::Keyword(Keyword::Float4) => Ok(SqlType::Float4),
			Token::Keyword(Keyword::Float8) => Ok(SqlType::Float8),
			Token::Keyword(Keyword::Real) => Ok(SqlType::Real),
			Token::Keyword(Keyword::Double) => {
				// DOUBLE PRECISION
				if self.at_keyword(&Keyword::Precision) {
					self.advance()?;
				}
				Ok(SqlType::Double)
			}
			Token::Keyword(Keyword::Boolean) => Ok(SqlType::Boolean),
			Token::Keyword(Keyword::Bool) => Ok(SqlType::Bool),
			Token::Keyword(Keyword::Text) => Ok(SqlType::Text),
			Token::Keyword(Keyword::Utf8) => Ok(SqlType::Utf8),
			Token::Keyword(Keyword::Blob) => Ok(SqlType::Blob),
			Token::Keyword(Keyword::Varchar) => {
				let len = if self.at_token(&Token::OpenParen) {
					self.advance()?;
					let n = self.parse_u64()?;
					self.expect_token(Token::CloseParen)?;
					Some(n)
				} else {
					None
				};
				Ok(SqlType::Varchar(len))
			}
			Token::Keyword(Keyword::Char) => {
				let len = if self.at_token(&Token::OpenParen) {
					self.advance()?;
					let n = self.parse_u64()?;
					self.expect_token(Token::CloseParen)?;
					Some(n)
				} else {
					None
				};
				Ok(SqlType::Char(len))
			}
			_ => Err(Error(format!("expected SQL type, got {tok:?}"))),
		}
	}

	// ── Expression parsing (Pratt-style precedence) ─────────────────────

	fn parse_expr(&mut self) -> Result<Expr, Error> {
		self.parse_or()
	}

	fn parse_or(&mut self) -> Result<Expr, Error> {
		let mut left = self.parse_and()?;
		while self.at_keyword(&Keyword::Or) {
			self.advance()?;
			let right = self.parse_and()?;
			left = Expr::BinaryOp {
				left: Box::new(left),
				op: BinaryOp::Or,
				right: Box::new(right),
			};
		}
		Ok(left)
	}

	fn parse_and(&mut self) -> Result<Expr, Error> {
		let mut left = self.parse_not()?;
		while self.at_keyword(&Keyword::And) {
			self.advance()?;
			let right = self.parse_not()?;
			left = Expr::BinaryOp {
				left: Box::new(left),
				op: BinaryOp::And,
				right: Box::new(right),
			};
		}
		Ok(left)
	}

	fn parse_not(&mut self) -> Result<Expr, Error> {
		if self.at_keyword(&Keyword::Not) {
			self.advance()?;
			let expr = self.parse_not()?;
			Ok(Expr::UnaryOp {
				op: UnaryOp::Not,
				expr: Box::new(expr),
			})
		} else {
			self.parse_comparison()
		}
	}

	fn parse_comparison(&mut self) -> Result<Expr, Error> {
		let mut left = self.parse_addition()?;

		// IS NULL / IS NOT NULL
		if self.at_keyword(&Keyword::Is) {
			self.advance()?;
			if self.at_keyword(&Keyword::Not) {
				self.advance()?;
				self.expect_keyword(Keyword::Null)?;
				return Ok(Expr::IsNull {
					expr: Box::new(left),
					negated: true,
				});
			} else {
				self.expect_keyword(Keyword::Null)?;
				return Ok(Expr::IsNull {
					expr: Box::new(left),
					negated: false,
				});
			}
		}

		// NOT BETWEEN / BETWEEN ... AND ...
		if self.at_keyword(&Keyword::Not)
			&& matches!(self.tokens.get(self.pos + 1), Some(Token::Keyword(Keyword::Between)))
		{
			self.advance()?; // NOT
			self.advance()?; // BETWEEN
			let low = self.parse_addition()?;
			self.expect_keyword(Keyword::And)?;
			let high = self.parse_addition()?;
			return Ok(Expr::Between {
				expr: Box::new(left),
				low: Box::new(low),
				high: Box::new(high),
				negated: true,
			});
		}

		if self.at_keyword(&Keyword::Between) {
			self.advance()?;
			let low = self.parse_addition()?;
			self.expect_keyword(Keyword::And)?;
			let high = self.parse_addition()?;
			return Ok(Expr::Between {
				expr: Box::new(left),
				low: Box::new(low),
				high: Box::new(high),
				negated: false,
			});
		}

		// NOT IN / IN (...)
		if self.at_keyword(&Keyword::Not)
			&& matches!(self.tokens.get(self.pos + 1), Some(Token::Keyword(Keyword::In)))
		{
			self.advance()?; // NOT
			self.advance()?; // IN
			self.expect_token(Token::OpenParen)?;
			let list = self.parse_expr_list()?;
			self.expect_token(Token::CloseParen)?;
			return Ok(Expr::InList {
				expr: Box::new(left),
				list,
				negated: true,
			});
		}

		if self.at_keyword(&Keyword::In) {
			self.advance()?;
			self.expect_token(Token::OpenParen)?;
			let list = self.parse_expr_list()?;
			self.expect_token(Token::CloseParen)?;
			return Ok(Expr::InList {
				expr: Box::new(left),
				list,
				negated: false,
			});
		}

		// Comparison operators
		let op = match self.tokens.get(self.pos) {
			Some(Token::Eq) => Some(BinaryOp::Eq),
			Some(Token::NotEq) => Some(BinaryOp::NotEq),
			Some(Token::Lt) => Some(BinaryOp::Lt),
			Some(Token::Gt) => Some(BinaryOp::Gt),
			Some(Token::LtEq) => Some(BinaryOp::LtEq),
			Some(Token::GtEq) => Some(BinaryOp::GtEq),
			_ => None,
		};
		if let Some(op) = op {
			self.advance()?;
			let right = self.parse_addition()?;
			left = Expr::BinaryOp {
				left: Box::new(left),
				op,
				right: Box::new(right),
			};
		}

		Ok(left)
	}

	fn parse_addition(&mut self) -> Result<Expr, Error> {
		let mut left = self.parse_multiplication()?;
		loop {
			let op = match self.tokens.get(self.pos) {
				Some(Token::Plus) => Some(BinaryOp::Add),
				Some(Token::Minus) => Some(BinaryOp::Sub),
				_ => None,
			};
			if let Some(op) = op {
				self.advance()?;
				let right = self.parse_multiplication()?;
				left = Expr::BinaryOp {
					left: Box::new(left),
					op,
					right: Box::new(right),
				};
			} else {
				break;
			}
		}
		Ok(left)
	}

	fn parse_multiplication(&mut self) -> Result<Expr, Error> {
		let mut left = self.parse_unary()?;
		loop {
			let op = match self.tokens.get(self.pos) {
				Some(Token::Asterisk) => Some(BinaryOp::Mul),
				Some(Token::Slash) => Some(BinaryOp::Div),
				Some(Token::Percent) => Some(BinaryOp::Mod),
				_ => None,
			};
			if let Some(op) = op {
				self.advance()?;
				let right = self.parse_unary()?;
				left = Expr::BinaryOp {
					left: Box::new(left),
					op,
					right: Box::new(right),
				};
			} else {
				break;
			}
		}
		Ok(left)
	}

	fn parse_unary(&mut self) -> Result<Expr, Error> {
		if self.at_token(&Token::Minus) {
			self.advance()?;
			let expr = self.parse_primary()?;
			return Ok(Expr::UnaryOp {
				op: UnaryOp::Neg,
				expr: Box::new(expr),
			});
		}
		self.parse_primary()
	}

	fn parse_primary(&mut self) -> Result<Expr, Error> {
		let tok = self.peek()?.clone();
		match tok {
			Token::Integer(n) => {
				self.advance()?;
				Ok(Expr::IntegerLiteral(n))
			}
			Token::Float(f) => {
				self.advance()?;
				Ok(Expr::FloatLiteral(f))
			}
			Token::StringLit(s) => {
				self.advance()?;
				Ok(Expr::StringLiteral(s))
			}
			Token::Keyword(Keyword::True) => {
				self.advance()?;
				Ok(Expr::BoolLiteral(true))
			}
			Token::Keyword(Keyword::False) => {
				self.advance()?;
				Ok(Expr::BoolLiteral(false))
			}
			Token::Keyword(Keyword::Null) => {
				self.advance()?;
				Ok(Expr::Null)
			}
			Token::Keyword(Keyword::Cast) => self.parse_cast_expr(),
			// Aggregate function keywords
			Token::Keyword(Keyword::Count)
			| Token::Keyword(Keyword::Sum)
			| Token::Keyword(Keyword::Avg)
			| Token::Keyword(Keyword::Min)
			| Token::Keyword(Keyword::Max) => {
				let name = keyword_to_string(match &self.advance()? {
					Token::Keyword(kw) => kw,
					_ => unreachable!(),
				});
				self.expect_token(Token::OpenParen)?;
				let args = if self.at_token(&Token::Asterisk) {
					self.advance()?;
					vec![Expr::Identifier("*".into())]
				} else {
					self.parse_expr_list()?
				};
				self.expect_token(Token::CloseParen)?;
				Ok(Expr::FunctionCall {
					name,
					args,
				})
			}
			Token::OpenParen => {
				self.advance()?;
				let expr = self.parse_expr()?;
				self.expect_token(Token::CloseParen)?;
				Ok(Expr::Nested(Box::new(expr)))
			}
			Token::Ident(_) => {
				let name = self.expect_ident()?;
				// Check for qualified identifier (table.column)
				if self.at_token(&Token::Dot) {
					self.advance()?;
					let col = self.expect_ident()?;
					// Check for function call on qualified name
					if self.at_token(&Token::OpenParen) {
						self.advance()?;
						let args = if self.at_token(&Token::CloseParen) {
							vec![]
						} else if self.at_token(&Token::Asterisk) {
							self.advance()?;
							vec![Expr::Identifier("*".into())]
						} else {
							self.parse_expr_list()?
						};
						self.expect_token(Token::CloseParen)?;
						Ok(Expr::FunctionCall {
							name: format!("{name}.{col}"),
							args,
						})
					} else {
						Ok(Expr::QualifiedIdentifier(name, col))
					}
				}
				// Check for function call
				else if self.at_token(&Token::OpenParen) {
					self.advance()?;
					let args = if self.at_token(&Token::CloseParen) {
						vec![]
					} else if self.at_token(&Token::Asterisk) {
						self.advance()?;
						vec![Expr::Identifier("*".into())]
					} else {
						self.parse_expr_list()?
					};
					self.expect_token(Token::CloseParen)?;
					Ok(Expr::FunctionCall {
						name,
						args,
					})
				} else {
					Ok(Expr::Identifier(name))
				}
			}
			_ => Err(Error(format!("unexpected token in expression: {tok:?}"))),
		}
	}

	fn parse_cast_expr(&mut self) -> Result<Expr, Error> {
		self.expect_keyword(Keyword::Cast)?;
		self.expect_token(Token::OpenParen)?;
		let expr = self.parse_expr()?;
		self.expect_keyword(Keyword::As)?;
		let data_type = self.parse_sql_type()?;
		self.expect_token(Token::CloseParen)?;
		Ok(Expr::Cast {
			expr: Box::new(expr),
			data_type,
		})
	}

	// ── Utility ─────────────────────────────────────────────────────────

	fn parse_expr_list(&mut self) -> Result<Vec<Expr>, Error> {
		let mut exprs = Vec::new();
		exprs.push(self.parse_expr()?);
		while self.at_token(&Token::Comma) {
			self.advance()?;
			exprs.push(self.parse_expr()?);
		}
		Ok(exprs)
	}

	fn parse_ident_list(&mut self) -> Result<Vec<String>, Error> {
		let mut names = Vec::new();
		names.push(self.expect_ident()?);
		while self.at_token(&Token::Comma) {
			self.advance()?;
			names.push(self.expect_ident()?);
		}
		Ok(names)
	}

	fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>, Error> {
		let mut items = Vec::new();
		loop {
			let expr = self.parse_expr()?;
			let direction = if self.at_keyword(&Keyword::Desc) {
				self.advance()?;
				OrderDirection::Desc
			} else {
				if self.at_keyword(&Keyword::Asc) {
					self.advance()?;
				}
				OrderDirection::Asc
			};
			items.push(OrderByItem {
				expr,
				direction,
			});
			if self.at_token(&Token::Comma) {
				self.advance()?;
			} else {
				break;
			}
		}
		Ok(items)
	}

	fn parse_u64(&mut self) -> Result<u64, Error> {
		let tok = self.advance()?;
		match tok {
			Token::Integer(n) if n >= 0 => Ok(n as u64),
			_ => Err(Error(format!("expected positive integer, got {tok:?}"))),
		}
	}

	fn parse_table_name(&mut self) -> Result<(Option<String>, String), Error> {
		let name = self.expect_ident()?;
		if self.at_token(&Token::Dot) {
			self.advance()?;
			let table = self.expect_ident()?;
			Ok((Some(name), table))
		} else {
			Ok((None, name))
		}
	}
}

fn keyword_to_string(kw: &Keyword) -> String {
	match kw {
		Keyword::Select => "SELECT",
		Keyword::From => "FROM",
		Keyword::Where => "WHERE",
		Keyword::And => "AND",
		Keyword::Or => "OR",
		Keyword::Not => "NOT",
		Keyword::As => "AS",
		Keyword::Order => "ORDER",
		Keyword::By => "BY",
		Keyword::Asc => "ASC",
		Keyword::Desc => "DESC",
		Keyword::Limit => "LIMIT",
		Keyword::Offset => "OFFSET",
		Keyword::Group => "GROUP",
		Keyword::Having => "HAVING",
		Keyword::Distinct => "DISTINCT",
		Keyword::Insert => "INSERT",
		Keyword::Into => "INTO",
		Keyword::Values => "VALUES",
		Keyword::Update => "UPDATE",
		Keyword::Set => "SET",
		Keyword::Delete => "DELETE",
		Keyword::Create => "CREATE",
		Keyword::Table => "TABLE",
		Keyword::Join => "JOIN",
		Keyword::Inner => "INNER",
		Keyword::Left => "LEFT",
		Keyword::Right => "RIGHT",
		Keyword::On => "ON",
		Keyword::Null => "NULL",
		Keyword::True => "true",
		Keyword::False => "false",
		Keyword::Is => "IS",
		Keyword::In => "IN",
		Keyword::Between => "BETWEEN",
		Keyword::Cast => "CAST",
		Keyword::Count => "COUNT",
		Keyword::Sum => "SUM",
		Keyword::Avg => "AVG",
		Keyword::Min => "MIN",
		Keyword::Max => "MAX",
		Keyword::Int => "INT",
		Keyword::Int2 => "INT2",
		Keyword::Int4 => "INT4",
		Keyword::Int8 => "INT8",
		Keyword::Smallint => "SMALLINT",
		Keyword::Integer => "INTEGER",
		Keyword::Bigint => "BIGINT",
		Keyword::Float4 => "FLOAT4",
		Keyword::Float8 => "FLOAT8",
		Keyword::Real => "REAL",
		Keyword::Double => "DOUBLE",
		Keyword::Precision => "PRECISION",
		Keyword::Boolean => "BOOLEAN",
		Keyword::Bool => "BOOL",
		Keyword::Varchar => "VARCHAR",
		Keyword::Text => "TEXT",
		Keyword::Char => "CHAR",
		Keyword::Utf8 => "UTF8",
		Keyword::Blob => "BLOB",
		Keyword::Primary => "PRIMARY",
		Keyword::Key => "KEY",
		Keyword::With => "WITH",
		Keyword::Recursive => "RECURSIVE",
	}
	.into()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::token::tokenize;

	#[test]
	fn test_parse_simple_select() {
		let tokens = tokenize("SELECT id, name FROM users").unwrap();
		let stmt = Parser::new(tokens).parse().unwrap();
		match stmt {
			Statement::Select(sel) => {
				assert_eq!(sel.columns.len(), 2);
				assert!(sel.from.is_some());
			}
			_ => panic!("expected select"),
		}
	}

	#[test]
	fn test_parse_select_star() {
		let tokens = tokenize("SELECT * FROM users").unwrap();
		let stmt = Parser::new(tokens).parse().unwrap();
		match stmt {
			Statement::Select(sel) => {
				assert!(matches!(sel.columns[0], SelectColumn::AllColumns));
			}
			_ => panic!("expected select"),
		}
	}

	#[test]
	fn test_parse_where() {
		let tokens = tokenize("SELECT * FROM users WHERE age > 18").unwrap();
		let stmt = Parser::new(tokens).parse().unwrap();
		match stmt {
			Statement::Select(sel) => {
				assert!(sel.where_clause.is_some());
			}
			_ => panic!("expected select"),
		}
	}

	#[test]
	fn test_parse_insert() {
		let tokens = tokenize("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
		let stmt = Parser::new(tokens).parse().unwrap();
		match stmt {
			Statement::Insert(ins) => {
				assert_eq!(ins.table, "users");
				assert_eq!(ins.columns.len(), 2);
				assert_eq!(ins.values.len(), 1);
			}
			_ => panic!("expected insert"),
		}
	}
}
