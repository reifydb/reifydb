// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Expression compilation.

use bumpalo::collections::Vec as BumpVec;
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Type;

use super::core::{PlanError, PlanErrorKind, Planner, Result};
use crate::{
	ast::{
		Expr, Statement,
		expr::{BinaryExpr, BinaryOp, Literal, UnaryOp},
	},
	plan::{
		CatalogColumn, Column, ComputedColumn, Function, OutputSchema,
		node::expr::{BinaryPlanOp, PlanExpr, UnaryPlanOp},
	},
	token::Span,
};

impl<'bump, 'cat, T: IntoStandardTransaction> Planner<'bump, 'cat, T> {
	/// Compile an AST expression to a PlanExpr.
	///
	/// If `schema` is provided, column references will be resolved against it.
	/// Otherwise, placeholder columns with `ColumnId(0)` and `Type::Any` are created.
	pub(super) fn compile_expr(
		&self,
		expr: &Expr<'bump>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<&'bump PlanExpr<'bump>> {
		let plan_expr = match expr {
			// Literals
			Expr::Literal(lit) => self.compile_literal(lit),

			// Identifiers - resolve via schema if available
			Expr::Identifier(ident) => {
				if let Some(schema) = schema {
					let col = schema.resolve_unqualified(ident.name, ident.span)?;
					self.schema_column_to_plan_expr(col)
				} else {
					let computed = self.bump.alloc(ComputedColumn {
						name: self.bump.alloc_str(ident.name),
						column_type: Type::Any,
						span: ident.span,
					});
					PlanExpr::Column(Column::Computed(computed))
				}
			}

			Expr::QualifiedIdent(qual) => {
				// For now, treat as column reference with the full name
				let name = qual.name();
				let computed = self.bump.alloc(ComputedColumn {
					name: self.bump.alloc_str(name),
					column_type: Type::Any,
					span: qual.span,
				});
				PlanExpr::Column(Column::Computed(computed))
			}

			Expr::Variable(var) => {
				let resolved = self.resolve_variable(var.name, var.span)?;
				PlanExpr::Variable(resolved)
			}

			Expr::Wildcard(w) => PlanExpr::Wildcard(w.span),
			Expr::Rownum(r) => PlanExpr::Rownum(r.span),
			Expr::Environment(_) => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("$env in expression context".to_string()),
					span: expr.span(),
				});
			}

			// Operators
			Expr::Binary(bin) => {
				// Dot operator - handle field access on variables or column resolution
				if bin.op == BinaryOp::Dot {
					// Check if left side is a variable - this is field access
					if let Expr::Variable(var) = bin.left {
						let field = match bin.right {
							Expr::Identifier(ident) => ident.name,
							_ => {
								return Err(PlanError {
									kind: PlanErrorKind::Unsupported(
										"non-identifier after dot".to_string(),
									),
									span: bin.span,
								});
							}
						};
						let resolved = self.resolve_variable(var.name, var.span)?;
						let base = self.bump.alloc(PlanExpr::Variable(resolved));
						return Ok(self.bump.alloc(PlanExpr::FieldAccess {
							base,
							field,
							span: bin.span,
						}));
					}

					// Otherwise resolve via schema if available
					if let Some(schema) = schema {
						// Extract source.column from dot expression
						let (source, column) = self.extract_dot_parts(bin)?;
						if let Some(col) = schema.resolve_qualified(source, column) {
							return Ok(self
								.bump
								.alloc(self.schema_column_to_plan_expr(col)));
						}
						// Column not found in schema
						return Err(PlanError {
							kind: PlanErrorKind::ColumnNotFound(format!(
								"{}.{}",
								source, column
							)),
							span: bin.span,
						});
					} else {
						// No schema - create placeholder with qualified name
						let qualified_name = self.extract_qualified_name(bin)?;
						let computed = self.bump.alloc(ComputedColumn {
							name: self.bump.alloc_str(&qualified_name),
							column_type: Type::Any,
							span: bin.span,
						});
						return Ok(self
							.bump
							.alloc(PlanExpr::Column(Column::Computed(computed))));
					}
				}

				let left = self.compile_expr(bin.left, schema)?;
				let right = self.compile_expr(bin.right, schema)?;
				let op = self.convert_binary_op(bin.op)?;
				PlanExpr::Binary {
					op,
					left,
					right,
					span: bin.span,
				}
			}

			Expr::Unary(unary) => {
				let operand = self.compile_expr(unary.operand, schema)?;
				let op = self.convert_unary_op(unary.op);
				PlanExpr::Unary {
					op,
					operand,
					span: unary.span,
				}
			}

			// Special expressions
			Expr::Between(between) => {
				let expr = self.compile_expr(between.value, schema)?;
				let low = self.compile_expr(between.lower, schema)?;
				let high = self.compile_expr(between.upper, schema)?;
				PlanExpr::Between {
					expr,
					low,
					high,
					negated: false,
					span: between.span,
				}
			}

			Expr::In(in_expr) => {
				let expr = self.compile_expr(in_expr.value, schema)?;
				let list = self.compile_expr_list_from_expr(in_expr.list, schema)?;
				PlanExpr::In {
					expr,
					list,
					negated: in_expr.negated,
					span: in_expr.span,
				}
			}

			Expr::Cast(cast) => {
				let expr = self.compile_expr(cast.expr, schema)?;
				let target_type = self.parse_type_from_expr(cast.target_type)?;
				PlanExpr::Cast {
					expr,
					target_type,
					span: cast.span,
				}
			}

			Expr::Call(call) => {
				// Check if it's a script function call
				if let Expr::Identifier(ident) = call.function {
					if self.script_functions.iter().any(|&name| name == ident.name) {
						let arguments = self.compile_expr_slice(call.arguments, schema)?;
						return Ok(self.bump.alloc(PlanExpr::CallScriptFunction {
							name: self.bump.alloc_str(ident.name),
							arguments,
							span: call.span,
						}));
					}
				}

				// Otherwise treat as builtin function
				let (name, is_aggregate) = self.resolve_function_name(call.function)?;
				let arguments = self.compile_expr_slice(call.arguments, schema)?;
				let function = self.bump.alloc(Function {
					name,
					is_aggregate,
					span: call.span,
				});
				if is_aggregate {
					PlanExpr::Aggregate {
						function,
						arguments,
						distinct: false,
						span: call.span,
					}
				} else {
					PlanExpr::Call {
						function,
						arguments,
						span: call.span,
					}
				}
			}

			Expr::IfExpr(if_expr) => {
				let condition = self.compile_expr(if_expr.condition, schema)?;
				// Extract the last expression from each branch for expression semantics
				let then_expr =
					self.compile_block_as_expr(if_expr.then_branch, if_expr.span, schema)?;
				let else_expr = if let Some(else_branch) = if_expr.else_branch {
					self.compile_block_as_expr(else_branch, if_expr.span, schema)?
				} else {
					self.bump.alloc(PlanExpr::LiteralUndefined(if_expr.span))
				};
				PlanExpr::Conditional {
					condition,
					then_expr,
					else_expr,
					span: if_expr.span,
				}
			}

			// Collections
			Expr::List(list) => {
				let items = self.compile_expr_slice(list.elements, schema)?;
				PlanExpr::List(items, list.span)
			}

			Expr::Tuple(tuple) => {
				let items = self.compile_expr_slice(tuple.elements, schema)?;
				PlanExpr::Tuple(items, tuple.span)
			}

			Expr::Inline(inline) => {
				let mut fields = BumpVec::with_capacity_in(inline.fields.len(), self.bump);
				for field in inline.fields.iter() {
					let name = self.bump.alloc_str(field.key);
					let value = self.compile_expr(field.value, schema)?;
					fields.push((name as &'bump str, value));
				}
				PlanExpr::Record(fields.into_bump_slice(), inline.span)
			}

			// Paren is just an unwrapping
			Expr::Paren(inner) => return self.compile_expr(inner, schema),

			// Query operations should not appear in expression context
			Expr::From(_)
			| Expr::Filter(_)
			| Expr::Map(_)
			| Expr::Extend(_)
			| Expr::Aggregate(_)
			| Expr::Sort(_)
			| Expr::Distinct(_)
			| Expr::Take(_)
			| Expr::Join(_)
			| Expr::Merge(_)
			| Expr::Window(_)
			| Expr::Apply(_)
			| Expr::SubQuery(_)
			| Expr::LoopExpr(_)
			| Expr::ForExpr(_) => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported(format!(
						"query or control flow operation in expression context"
					)),
					span: expr.span(),
				});
			}
		};

		Ok(self.bump.alloc(plan_expr))
	}

	/// Compile a literal to a PlanExpr.
	pub(super) fn compile_literal(&self, lit: &Literal<'bump>) -> PlanExpr<'bump> {
		match lit {
			Literal::Integer {
				value,
				span,
			} => {
				let v: i64 = value.parse().unwrap_or(0);
				PlanExpr::LiteralInt(v, *span)
			}
			Literal::Float {
				value,
				span,
			} => {
				let v: f64 = value.parse().unwrap_or(0.0);
				PlanExpr::LiteralFloat(v, *span)
			}
			Literal::String {
				value,
				span,
			} => PlanExpr::LiteralString(self.bump.alloc_str(value), *span),
			Literal::Bool {
				value,
				span,
			} => PlanExpr::LiteralBool(*value, *span),
			Literal::Undefined {
				span,
			} => PlanExpr::LiteralUndefined(*span),
			Literal::Temporal {
				value,
				span,
			} => {
				// For now, treat temporal as string
				PlanExpr::LiteralString(self.bump.alloc_str(value), *span)
			}
		}
	}

	/// Convert AST binary operator to plan binary operator.
	fn convert_binary_op(&self, op: BinaryOp) -> Result<BinaryPlanOp> {
		Ok(match op {
			BinaryOp::Add => BinaryPlanOp::Add,
			BinaryOp::Sub => BinaryPlanOp::Sub,
			BinaryOp::Mul => BinaryPlanOp::Mul,
			BinaryOp::Div => BinaryPlanOp::Div,
			BinaryOp::Rem => BinaryPlanOp::Rem,
			BinaryOp::Eq => BinaryPlanOp::Eq,
			BinaryOp::Ne => BinaryPlanOp::Ne,
			BinaryOp::Lt => BinaryPlanOp::Lt,
			BinaryOp::Le => BinaryPlanOp::Le,
			BinaryOp::Gt => BinaryPlanOp::Gt,
			BinaryOp::Ge => BinaryPlanOp::Ge,
			BinaryOp::And => BinaryPlanOp::And,
			BinaryOp::Or => BinaryPlanOp::Or,
			BinaryOp::Xor => BinaryPlanOp::Xor,
			BinaryOp::Dot
			| BinaryOp::DoubleColon
			| BinaryOp::Arrow
			| BinaryOp::As
			| BinaryOp::Assign
			| BinaryOp::KeyValue => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported(format!(
						"operator {} in expression",
						op.as_str()
					)),
					span: Span::default(),
				});
			}
		})
	}

	/// Convert AST unary operator to plan unary operator.
	fn convert_unary_op(&self, op: UnaryOp) -> UnaryPlanOp {
		match op {
			UnaryOp::Neg => UnaryPlanOp::Neg,
			UnaryOp::Not => UnaryPlanOp::Not,
			UnaryOp::Plus => UnaryPlanOp::Plus,
		}
	}

	/// Extract a qualified name from a Dot binary expression (e.g., o.user_id -> "o.user_id")
	fn extract_qualified_name(&self, bin: &BinaryExpr<'bump>) -> Result<String> {
		let left = match bin.left {
			Expr::Identifier(ident) => ident.name.to_string(),
			Expr::Binary(inner) if inner.op == BinaryOp::Dot => self.extract_qualified_name(inner)?,
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported(
						"non-identifier in dot expression".to_string(),
					),
					span: bin.span,
				});
			}
		};
		let right = match bin.right {
			Expr::Identifier(ident) => ident.name,
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("non-identifier after dot".to_string()),
					span: bin.span,
				});
			}
		};
		Ok(format!("{}.{}", left, right))
	}

	/// Extract source and column name from a simple dot expression (e.g., u.name -> ("u", "name"))
	fn extract_dot_parts(&self, bin: &BinaryExpr<'bump>) -> Result<(&'bump str, &'bump str)> {
		let source = match bin.left {
			Expr::Identifier(ident) => ident.name,
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported(
						"non-identifier in dot expression".to_string(),
					),
					span: bin.span,
				});
			}
		};
		let column = match bin.right {
			Expr::Identifier(ident) => ident.name,
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("non-identifier after dot".to_string()),
					span: bin.span,
				});
			}
		};
		Ok((source, column))
	}

	/// Convert a Column to a PlanExpr.
	fn schema_column_to_plan_expr(&self, col: Column<'bump>) -> PlanExpr<'bump> {
		PlanExpr::Column(col)
	}

	/// Compile a slice of expressions.
	pub(super) fn compile_expr_slice(
		&self,
		exprs: &[Expr<'bump>],
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<&'bump [&'bump PlanExpr<'bump>]> {
		let mut compiled = BumpVec::with_capacity_in(exprs.len(), self.bump);
		for expr in exprs {
			compiled.push(self.compile_expr(expr, schema)?);
		}
		Ok(compiled.into_bump_slice())
	}

	/// Compile an expression that should be a list into a slice.
	pub(super) fn compile_expr_list_from_expr(
		&self,
		expr: &Expr<'bump>,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<&'bump [&'bump PlanExpr<'bump>]> {
		match expr {
			Expr::List(list) => self.compile_expr_slice(list.elements, schema),
			Expr::Tuple(tuple) => self.compile_expr_slice(tuple.elements, schema),
			_ => {
				// Single item
				let compiled = self.compile_expr(expr, schema)?;
				Ok(self.bump.alloc_slice_copy(&[compiled]))
			}
		}
	}

	/// Parse type from an expression (for CAST).
	pub(super) fn parse_type_from_expr(&self, expr: &Expr<'bump>) -> Result<Type> {
		match expr {
			Expr::Identifier(ident) => {
				let type_name = ident.name.to_lowercase();
				Ok(match type_name.as_str() {
					"int" | "int8" | "int64" | "integer" => Type::Int8,
					"uint8" | "uint64" => Type::Uint8,
					"float" | "float8" | "float64" | "double" => Type::Float8,
					"bool" | "boolean" => Type::Boolean,
					"string" | "text" | "utf8" => Type::Utf8,
					"bytes" | "binary" => Type::Blob,
					_ => Type::Any,
				})
			}
			_ => Ok(Type::Any),
		}
	}

	/// Resolve function name from expression.
	fn resolve_function_name(&self, expr: &Expr<'bump>) -> Result<(&'bump str, bool)> {
		let name = match expr {
			Expr::Identifier(ident) => self.bump.alloc_str(ident.name),
			Expr::QualifiedIdent(qual) => self.bump.alloc_str(qual.name()),
			// Handle namespace::function syntax (Binary with DoubleColon)
			Expr::Binary(bin) if bin.op == BinaryOp::DoubleColon => {
				let left = self.extract_identifier_name(bin.left)?;
				let right = self.extract_identifier_name(bin.right)?;
				let full_name = format!("{}::{}", left, right);
				self.bump.alloc_str(&full_name)
			}
			_ => {
				return Err(PlanError {
					kind: PlanErrorKind::Unsupported("non-identifier function name".to_string()),
					span: expr.span(),
				});
			}
		};

		// Check if it's an aggregate function
		let lower_name = name.to_lowercase();
		let is_aggregate = matches!(
			lower_name.as_str(),
			"count" | "sum" | "avg" | "min" | "max" | "first" | "last" | "collect"
		);

		Ok((name, is_aggregate))
	}
	/// Compile an expression with alias context for resolving qualified column references.
	pub(super) fn compile_expr_with_aliases(
		&self,
		expr: &Expr<'bump>,
		right_alias: Option<&str>,
		right_columns: Option<&'bump [CatalogColumn<'bump>]>,
	) -> Result<&'bump PlanExpr<'bump>> {
		// Check if this is a qualified column reference (Dot expression)
		if let Expr::Binary(bin) = expr {
			if bin.op == BinaryOp::Dot {
				if let (Some(alias), Some(columns)) = (right_alias, right_columns) {
					// Try to extract alias and column name
					if let (Ok(expr_alias), Ok(col_name)) = (
						self.extract_identifier_name(bin.left),
						self.extract_identifier_name(bin.right),
					) {
						if expr_alias == alias {
							// Look up column in right_columns
							if let Some(col) = columns.iter().find(|c| c.name == col_name) {
								return Ok(self.bump.alloc(PlanExpr::Column(
									Column::Catalog(col),
								)));
							}
							return Err(PlanError {
								kind: PlanErrorKind::ColumnNotFound(format!(
									"{}.{}",
									alias, col_name
								)),
								span: bin.span,
							});
						}
					}
				}
			}
		}
		// Fall back to regular expression compilation
		self.compile_expr(expr, None)
	}

	/// Extract identifier name from an expression.
	pub(super) fn extract_identifier_name(&self, expr: &Expr<'bump>) -> Result<&'bump str> {
		match expr {
			Expr::Identifier(ident) => Ok(ident.name),
			_ => Err(PlanError {
				kind: PlanErrorKind::Unsupported("expected identifier".to_string()),
				span: expr.span(),
			}),
		}
	}

	/// Parse type from a string (for column definitions).
	pub(super) fn parse_type_from_string(&self, type_str: &str) -> Result<Type> {
		let type_name = type_str.to_lowercase();
		Ok(match type_name.as_str() {
			"int1" => Type::Int1,
			"int4" | "int32" => Type::Int4,
			"int2" | "int16" | "smallint" => Type::Int2,
			"int" | "int8" | "int64" | "integer" => Type::Int8,
			"uint1" => Type::Uint1,
			"uint2" => Type::Uint2,
			"uint4" => Type::Uint4,
			"uint8" => Type::Uint8,
			"float4" => Type::Float4,
			"float8" => Type::Float8,
			"bool" => Type::Boolean,
			"text" | "utf8" => Type::Utf8,
			"blob" => Type::Blob,
			_ => Type::Any,
		})
	}

	/// Compile a statement block as an expression.
	///
	/// In expression-oriented semantics, a block's value is the value of its last
	/// expression statement. If the block is empty or ends with a non-expression
	/// statement, returns null.
	fn compile_block_as_expr(
		&self,
		stmts: &[Statement<'bump>],
		default_span: Span,
		schema: Option<&OutputSchema<'bump>>,
	) -> Result<&'bump PlanExpr<'bump>> {
		// Find the last expression statement
		if let Some(last) = stmts.last() {
			if let Statement::Expression(expr_stmt) = last {
				return Ok(self.bump.alloc(self.compile_expr(expr_stmt.expr, schema)?));
			}
		}
		// No expression found, return undefined
		Ok(self.bump.alloc(PlanExpr::LiteralUndefined(default_span)))
	}
}
