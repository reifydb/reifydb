// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::{
		Ast, AstBlock, AstCallFunction, AstDefFunction, AstFor, AstIf, AstLet, AstLiteral, AstLiteralNone,
		AstLoop, AstMatch, AstMatchArm, AstReturn, AstWhile, LetValue as AstLetValue,
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	convert_data_type_with_constraints,
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, ConditionalNode, DeclareNode, ElseIfBranch, ForNode, LetValue, LogicalPlan, LoopNode,
		WhileNode,
		function::{CallFunctionNode, DefineFunctionNode, FunctionParameter, ReturnNode},
	},
	token::token::{Literal, Token, TokenKind},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_let(
		&self,
		ast: AstLet<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let value = match ast.value {
			AstLetValue::Expression(expr) => {
				let inner = BumpBox::into_inner(expr);
				// Detect LET $x = [] → empty Frame
				if matches!(&inner, Ast::List(list) if list.len() == 0) {
					LetValue::EmptyFrame
				} else {
					LetValue::Expression(ExpressionCompiler::compile(inner)?)
				}
			}
			AstLetValue::Statement(statement) => {
				let plan = self.compile(statement, tx)?;
				LetValue::Statement(plan)
			}
		};

		Ok(LogicalPlan::Declare(DeclareNode {
			name: BumpFragment::internal(self.bump, ast.name.text()),
			value,
		}))
	}

	/// Produce a MAP { value: none } plan node.
	fn none_as_map(&self) -> crate::Result<LogicalPlan<'bump>> {
		let none_literal = Ast::Literal(AstLiteral::None(AstLiteralNone(Token {
			kind: TokenKind::Literal(Literal::None),
			fragment: BumpFragment::internal(self.bump, "none"),
		})));
		self.compile_scalar_as_map(none_literal)
	}

	pub(crate) fn compile_if(
		&self,
		ast: AstIf<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		// Compile the condition expression
		let condition = ExpressionCompiler::compile(BumpBox::into_inner(ast.condition))?;

		// Compile the then branch from block
		let then_branch = BumpBox::new_in(self.compile_block_single(ast.then_block, tx)?, self.bump);

		// Compile else if branches
		let mut else_ifs = Vec::new();
		for else_if in ast.else_ifs {
			let condition = ExpressionCompiler::compile(BumpBox::into_inner(else_if.condition))?;
			let then_branch =
				BumpBox::new_in(self.compile_block_single(else_if.then_block, tx)?, self.bump);

			else_ifs.push(ElseIfBranch {
				condition,
				then_branch,
			});
		}

		// Compile optional else branch
		let else_branch = if let Some(else_block) = ast.else_block {
			Some(BumpBox::new_in(self.compile_block_single(else_block, tx)?, self.bump))
		} else {
			Some(BumpBox::new_in(self.none_as_map()?, self.bump))
		};

		Ok(LogicalPlan::Conditional(ConditionalNode {
			condition,
			then_branch,
			else_ifs,
			else_branch,
		}))
	}

	pub(crate) fn compile_match(
		&self,
		ast: AstMatch<'bump>,
		_tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		use reifydb_type::fragment::Fragment;

		use crate::{
			expression::{
				AliasExpression, AndExpression, ColumnExpression, EqExpression, Expression,
				IdentExpression, IsVariantExpression,
			},
			plan::logical::MapNode,
		};

		let fragment = ast.token.fragment.to_owned();

		// Compile subject expression (if present)
		let subject = match ast.subject {
			Some(s) => Some(ExpressionCompiler::compile(BumpBox::into_inner(s))?),
			None => None,
		};

		// Extract the subject column name for field rewriting (if subject is a simple column)
		let subject_col_name = subject.as_ref().and_then(|s| match s {
			Expression::Column(ColumnExpression(col)) => Some(col.name.text().to_string()),
			_ => None,
		});

		// Build list of (condition, LogicalPlan) pairs + optional else
		let mut branches: Vec<(Expression, LogicalPlan<'bump>)> = Vec::new();
		let mut else_plan: Option<LogicalPlan<'bump>> = None;

		for arm in ast.arms {
			match arm {
				AstMatchArm::Else {
					result,
				} => {
					else_plan = Some(self.compile_scalar_as_map(BumpBox::into_inner(result))?);
				}
				AstMatchArm::Value {
					pattern,
					guard,
					result,
				} => {
					let subject_expr = subject.clone().expect("Value arm requires a MATCH subject");
					let pattern_expr = ExpressionCompiler::compile(BumpBox::into_inner(pattern))?;

					let mut condition = Expression::Equal(EqExpression {
						left: Box::new(subject_expr),
						right: Box::new(pattern_expr),
						fragment: fragment.clone(),
					});

					if let Some(guard) = guard {
						let guard_expr =
							ExpressionCompiler::compile(BumpBox::into_inner(guard))?;
						condition = Expression::And(AndExpression {
							left: Box::new(condition),
							right: Box::new(guard_expr),
							fragment: fragment.clone(),
						});
					}

					let result_plan = self.compile_scalar_as_map(BumpBox::into_inner(result))?;
					branches.push((condition, result_plan));
				}
				AstMatchArm::IsVariant {
					namespace,
					sumtype_name,
					variant_name,
					destructure,
					guard,
					result,
				} => {
					let subject_expr = subject.clone().expect("IS arm requires a MATCH subject");

					// Build field bindings for rewriting
					let bindings: Vec<(String, String)> = match (&destructure, &subject_col_name) {
						(Some(destr), Some(col_name)) => {
							let variant_lower = variant_name.text().to_lowercase();
							destr.fields
								.iter()
								.map(|f| {
									let field_name = f.text().to_string();
									let physical = format!(
										"{}_{}_{}",
										col_name,
										variant_lower,
										field_name.to_lowercase()
									);
									(field_name, physical)
								})
								.collect()
						}
						_ => vec![],
					};

					let mut condition = Expression::IsVariant(IsVariantExpression {
						expression: Box::new(subject_expr),
						namespace: namespace.map(|n| n.to_owned()),
						sumtype_name: sumtype_name.to_owned(),
						variant_name: variant_name.to_owned(),
						tag: None,
						fragment: fragment.clone(),
					});

					if let Some(guard) = guard {
						let mut guard_expr =
							ExpressionCompiler::compile(BumpBox::into_inner(guard))?;
						ExpressionCompiler::rewrite_field_refs(&mut guard_expr, &bindings);
						condition = Expression::And(AndExpression {
							left: Box::new(condition),
							right: Box::new(guard_expr),
							fragment: fragment.clone(),
						});
					}

					// Compile result, rewrite field refs, then wrap in MapNode
					let mut result_expr = ExpressionCompiler::compile(BumpBox::into_inner(result))?;
					ExpressionCompiler::rewrite_field_refs(&mut result_expr, &bindings);

					let alias_expr = AliasExpression {
						alias: IdentExpression(Fragment::internal("value")),
						expression: Box::new(result_expr),
						fragment: fragment.clone(),
					};
					let result_plan = LogicalPlan::Map(MapNode {
						map: vec![Expression::Alias(alias_expr)],
					});

					branches.push((condition, result_plan));
				}
				AstMatchArm::Variant {
					variant_name,
					destructure,
					guard,
					result,
				} => {
					let subject_expr =
						subject.clone().expect("Variant arm requires a MATCH subject");

					// Build field bindings for rewriting
					let bindings: Vec<(String, String)> = match (&destructure, &subject_col_name) {
						(Some(destr), Some(col_name)) => {
							let variant_lower = variant_name.text().to_lowercase();
							destr.fields
								.iter()
								.map(|f| {
									let field_name = f.text().to_string();
									let physical = format!(
										"{}_{}_{}",
										col_name,
										variant_lower,
										field_name.to_lowercase()
									);
									(field_name, physical)
								})
								.collect()
						}
						_ => vec![],
					};

					let mut condition = Expression::IsVariant(IsVariantExpression {
						expression: Box::new(subject_expr),
						namespace: None,
						sumtype_name: variant_name.to_owned(),
						variant_name: variant_name.to_owned(),
						tag: None,
						fragment: fragment.clone(),
					});

					if let Some(guard) = guard {
						let mut guard_expr =
							ExpressionCompiler::compile(BumpBox::into_inner(guard))?;
						ExpressionCompiler::rewrite_field_refs(&mut guard_expr, &bindings);
						condition = Expression::And(AndExpression {
							left: Box::new(condition),
							right: Box::new(guard_expr),
							fragment: fragment.clone(),
						});
					}

					// Compile result, rewrite field refs, then wrap in MapNode
					let mut result_expr = ExpressionCompiler::compile(BumpBox::into_inner(result))?;
					ExpressionCompiler::rewrite_field_refs(&mut result_expr, &bindings);

					let alias_expr = AliasExpression {
						alias: IdentExpression(Fragment::internal("value")),
						expression: Box::new(result_expr),
						fragment: fragment.clone(),
					};
					let result_plan = LogicalPlan::Map(MapNode {
						map: vec![Expression::Alias(alias_expr)],
					});

					branches.push((condition, result_plan));
				}
				AstMatchArm::Condition {
					condition,
					guard,
					result,
				} => {
					let mut cond = ExpressionCompiler::compile(BumpBox::into_inner(condition))?;

					if let Some(guard) = guard {
						let guard_expr =
							ExpressionCompiler::compile(BumpBox::into_inner(guard))?;
						cond = Expression::And(AndExpression {
							left: Box::new(cond),
							right: Box::new(guard_expr),
							fragment: fragment.clone(),
						});
					}

					let result_plan = self.compile_scalar_as_map(BumpBox::into_inner(result))?;
					branches.push((cond, result_plan));
				}
			}
		}

		// Assemble into ConditionalNode
		if branches.is_empty() {
			return match else_plan {
				Some(plan) => Ok(plan),
				None => self.none_as_map(),
			};
		}

		let (first_cond, first_then) = branches.remove(0);

		let else_ifs: Vec<ElseIfBranch<'bump>> = branches
			.into_iter()
			.map(|(cond, plan)| ElseIfBranch {
				condition: cond,
				then_branch: BumpBox::new_in(plan, self.bump),
			})
			.collect();

		let else_branch = match else_plan {
			Some(plan) => Some(BumpBox::new_in(plan, self.bump)),
			None => Some(BumpBox::new_in(self.none_as_map()?, self.bump)),
		};

		Ok(LogicalPlan::Conditional(ConditionalNode {
			condition: first_cond,
			then_branch: BumpBox::new_in(first_then, self.bump),
			else_ifs,
			else_branch,
		}))
	}

	/// Compile a block as a single logical plan node.
	/// Takes the first expression from the first statement.
	fn compile_block_single(
		&self,
		block: AstBlock<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		if let Some(first_stmt) = block.statements.into_iter().next() {
			if let Some(first_node) = first_stmt.nodes.into_iter().next() {
				return self.compile_single(first_node, tx);
			}
		}
		// Empty block → none wrapped in MAP
		self.none_as_map()
	}

	/// Compile all statements in a block into a Vec<BumpVec<LogicalPlan>>
	pub(crate) fn compile_block(
		&self,
		block: AstBlock<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<Vec<BumpVec<'bump, LogicalPlan<'bump>>>> {
		let mut result = Vec::new();
		for stmt in block.statements {
			let plans = self.compile(stmt, tx)?;
			result.push(plans);
		}
		Ok(result)
	}

	pub(crate) fn compile_loop(
		&self,
		ast: AstLoop<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let body = self.compile_block(ast.body, tx)?;
		Ok(LogicalPlan::Loop(LoopNode {
			body,
		}))
	}

	pub(crate) fn compile_while(
		&self,
		ast: AstWhile<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let condition = ExpressionCompiler::compile(BumpBox::into_inner(ast.condition))?;
		let body = self.compile_block(ast.body, tx)?;
		Ok(LogicalPlan::While(WhileNode {
			condition,
			body,
		}))
	}

	pub(crate) fn compile_for(
		&self,
		ast: AstFor<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let variable_name = {
			let text = ast.variable.token.fragment.text();
			let clean = if text.starts_with('$') {
				&text[1..]
			} else {
				text
			};
			BumpFragment::internal(self.bump, clean)
		};
		let iterable_ast = BumpBox::into_inner(ast.iterable);
		let iterable_stmt = crate::ast::ast::AstStatement {
			nodes: vec![iterable_ast],
			has_pipes: false,
			is_output: false,
		};
		let iterable = self.compile(iterable_stmt, tx)?;
		let body = self.compile_block(ast.body, tx)?;
		Ok(LogicalPlan::For(ForNode {
			variable_name,
			iterable,
			body,
		}))
	}

	/// Compile a function definition
	pub(crate) fn compile_def_function(
		&self,
		ast: AstDefFunction<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		// Convert function name
		let name = ast.name.token.fragment;

		// Convert parameters
		let mut parameters = Vec::new();
		for param in ast.parameters {
			let param_name = param.variable.token.fragment;
			let type_constraint = if let Some(ref ty) = param.type_annotation {
				Some(convert_data_type_with_constraints(ty)?)
			} else {
				None
			};
			parameters.push(FunctionParameter {
				name: param_name,
				type_constraint,
			});
		}

		// Convert optional return type
		let return_type = if let Some(ref ty) = ast.return_type {
			Some(convert_data_type_with_constraints(ty)?)
		} else {
			None
		};

		// Compile the body
		let body = self.compile_block(ast.body, tx)?;

		Ok(LogicalPlan::DefineFunction(DefineFunctionNode {
			name,
			parameters,
			return_type,
			body,
		}))
	}

	/// Compile a return statement
	pub(crate) fn compile_return(&self, ast: AstReturn<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		let value = if let Some(expr) = ast.value {
			Some(ExpressionCompiler::compile(BumpBox::into_inner(expr))?)
		} else {
			None
		};

		Ok(LogicalPlan::Return(ReturnNode {
			value,
		}))
	}

	/// Compile a function call (potentially user-defined)
	pub(crate) fn compile_call_function(&self, ast: AstCallFunction<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		let name = ast.function.name;

		// Compile arguments as expressions
		let mut arguments = Vec::new();
		for arg in ast.arguments.nodes {
			arguments.push(ExpressionCompiler::compile(arg)?);
		}

		Ok(LogicalPlan::CallFunction(CallFunctionNode {
			name,
			arguments,
		}))
	}
}
