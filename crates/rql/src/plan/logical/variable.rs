// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	ast::ast::{
		Ast, AstAssign, AstBlock, AstCall, AstCallFunction, AstDefFunction, AstFor, AstIf, AstLet, AstLiteral,
		AstLiteralNone, AstLoop, AstMatch, AstMatchArm, AstReturn, AstStatement, AstWhile,
		LetValue as AstLetValue,
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	convert_data_type_with_constraints,
	expression::{
		AliasExpression, AndExpression, ColumnExpression, EqExpression, Expression, ExpressionCompiler,
		IdentExpression, IsVariantExpression,
	},
	plan::logical::{
		AssignNode, AssignValue, Compiler, ConditionalNode, DeclareNode, ElseIfBranch, ForNode, LetValue,
		LogicalPlan, LoopNode, MapNode, PipelineNode, WhileNode,
		function::{CallFunctionNode, DefineFunctionNode, FunctionParameter, ReturnNode, ReturnValue},
	},
	token::token::{Literal, Token, TokenKind},
};

type MatchSubject = (Option<Expression>, Option<String>);
type MatchBranches<'bump> = (Vec<(Expression, LogicalPlan<'bump>)>, Option<LogicalPlan<'bump>>);

enum ExprOrPlan<'bump> {
	Expr(Expression),
	Plan(BumpVec<'bump, LogicalPlan<'bump>>),
}

impl<'bump> Compiler<'bump> {
	fn compile_value_expression(&self, inner: Ast<'bump>, tx: &mut Transaction<'_>) -> Result<ExprOrPlan<'bump>> {
		match inner {
			inner @ (Ast::If(_) | Ast::Match(_)) => {
				let plan = self.compile_single(inner, tx)?;
				let mut plans = BumpVec::new_in(self.bump);
				plans.push(plan);
				Ok(ExprOrPlan::Plan(plans))
			}
			other => Ok(ExprOrPlan::Expr(ExpressionCompiler::compile(other)?)),
		}
	}

	pub(crate) fn compile_let(&self, ast: AstLet<'bump>, tx: &mut Transaction<'_>) -> Result<LogicalPlan<'bump>> {
		let value = match ast.value {
			AstLetValue::Expression(expr) => {
				let inner = BumpBox::into_inner(expr);

				if matches!(&inner, Ast::List(list) if list.is_empty()) {
					LetValue::EmptyFrame
				} else if matches!(&inner, Ast::Closure(_)) {
					let Ast::Closure(closure_node) = inner else {
						unreachable!()
					};
					let closure_plan = self.compile_closure(closure_node, tx)?;
					let mut plans = BumpVec::new_in(self.bump);
					plans.push(closure_plan);
					LetValue::Statement(plans)
				} else {
					match self.compile_value_expression(inner, tx)? {
						ExprOrPlan::Expr(expr) => LetValue::Expression(expr),
						ExprOrPlan::Plan(plans) => LetValue::Statement(plans),
					}
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

	pub(crate) fn compile_assign(
		&self,
		ast: AstAssign<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let value = match ast.value {
			AstLetValue::Expression(expr) => {
				match self.compile_value_expression(BumpBox::into_inner(expr), tx)? {
					ExprOrPlan::Expr(expr) => AssignValue::Expression(expr),
					ExprOrPlan::Plan(plans) => AssignValue::Statement(plans),
				}
			}
			AstLetValue::Statement(statement) => {
				let plan = self.compile(statement, tx)?;
				AssignValue::Statement(plan)
			}
		};

		Ok(LogicalPlan::Assign(AssignNode {
			name: BumpFragment::internal(self.bump, ast.variable.name()),
			value,
		}))
	}

	fn none_as_map(&self) -> Result<LogicalPlan<'bump>> {
		let none_literal = Ast::Literal(AstLiteral::None(AstLiteralNone(Token {
			kind: TokenKind::Literal(Literal::None),
			fragment: BumpFragment::internal(self.bump, "none"),
		})));
		self.compile_scalar_as_map(none_literal)
	}

	pub(crate) fn compile_if(&self, ast: AstIf<'bump>, tx: &mut Transaction<'_>) -> Result<LogicalPlan<'bump>> {
		let condition = ExpressionCompiler::compile(BumpBox::into_inner(ast.condition))?;

		let then_branch = BumpBox::new_in(self.compile_block_single(ast.then_block, tx)?, self.bump);

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
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let fragment = ast.token.fragment.to_owned();
		let (subject, subject_col_name) = Self::compile_match_subject(ast.subject)?;
		let (branches, else_plan) =
			self.lower_match_arms(ast.arms, &fragment, &subject, &subject_col_name, tx)?;
		self.assemble_match_conditional(branches, else_plan)
	}

	fn compile_match_arm_result(
		&self,
		result: AstLetValue<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		match result {
			AstLetValue::Expression(expr) => self.compile_scalar_as_map(BumpBox::into_inner(expr)),
			AstLetValue::Statement(statement) => {
				let plans = self.compile(statement, tx)?;
				self.fold_plans_into_single(plans)
			}
		}
	}

	fn compile_match_arm_result_with_bindings(
		&self,
		result: AstLetValue<'bump>,
		bindings: &[(String, String)],
		fragment: &Fragment,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		match result {
			AstLetValue::Expression(expr) => {
				let mut result_expr = ExpressionCompiler::compile(BumpBox::into_inner(expr))?;
				ExpressionCompiler::rewrite_field_refs(&mut result_expr, bindings);

				let alias_expr = AliasExpression {
					alias: IdentExpression(Fragment::internal("value")),
					expression: Box::new(result_expr),
					fragment: fragment.clone(),
				};
				Ok(LogicalPlan::Map(MapNode {
					map: vec![Expression::Alias(alias_expr)],
					rql: String::new(),
				}))
			}
			AstLetValue::Statement(statement) => {
				let plans = self.compile(statement, tx)?;
				self.fold_plans_into_single(plans)
			}
		}
	}

	#[inline]
	fn compile_match_subject(subject: Option<BumpBox<'bump, Ast<'bump>>>) -> Result<MatchSubject> {
		let subject = match subject {
			Some(s) => Some(ExpressionCompiler::compile(BumpBox::into_inner(s))?),
			None => None,
		};

		let subject_col_name = subject.as_ref().and_then(|s| match s {
			Expression::Column(ColumnExpression(col)) => Some(col.name.text().to_string()),
			_ => None,
		});

		Ok((subject, subject_col_name))
	}

	#[inline]
	fn lower_match_arms(
		&self,
		arms: Vec<AstMatchArm<'bump>>,
		fragment: &Fragment,
		subject: &Option<Expression>,
		subject_col_name: &Option<String>,
		tx: &mut Transaction<'_>,
	) -> Result<MatchBranches<'bump>> {
		let mut branches: Vec<(Expression, LogicalPlan<'bump>)> = Vec::new();
		let mut else_plan: Option<LogicalPlan<'bump>> = None;

		for arm in arms {
			match arm {
				AstMatchArm::Else {
					result,
				} => {
					else_plan = Some(self.compile_match_arm_result(result, tx)?);
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

					let result_plan = self.compile_match_arm_result(result, tx)?;
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

					let bindings: Vec<(String, String)> = match (&destructure, subject_col_name) {
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

					let result_plan = self.compile_match_arm_result_with_bindings(
						result, &bindings, fragment, tx,
					)?;

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

					let bindings: Vec<(String, String)> = match (&destructure, subject_col_name) {
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

					let result_plan = self.compile_match_arm_result_with_bindings(
						result, &bindings, fragment, tx,
					)?;

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

					let result_plan = self.compile_match_arm_result(result, tx)?;
					branches.push((cond, result_plan));
				}
			}
		}

		Ok((branches, else_plan))
	}

	#[inline]
	fn assemble_match_conditional(
		&self,
		mut branches: Vec<(Expression, LogicalPlan<'bump>)>,
		else_plan: Option<LogicalPlan<'bump>>,
	) -> Result<LogicalPlan<'bump>> {
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

	fn compile_block_single(&self, block: AstBlock<'bump>, tx: &mut Transaction<'_>) -> Result<LogicalPlan<'bump>> {
		if let Some(first_stmt) = block.statements.into_iter().next() {
			let plans = self.compile(first_stmt, tx)?;
			return self.fold_plans_into_single(plans);
		}

		self.none_as_map()
	}

	fn fold_plans_into_single(&self, mut plans: BumpVec<'bump, LogicalPlan<'bump>>) -> Result<LogicalPlan<'bump>> {
		match plans.len() {
			0 => self.none_as_map(),
			1 => Ok(plans.pop().unwrap()),
			_ => Ok(LogicalPlan::Pipeline(PipelineNode {
				steps: plans,
			})),
		}
	}

	pub(crate) fn compile_block(
		&self,
		block: AstBlock<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<Vec<BumpVec<'bump, LogicalPlan<'bump>>>> {
		let mut result = Vec::new();
		for stmt in block.statements {
			let plans = self.compile(stmt, tx)?;
			result.push(plans);
		}
		Ok(result)
	}

	pub(crate) fn compile_loop(&self, ast: AstLoop<'bump>, tx: &mut Transaction<'_>) -> Result<LogicalPlan<'bump>> {
		let body = self.compile_block(ast.body, tx)?;
		Ok(LogicalPlan::Loop(LoopNode {
			body,
		}))
	}

	pub(crate) fn compile_while(
		&self,
		ast: AstWhile<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let condition = ExpressionCompiler::compile(BumpBox::into_inner(ast.condition))?;
		let body = self.compile_block(ast.body, tx)?;
		Ok(LogicalPlan::While(WhileNode {
			condition,
			body,
		}))
	}

	pub(crate) fn compile_for(&self, ast: AstFor<'bump>, tx: &mut Transaction<'_>) -> Result<LogicalPlan<'bump>> {
		let variable_name = {
			let text = ast.variable.token.fragment.text();
			let clean = text.strip_prefix('$').unwrap_or(text);
			BumpFragment::internal(self.bump, clean)
		};
		let iterable = match ast.iterable {
			AstLetValue::Expression(expr) => {
				let iterable_stmt = AstStatement {
					nodes: vec![BumpBox::into_inner(expr)],
					has_pipes: false,
					is_output: false,
					rql: "",
				};
				self.compile(iterable_stmt, tx)?
			}
			AstLetValue::Statement(statement) => self.compile(statement, tx)?,
		};
		let body = self.compile_block(ast.body, tx)?;
		Ok(LogicalPlan::For(ForNode {
			variable_name,
			iterable,
			body,
		}))
	}

	pub(crate) fn compile_def_function(
		&self,
		ast: AstDefFunction<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let name = ast.name.token.fragment;

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

		let return_type = if let Some(ref ty) = ast.return_type {
			Some(convert_data_type_with_constraints(ty)?)
		} else {
			None
		};

		let body = self.compile_block(ast.body, tx)?;

		Ok(LogicalPlan::DefineFunction(DefineFunctionNode {
			name,
			parameters,
			return_type,
			body,
		}))
	}

	pub(crate) fn compile_return(
		&self,
		ast: AstReturn<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let value = match ast.value {
			Some(AstLetValue::Expression(expr)) => {
				Some(match self.compile_value_expression(BumpBox::into_inner(expr), tx)? {
					ExprOrPlan::Expr(expr) => ReturnValue::Expression(expr),
					ExprOrPlan::Plan(plans) => ReturnValue::Statement(plans),
				})
			}
			Some(AstLetValue::Statement(statement)) => {
				let plan = self.compile(statement, tx)?;
				Some(ReturnValue::Statement(plan))
			}
			None => None,
		};

		Ok(LogicalPlan::Return(ReturnNode {
			value,
		}))
	}

	pub(crate) fn compile_call_function(&self, ast: AstCallFunction<'bump>) -> Result<LogicalPlan<'bump>> {
		let name = if ast.function.namespaces.is_empty() {
			ast.function.name
		} else {
			let first_ns = &ast.function.namespaces[0];
			let mut qualified = String::new();
			for ns in &ast.function.namespaces {
				qualified.push_str(ns.text());
				qualified.push_str("::");
			}
			qualified.push_str(ast.function.name.text());
			let qualified_text = self.bump.alloc_str(&qualified);
			match first_ns {
				BumpFragment::Statement {
					offset,
					line,
					column,
					..
				} => BumpFragment::Statement {
					text: qualified_text,
					offset: *offset,
					source_end: ast.function.name.source_end(),
					line: *line,
					column: *column,
				},
				_ => BumpFragment::internal(self.bump, &qualified),
			}
		};

		let mut arguments = Vec::new();
		for arg in ast.arguments.nodes {
			arguments.push(ExpressionCompiler::compile(arg)?);
		}

		Ok(LogicalPlan::CallFunction(CallFunctionNode {
			name,
			arguments,
			is_procedure_call: false,
		}))
	}

	pub(crate) fn compile_call(&self, ast: AstCall<'bump>) -> Result<LogicalPlan<'bump>> {
		let name = if ast.function.namespaces.is_empty() {
			ast.function.name
		} else {
			let first_ns = &ast.function.namespaces[0];
			let mut qualified = String::new();
			for ns in &ast.function.namespaces {
				qualified.push_str(ns.text());
				qualified.push_str("::");
			}
			qualified.push_str(ast.function.name.text());
			let qualified_text = self.bump.alloc_str(&qualified);
			match first_ns {
				BumpFragment::Statement {
					offset,
					line,
					column,
					..
				} => BumpFragment::Statement {
					text: qualified_text,
					offset: *offset,
					source_end: ast.function.name.source_end(),
					line: *line,
					column: *column,
				},
				_ => BumpFragment::internal(self.bump, &qualified),
			}
		};

		let mut arguments = Vec::new();
		for arg in ast.arguments.nodes {
			arguments.push(ExpressionCompiler::compile(arg)?);
		}

		Ok(LogicalPlan::CallFunction(CallFunctionNode {
			name,
			arguments,
			is_procedure_call: true,
		}))
	}
}
