// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Control flow operations compilation.

use bumpalo::collections::Vec as BumpVec;
use reifydb_transaction::IntoStandardTransaction;

use super::core::{Planner, Result};
use crate::{
	ast::{
		Expr,
		stmt::{AssignStmt, DefStmt, ForIterable, ForStmt, IfStmt, LetStmt, LetValue, LoopStmt, ReturnStmt},
	},
	plan::{
		Plan, Variable,
		node::control::{
			AssignNode, ConditionalNode, DeclareNode, DeclareValue, DefineScriptFunctionNode, ElseIfBranch,
			ForIterableValue, ForNode, LoopNode, ReturnNode,
		},
	},
};

impl<'bump, 'cat, T: IntoStandardTransaction> Planner<'bump, 'cat, T> {
	pub(super) async fn compile_let(&mut self, let_stmt: &LetStmt<'bump>) -> Result<Plan<'bump>> {
		let var_id = self.declare_variable(let_stmt.name);
		let variable = self.bump.alloc(Variable {
			name: self.bump.alloc_str(let_stmt.name),
			variable_id: var_id,
			span: let_stmt.span,
		});

		let value = match &let_stmt.value {
			LetValue::Expr(expr) => {
				// Check if expression is a SubQuery - treat as pipeline
				if let Expr::SubQuery(subquery) = expr {
					let plan = self.compile_subquery(subquery, None).await?;
					// Store the schema for this variable
					let schema = self.build_schema_from_plan(&plan);
					self.store_variable_schema(var_id, schema);
					let plan_ref = self.bump.alloc(plan) as &'bump Plan<'bump>;
					DeclareValue::Plan(std::slice::from_ref(self.bump.alloc(plan_ref)))
				} else {
					let compiled = self.compile_expr(expr, None)?;
					DeclareValue::Expression(compiled)
				}
			}
			LetValue::Pipeline(stages) => {
				let plans = self.compile_statement_body_as_pipeline(stages).await?;
				// Store the schema from the last plan for this variable
				if let Some(last_plan) = plans.last() {
					let schema = self.build_schema_from_plan(last_plan);
					self.store_variable_schema(var_id, schema);
				}
				DeclareValue::Plan(plans)
			}
		};

		Ok(Plan::Declare(DeclareNode {
			variable,
			value,
			span: let_stmt.span,
		}))
	}
	pub(super) async fn compile_assign(&mut self, assign_stmt: &AssignStmt<'bump>) -> Result<Plan<'bump>> {
		let variable = self.resolve_variable(assign_stmt.name, assign_stmt.span)?;
		let value = self.compile_expr(assign_stmt.value, None)?;

		Ok(Plan::Assign(AssignNode {
			variable,
			value: DeclareValue::Expression(value),
			span: assign_stmt.span,
		}))
	}
	pub(super) async fn compile_if(&mut self, if_stmt: &IfStmt<'bump>) -> Result<Plan<'bump>> {
		let condition = self.compile_expr(if_stmt.condition, None)?;

		// Compile then branch
		self.push_scope();
		let then_branch = self.compile_statement_body(if_stmt.then_branch).await?;
		self.pop_scope();

		// Compile else-if branches
		let mut else_ifs = BumpVec::with_capacity_in(if_stmt.else_ifs.len(), self.bump);
		for else_if in if_stmt.else_ifs {
			let cond = self.compile_expr(else_if.condition, None)?;
			self.push_scope();
			let body = self.compile_statement_body(else_if.body).await?;
			self.pop_scope();
			else_ifs.push(ElseIfBranch {
				condition: cond,
				body,
			});
		}

		// Compile else branch
		let else_branch = if let Some(else_body) = if_stmt.else_branch {
			self.push_scope();
			let body = self.compile_statement_body(else_body).await?;
			self.pop_scope();
			Some(body)
		} else {
			None
		};

		Ok(Plan::Conditional(ConditionalNode {
			condition,
			then_branch,
			else_ifs: else_ifs.into_bump_slice(),
			else_branch,
			span: if_stmt.span,
		}))
	}
	pub(super) async fn compile_loop(&mut self, loop_stmt: &LoopStmt<'bump>) -> Result<Plan<'bump>> {
		self.push_scope();
		let body = self.compile_statement_body(loop_stmt.body).await?;
		self.pop_scope();

		Ok(Plan::Loop(LoopNode {
			body,
			span: loop_stmt.span,
		}))
	}
	pub(super) async fn compile_for(&mut self, for_stmt: &ForStmt<'bump>) -> Result<Plan<'bump>> {
		self.push_scope();

		// Declare the loop variable
		let var_id = self.declare_variable(for_stmt.variable);
		let variable = self.bump.alloc(Variable {
			name: self.bump.alloc_str(for_stmt.variable),
			variable_id: var_id,
			span: for_stmt.span,
		});

		// Compile the iterable (expression or pipeline)
		let iterable = match &for_stmt.iterable {
			ForIterable::Expr(expr) => {
				// Check if expression is a SubQuery - treat as pipeline
				if let Expr::SubQuery(subquery) = expr {
					let plan = self.compile_subquery(subquery, None).await?;
					// Store the schema for the loop variable
					let schema = self.build_schema_from_plan(&plan);
					self.store_variable_schema(var_id, schema);
					let plan_ref = self.bump.alloc(plan) as &'bump Plan<'bump>;
					ForIterableValue::Plan(std::slice::from_ref(self.bump.alloc(plan_ref)))
				} else if let Expr::From(_) = expr {
					// Single FROM expression - compile as pipeline stage
					let plan = self.compile_pipeline_stage(expr, None).await?;
					let schema = self.build_schema_from_plan(&plan);
					self.store_variable_schema(var_id, schema);
					let plan_ref = self.bump.alloc(plan) as &'bump Plan<'bump>;
					ForIterableValue::Plan(std::slice::from_ref(self.bump.alloc(plan_ref)))
				} else {
					let compiled = self.compile_expr(expr, None)?;
					ForIterableValue::Expression(compiled)
				}
			}
			ForIterable::Pipeline(stages) => {
				let plans = self.compile_statement_body_as_pipeline(stages).await?;
				// Store the schema from the last plan for the loop variable
				if let Some(last_plan) = plans.last() {
					let schema = self.build_schema_from_plan(last_plan);
					self.store_variable_schema(var_id, schema);
				}
				ForIterableValue::Plan(plans)
			}
		};

		// Compile the body
		let body = self.compile_statement_body(for_stmt.body).await?;

		self.pop_scope();

		Ok(Plan::For(ForNode {
			variable,
			iterable,
			body,
			span: for_stmt.span,
		}))
	}
	pub(super) async fn compile_return(&mut self, return_stmt: &ReturnStmt<'bump>) -> Result<Plan<'bump>> {
		let value = if let Some(expr) = return_stmt.value {
			Some(self.compile_expr(expr, None)?)
		} else {
			None
		};

		Ok(Plan::Return(ReturnNode {
			value,
			span: return_stmt.span,
		}))
	}

	pub(super) async fn compile_def(&mut self, def_stmt: &DefStmt<'bump>) -> Result<Plan<'bump>> {
		// Register the script function name
		let name = self.bump.alloc_str(def_stmt.name);
		self.script_functions.push(name);

		// Compile the function body
		self.push_scope();
		let body = self.compile_statement_body(def_stmt.body).await?;
		self.pop_scope();

		Ok(Plan::DefineScriptFunction(DefineScriptFunctionNode {
			name,
			body,
			span: def_stmt.span,
		}))
	}
}
