// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashSet, fmt::Debug, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::util::lru::LruCache;
use reifydb_runtime::hash::{Hash128, xxh3_128};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment, value::Value};

use crate::{
	ast::parse_str,
	bump::Bump,
	error::RqlError,
	expression::{Expression, ParameterExpression, PrefixOperator},
	instruction::{Addr, CompiledClosureDef, CompiledFunctionDef, Instruction, ScopeType},
	nodes,
	plan::{
		physical::{self, PhysicalPlan},
		plan,
	},
	query::QueryPlan,
};

const DEFAULT_CAPACITY: usize = 1024 * 8;

#[derive(Debug, Clone)]
pub struct Compiled {
	pub instructions: Vec<Instruction>,
	pub is_output: bool,
}

/// Result of compiling a query.
pub enum CompilationResult {
	Ready(Arc<Vec<Compiled>>),
	Incremental(IncrementalCompilation),
}

/// Opaque state for incremental compilation.
pub struct IncrementalCompilation {
	query: String,
	total_statements: usize,
	current: usize,
}

#[derive(Debug, Clone)]
pub struct Compiler(Arc<CompilerInner>);

struct CompilerInner {
	catalog: Catalog,
	cache: LruCache<Hash128, Arc<Vec<Compiled>>>,
}

impl Debug for CompilerInner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompilerInner")
			.field("catalog", &self.catalog)
			.field("cache_len", &self.cache.len())
			.field("cache_capacity", &self.cache.capacity())
			.finish()
	}
}

impl Compiler {
	pub fn new(catalog: Catalog) -> Self {
		Self(Arc::new(CompilerInner {
			catalog,
			cache: LruCache::new(DEFAULT_CAPACITY),
		}))
	}

	pub fn compile(&self, tx: &mut Transaction<'_>, query: &str) -> Result<CompilationResult> {
		let hash = xxh3_128(query.as_bytes());

		if let Some(cached) = self.0.cache.get(&hash) {
			return Ok(CompilationResult::Ready(cached));
		}

		let bump = Bump::new();
		let statements = parse_str(&bump, query)?;
		let has_ddl = statements.iter().any(|s| s.contains_ddl());
		let total_statements = statements.len();
		let needs_incremental = total_statements > 1 && has_ddl;

		if needs_incremental {
			return Ok(CompilationResult::Incremental(IncrementalCompilation {
				query: query.to_string(),
				total_statements,
				current: 0,
			}));
		}

		// Batch compile
		let mut plans = Vec::new();
		for statement in statements {
			let is_output = statement.is_output;
			if let Some(physical) = plan(&bump, &self.0.catalog, tx, statement)? {
				plans.push(Compiled {
					instructions: compile_instructions(physical)?,
					is_output,
				});
			}
		}

		let arc_plans = Arc::new(plans);
		if !has_ddl {
			self.0.cache.put(hash, arc_plans.clone());
		}
		Ok(CompilationResult::Ready(arc_plans))
	}

	/// Compile the next statement in an incremental compilation.
	/// Returns `None` when all statements have been compiled.
	pub fn compile_next(
		&self,
		tx: &mut Transaction<'_>,
		state: &mut IncrementalCompilation,
	) -> Result<Option<Compiled>> {
		if state.current >= state.total_statements {
			return Ok(None);
		}

		let bump = Bump::new();
		let statements = parse_str(&bump, &state.query)?;
		let idx = state.current;
		state.current += 1;

		let statement = statements.into_iter().nth(idx).unwrap();
		let is_output = statement.is_output;
		if let Some(physical) = plan(&bump, &self.0.catalog, tx, statement)? {
			Ok(Some(Compiled {
				instructions: compile_instructions(physical)?,
				is_output,
			}))
		} else {
			self.compile_next(tx, state)
		}
	}

	/// Clear all cached plans.
	pub fn clear(&self) {
		self.0.cache.clear();
	}

	/// Return the number of cached plans.
	pub fn len(&self) -> usize {
		self.0.cache.len()
	}

	/// Return true if the cache is empty.
	pub fn is_empty(&self) -> bool {
		self.0.cache.is_empty()
	}

	/// Return the cache capacity.
	pub fn capacity(&self) -> usize {
		self.0.cache.capacity()
	}
}

/// Recursively convert a bump-allocated query PhysicalPlan into an owned QueryPlan.
/// Panics if the plan contains non-query nodes (DDL, DML, control flow, etc.).
fn materialize_query_plan(plan: PhysicalPlan<'_>) -> QueryPlan {
	match plan {
		// Leaf nodes — reuse directly (already owned types from nodes.rs)
		PhysicalPlan::TableScan(node) => QueryPlan::TableScan(node),
		PhysicalPlan::TableVirtualScan(node) => QueryPlan::TableVirtualScan(node),
		PhysicalPlan::ViewScan(node) => QueryPlan::ViewScan(node),
		PhysicalPlan::RingBufferScan(node) => QueryPlan::RingBufferScan(node),
		PhysicalPlan::FlowScan(node) => QueryPlan::FlowScan(node),
		PhysicalPlan::DictionaryScan(node) => QueryPlan::DictionaryScan(node),
		PhysicalPlan::SeriesScan(node) => QueryPlan::SeriesScan(node),
		PhysicalPlan::IndexScan(node) => QueryPlan::IndexScan(node),
		PhysicalPlan::RowPointLookup(node) => QueryPlan::RowPointLookup(node),
		PhysicalPlan::RowListLookup(node) => QueryPlan::RowListLookup(node),
		PhysicalPlan::RowRangeScan(node) => QueryPlan::RowRangeScan(node),
		PhysicalPlan::InlineData(node) => QueryPlan::InlineData(node),
		PhysicalPlan::Generator(node) => QueryPlan::Generator(node),
		PhysicalPlan::Variable(node) => QueryPlan::Variable(node),
		PhysicalPlan::Environment(node) => QueryPlan::Environment(node),

		// Nodes with recursive children — materialize BumpBox to Box
		PhysicalPlan::Aggregate(node) => QueryPlan::Aggregate(nodes::AggregateNode {
			input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.input))),
			by: node.by,
			map: node.map,
		}),
		PhysicalPlan::Distinct(node) => QueryPlan::Distinct(nodes::DistinctNode {
			input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.input))),
			columns: node.columns,
		}),
		PhysicalPlan::Assert(node) => QueryPlan::Assert(nodes::AssertNode {
			input: node
				.input
				.map(|i| Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))),
			conditions: node.conditions,
			message: node.message,
		}),
		PhysicalPlan::Filter(node) => QueryPlan::Filter(nodes::FilterNode {
			input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.input))),
			conditions: node.conditions,
		}),
		PhysicalPlan::JoinInner(node) => QueryPlan::JoinInner(nodes::JoinInnerNode {
			left: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.left))),
			right: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.right))),
			on: node.on,
			alias: node.alias,
		}),
		PhysicalPlan::JoinLeft(node) => QueryPlan::JoinLeft(nodes::JoinLeftNode {
			left: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.left))),
			right: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.right))),
			on: node.on,
			alias: node.alias,
		}),
		PhysicalPlan::JoinNatural(node) => QueryPlan::JoinNatural(nodes::JoinNaturalNode {
			left: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.left))),
			right: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.right))),
			join_type: node.join_type,
			alias: node.alias,
		}),
		PhysicalPlan::Take(node) => QueryPlan::Take(nodes::TakeNode {
			input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.input))),
			take: node.take,
		}),
		PhysicalPlan::Sort(node) => QueryPlan::Sort(nodes::SortNode {
			input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.input))),
			by: node.by,
		}),
		PhysicalPlan::Map(node) => QueryPlan::Map(nodes::MapNode {
			input: node
				.input
				.map(|i| Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))),
			map: node.map,
		}),
		PhysicalPlan::Extend(node) => QueryPlan::Extend(nodes::ExtendNode {
			input: node
				.input
				.map(|i| Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))),
			extend: node.extend,
		}),
		PhysicalPlan::Patch(node) => QueryPlan::Patch(nodes::PatchNode {
			input: node
				.input
				.map(|i| Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))),
			assignments: node.assignments,
		}),
		PhysicalPlan::Apply(node) => QueryPlan::Apply(nodes::ApplyNode {
			input: node
				.input
				.map(|i| Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))),
			operator: node.operator,
			expressions: node.expressions,
		}),
		PhysicalPlan::Window(node) => QueryPlan::Window(nodes::WindowNode {
			input: node
				.input
				.map(|i| Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))),
			window_type: node.window_type,
			size: node.size,
			slide: node.slide,
			group_by: node.group_by,
			aggregations: node.aggregations,
			min_events: node.min_events,
			max_window_count: node.max_window_count,
			max_window_age: node.max_window_age,
		}),
		PhysicalPlan::Scalarize(node) => QueryPlan::Scalarize(nodes::ScalarizeNode {
			input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(node.input))),
			fragment: node.fragment,
		}),

		PhysicalPlan::Append(physical::AppendPhysicalNode::Query {
			left,
			right,
		}) => QueryPlan::Append(nodes::AppendQueryNode {
			left: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(left))),
			right: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(right))),
		}),

		// Non-query nodes cannot be materialized to QueryPlan
		other => panic!(
			"cannot materialize non-query PhysicalPlan to QueryPlan: {:?}",
			std::mem::discriminant(&other)
		),
	}
}

fn compile_instructions(plan: PhysicalPlan<'_>) -> Result<Vec<Instruction>> {
	let mut compiler = InstructionCompiler::new();
	compiler.compile_plan(plan)?;
	compiler.emit(Instruction::Halt);
	Ok(compiler.instructions)
}

// ============================================================================
// Instruction Compilation
// ============================================================================

/// Context for tracking loop information during compilation
struct LoopContext {
	/// Address of the condition check / ForNext (target for Continue)
	continue_addr: Addr,
	/// Placeholder indices in `instructions` where the loop-end address must be backpatched
	break_patches: Vec<usize>,
	/// Scope depth at the point the loop was entered (used to compute exit_scopes)
	scope_depth: usize,
}

/// Scan compiled closure body instructions for variable references not in the parameter list.
/// Returns the list of free variable names (as Fragments) that need to be captured.
fn scan_free_variables(body: &[Instruction], params: &[nodes::FunctionParameter]) -> Vec<Fragment> {
	let param_names: HashSet<&str> = params
		.iter()
		.map(|p| {
			let name = p.name.text();
			if name.starts_with('$') {
				&name[1..]
			} else {
				name
			}
		})
		.collect();

	// First pass: collect locally-declared variables.
	// These should NOT be propagated upward even if a nested closure captures them,
	// because they'll be available at runtime in the outer closure's scope.
	let mut local_vars = HashSet::new();
	for instr in body {
		if let Instruction::DeclareVar(name) = instr {
			let stripped = if name.text().starts_with('$') {
				&name.text()[1..]
			} else {
				name.text()
			};
			local_vars.insert(stripped.to_string());
		}
	}

	let mut free_vars = Vec::new();
	let mut seen = HashSet::new();

	for instr in body {
		match instr {
			Instruction::LoadVar(name) => {
				let stripped = if name.text().starts_with('$') {
					&name.text()[1..]
				} else {
					name.text()
				};
				if !param_names.contains(stripped)
					&& !local_vars.contains(stripped) && seen.insert(stripped.to_string())
				{
					free_vars.push(name.clone());
				}
			}
			Instruction::FieldAccess {
				object,
				..
			} => {
				let stripped = if object.text().starts_with('$') {
					&object.text()[1..]
				} else {
					object.text()
				};
				if !param_names.contains(stripped)
					&& !local_vars.contains(stripped) && seen.insert(stripped.to_string())
				{
					free_vars.push(object.clone());
				}
			}
			Instruction::DefineClosure(closure_def) => {
				// Propagate nested closure captures upward.
				// Inner closures are compiled bottom-up, so their captures are already populated.
				for cap in &closure_def.captures {
					let stripped = if cap.text().starts_with('$') {
						&cap.text()[1..]
					} else {
						cap.text()
					};
					if !param_names.contains(stripped)
						&& !local_vars.contains(stripped) && seen.insert(stripped.to_string())
					{
						free_vars.push(cap.clone());
					}
				}
			}
			_ => {}
		}
	}
	free_vars
}

/// Instruction compiler that transforms PhysicalPlan to Instructions
struct InstructionCompiler {
	instructions: Vec<Instruction>,
	loop_stack: Vec<LoopContext>,
	scope_depth: usize,
}

impl InstructionCompiler {
	fn new() -> Self {
		Self {
			instructions: Vec::new(),
			loop_stack: Vec::new(),
			scope_depth: 0,
		}
	}

	fn emit(&mut self, instr: Instruction) -> usize {
		let addr = self.instructions.len();
		self.instructions.push(instr);
		addr
	}

	fn current_addr(&self) -> Addr {
		self.instructions.len()
	}

	/// Compile an expression to bytecode and emit a JumpIfFalsePop, returning its index for backpatching.
	fn emit_conditional_jump(&mut self, condition: Expression) -> usize {
		self.compile_expression(&condition);
		self.emit(Instruction::JumpIfFalsePop(0))
	}

	// ========================================================================
	// Expression compilation
	// ========================================================================
	fn compile_expression(&mut self, expr: &Expression) {
		match expr {
			Expression::Constant(c) => {
				let value = c.to_value();
				if matches!(value, Value::None { .. }) {
					self.emit(Instruction::PushNone);
				} else {
					self.emit(Instruction::PushConst(value));
				}
			}
			Expression::Variable(v) => {
				self.emit(Instruction::LoadVar(v.fragment.clone()));
			}
			Expression::Add(a) => {
				self.compile_expression(&a.left);
				self.compile_expression(&a.right);
				self.emit(Instruction::Add);
			}
			Expression::Sub(s) => {
				self.compile_expression(&s.left);
				self.compile_expression(&s.right);
				self.emit(Instruction::Sub);
			}
			Expression::Mul(m) => {
				self.compile_expression(&m.left);
				self.compile_expression(&m.right);
				self.emit(Instruction::Mul);
			}
			Expression::Div(d) => {
				self.compile_expression(&d.left);
				self.compile_expression(&d.right);
				self.emit(Instruction::Div);
			}
			Expression::Rem(r) => {
				self.compile_expression(&r.left);
				self.compile_expression(&r.right);
				self.emit(Instruction::Rem);
			}
			Expression::Equal(e) => {
				self.compile_expression(&e.left);
				self.compile_expression(&e.right);
				self.emit(Instruction::CmpEq);
			}
			Expression::NotEqual(e) => {
				self.compile_expression(&e.left);
				self.compile_expression(&e.right);
				self.emit(Instruction::CmpNe);
			}
			Expression::GreaterThan(e) => {
				self.compile_expression(&e.left);
				self.compile_expression(&e.right);
				self.emit(Instruction::CmpGt);
			}
			Expression::GreaterThanEqual(e) => {
				self.compile_expression(&e.left);
				self.compile_expression(&e.right);
				self.emit(Instruction::CmpGe);
			}
			Expression::LessThan(e) => {
				self.compile_expression(&e.left);
				self.compile_expression(&e.right);
				self.emit(Instruction::CmpLt);
			}
			Expression::LessThanEqual(e) => {
				self.compile_expression(&e.left);
				self.compile_expression(&e.right);
				self.emit(Instruction::CmpLe);
			}
			Expression::And(a) => {
				self.compile_expression(&a.left);
				self.compile_expression(&a.right);
				self.emit(Instruction::LogicAnd);
			}
			Expression::Or(o) => {
				self.compile_expression(&o.left);
				self.compile_expression(&o.right);
				self.emit(Instruction::LogicOr);
			}
			Expression::Xor(x) => {
				self.compile_expression(&x.left);
				self.compile_expression(&x.right);
				self.emit(Instruction::LogicXor);
			}
			Expression::Prefix(p) => match &p.operator {
				PrefixOperator::Minus(_) => {
					self.compile_expression(&p.expression);
					self.emit(Instruction::Negate);
				}
				PrefixOperator::Not(_) => {
					self.compile_expression(&p.expression);
					self.emit(Instruction::LogicNot);
				}
				PrefixOperator::Plus(_) => {
					self.compile_expression(&p.expression);
				}
			},
			Expression::Call(c) => {
				let arity = c.args.len();
				for arg in &c.args {
					self.compile_expression(arg);
				}
				self.emit(Instruction::Call {
					name: c.func.0.clone(),
					arity: arity as u8,
					is_procedure_call: false,
				});
			}
			Expression::Cast(c) => {
				self.compile_expression(&c.expression);
				self.emit(Instruction::Cast(c.to.ty.clone()));
			}
			Expression::Between(b) => {
				self.compile_expression(&b.value);
				self.compile_expression(&b.lower);
				self.compile_expression(&b.upper);
				self.emit(Instruction::Between);
			}
			Expression::In(i) => {
				self.compile_expression(&i.value);
				// The list is a Tuple expression
				let items = match i.list.as_ref() {
					Expression::Tuple(t) => &t.expressions,
					_ => {
						// Single-item list
						self.compile_expression(&i.list);
						self.emit(Instruction::InList {
							count: 1,
							negated: i.negated,
						});
						return;
					}
				};
				let count = items.len();
				for item in items {
					self.compile_expression(item);
				}
				self.emit(Instruction::InList {
					count: count as u16,
					negated: i.negated,
				});
			}
			Expression::If(i) => {
				// Conditional expression via jumps
				self.compile_expression(&i.condition);
				let false_jump = self.emit(Instruction::JumpIfFalsePop(0));

				// Then branch
				self.compile_expression(&i.then_expr);
				let end_jump = self.emit(Instruction::Jump(0));

				// Patch false jump to else-if/else
				let else_start = self.current_addr();
				self.patch_jump_if_false_pop(false_jump, else_start);

				// Else-if chains
				let mut end_patches = vec![end_jump];
				for else_if in &i.else_ifs {
					self.compile_expression(&else_if.condition);
					let false_jump = self.emit(Instruction::JumpIfFalsePop(0));
					self.compile_expression(&else_if.then_expr);
					let end_jump = self.emit(Instruction::Jump(0));
					end_patches.push(end_jump);
					let next_start = self.current_addr();
					self.patch_jump_if_false_pop(false_jump, next_start);
				}

				// Else branch or none
				if let Some(else_expr) = &i.else_expr {
					self.compile_expression(else_expr);
				} else {
					self.emit(Instruction::PushNone);
				}

				let end_addr = self.current_addr();
				for patch_idx in end_patches {
					self.patch_jump(patch_idx, end_addr);
				}
			}
			Expression::Parameter(p) => {
				// Parameters resolve to LoadVar at runtime
				match p {
					ParameterExpression::Positional {
						fragment,
					} => {
						self.emit(Instruction::LoadVar(fragment.clone()));
					}
					ParameterExpression::Named {
						fragment,
					} => {
						self.emit(Instruction::LoadVar(fragment.clone()));
					}
				}
			}
			// Tuple: parenthesized expressions
			Expression::Tuple(t) => {
				if t.expressions.len() == 1 {
					// Single-element tuple = parenthesized expression, compile transparently
					self.compile_expression(&t.expressions[0]);
				} else {
					// Multi-element tuple - not supported in scripting context
					self.emit(Instruction::PushNone);
				}
			}
			Expression::Map(m) => {
				if m.expressions.len() == 1 {
					match &m.expressions[0] {
						Expression::Alias(a) => {
							self.compile_expression(&a.expression);
						}
						other => {
							self.compile_expression(other);
						}
					}
				} else {
					self.emit(Instruction::PushNone);
				}
			}
			Expression::Type(type_expr) => {
				self.emit(Instruction::PushConst(Value::Type(type_expr.ty.clone())));
			}
			Expression::FieldAccess(fa) => {
				// Extract variable name from the object expression
				match fa.object.as_ref() {
					Expression::Variable(var) => {
						self.emit(Instruction::FieldAccess {
							object: var.fragment.clone(),
							field: fa.field.clone(),
						});
					}
					_ => {
						// Fallback: compile object, but field access on non-variables isn't
						// supported yet
						self.compile_expression(&fa.object);
						self.emit(Instruction::PushNone);
					}
				}
			}
			Expression::Column(_)
			| Expression::AccessSource(_)
			| Expression::Alias(_)
			| Expression::Extend(_)
			| Expression::SumTypeConstructor(_)
			| Expression::IsVariant(_) => {
				self.emit(Instruction::PushNone);
			}
		}
	}

	// ========================================================================
	// Plan compilation (operates on bump-allocated PhysicalPlan directly)
	// ========================================================================

	fn compile_plan(&mut self, plan: PhysicalPlan<'_>) -> Result<()> {
		match plan {
			// DDL — leaf instructions (no query children to materialize)
			PhysicalPlan::CreateNamespace(node) => {
				self.emit(Instruction::CreateNamespace(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateTable(node) => {
				self.emit(Instruction::CreateTable(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateRingBuffer(node) => {
				self.emit(Instruction::CreateRingBuffer(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateDictionary(node) => {
				self.emit(Instruction::CreateDictionary(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateSumType(node) => {
				self.emit(Instruction::CreateSumType(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::AlterSequence(node) => {
				self.emit(Instruction::AlterSequence(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreatePrimaryKey(node) => {
				self.emit(Instruction::CreatePrimaryKey(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreatePolicy(node) => {
				self.emit(Instruction::CreatePolicy(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateProcedure(node) => {
				self.emit(Instruction::CreateProcedure(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateSeries(node) => {
				self.emit(Instruction::CreateSeries(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateEvent(node) => {
				self.emit(Instruction::CreateEvent(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateTag(node) => {
				self.emit(Instruction::CreateTag(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateHandler(node) => {
				self.emit(Instruction::CreateHandler(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Dispatch(node) => {
				self.emit(Instruction::Dispatch(node));
				self.emit(Instruction::Emit);
			}

			// DDL (Drop) — leaf instructions
			PhysicalPlan::DropNamespace(node) => {
				self.emit(Instruction::DropNamespace(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropTable(node) => {
				self.emit(Instruction::DropTable(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropView(node) => {
				self.emit(Instruction::DropView(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropRingBuffer(node) => {
				self.emit(Instruction::DropRingBuffer(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropDictionary(node) => {
				self.emit(Instruction::DropDictionary(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropSumType(node) => {
				self.emit(Instruction::DropSumType(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropFlow(node) => {
				self.emit(Instruction::DropFlow(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropSubscription(node) => {
				self.emit(Instruction::DropSubscription(node));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DropSeries(node) => {
				self.emit(Instruction::DropSeries(node));
				self.emit(Instruction::Emit);
			}

			// DDL — nodes with query subtrees that need materialization
			PhysicalPlan::CreateDeferredView(node) => {
				self.emit(Instruction::CreateDeferredView(nodes::CreateDeferredViewNode {
					namespace: node.namespace,
					view: node.view,
					if_not_exists: node.if_not_exists,
					columns: node.columns,
					as_clause: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.as_clause,
					))),
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateTransactionalView(node) => {
				self.emit(Instruction::CreateTransactionalView(nodes::CreateTransactionalViewNode {
					namespace: node.namespace,
					view: node.view,
					if_not_exists: node.if_not_exists,
					columns: node.columns,
					as_clause: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.as_clause,
					))),
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateFlow(node) => {
				self.emit(Instruction::CreateFlow(nodes::CreateFlowNode {
					namespace: node.namespace,
					flow: node.flow,
					if_not_exists: node.if_not_exists,
					as_clause: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.as_clause,
					))),
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::CreateSubscription(node) => {
				self.emit(Instruction::CreateSubscription(nodes::CreateSubscriptionNode {
					columns: node.columns,
					as_clause: node.as_clause.map(|a| {
						Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(a)))
					}),
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::AlterFlow(node) => {
				self.emit(Instruction::AlterFlow(nodes::AlterFlowNode {
					flow: node.flow,
					action: match node.action {
						physical::AlterFlowAction::Rename {
							new_name,
						} => nodes::AlterFlowAction::Rename {
							new_name,
						},
						physical::AlterFlowAction::SetQuery {
							query,
						} => nodes::AlterFlowAction::SetQuery {
							query: Box::new(materialize_query_plan(
								crate::bump::BumpBox::into_inner(query),
							)),
						},
						physical::AlterFlowAction::Pause => nodes::AlterFlowAction::Pause,
						physical::AlterFlowAction::Resume => nodes::AlterFlowAction::Resume,
					},
				}));
				self.emit(Instruction::Emit);
			}

			// DML — materialize query subtrees inline
			PhysicalPlan::Delete(node) => {
				self.emit(Instruction::Delete(nodes::DeleteTableNode {
					input: node.input.map(|i| {
						Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))
					}),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DeleteRingBuffer(node) => {
				self.emit(Instruction::DeleteRingBuffer(nodes::DeleteRingBufferNode {
					input: node.input.map(|i| {
						Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))
					}),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertTable(node) => {
				self.emit(Instruction::InsertTable(nodes::InsertTableNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertRingBuffer(node) => {
				self.emit(Instruction::InsertRingBuffer(nodes::InsertRingBufferNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertDictionary(node) => {
				self.emit(Instruction::InsertDictionary(nodes::InsertDictionaryNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InsertSeries(node) => {
				self.emit(Instruction::InsertSeries(nodes::InsertSeriesNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DeleteSeries(node) => {
				self.emit(Instruction::DeleteSeries(nodes::DeleteSeriesNode {
					input: node.input.map(|i| {
						Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(i)))
					}),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Update(node) => {
				self.emit(Instruction::Update(nodes::UpdateTableNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::UpdateRingBuffer(node) => {
				self.emit(Instruction::UpdateRingBuffer(nodes::UpdateRingBufferNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::UpdateSeries(node) => {
				self.emit(Instruction::UpdateSeries(nodes::UpdateSeriesNode {
					input: Box::new(materialize_query_plan(crate::bump::BumpBox::into_inner(
						node.input,
					))),
					target: node.target,
				}));
				self.emit(Instruction::Emit);
			}

			// Variables
			PhysicalPlan::Declare(node) => {
				match node.value {
					physical::LetValue::Expression(expr) => {
						self.compile_expression(&expr);
					}
					physical::LetValue::Statement(plan) => {
						let inner = crate::bump::BumpBox::into_inner(plan);
						if matches!(&inner, PhysicalPlan::DefineClosure(_)) {
							// Closures push their value onto the stack directly
							self.compile_plan(inner)?;
						} else {
							let query = materialize_query_plan(inner);
							self.emit(Instruction::Query(query));
						}
					}
					physical::LetValue::EmptyFrame => {
						self.emit(Instruction::PushNone);
					}
				}
				self.emit(Instruction::DeclareVar(node.name));
			}
			PhysicalPlan::Assign(node) => {
				match node.value {
					physical::AssignValue::Expression(expr) => {
						self.compile_expression(&expr);
					}
					physical::AssignValue::Statement(plan) => {
						let query =
							materialize_query_plan(crate::bump::BumpBox::into_inner(plan));
						self.emit(Instruction::Query(query));
					}
				}
				self.emit(Instruction::StoreVar(node.name));
			}
			PhysicalPlan::Append(node) => match node {
				physical::AppendPhysicalNode::IntoVariable {
					..
				} => {
					self.compile_append(node)?;
				}
				physical::AppendPhysicalNode::Query {
					left,
					right,
				} => {
					self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Append(
						physical::AppendPhysicalNode::Query {
							left,
							right,
						},
					))));
					self.emit(Instruction::Emit);
				}
			},

			// Control flow
			PhysicalPlan::Conditional(node) => {
				self.compile_conditional(node)?;
			}
			PhysicalPlan::Loop(node) => {
				self.compile_loop(node)?;
			}
			PhysicalPlan::While(node) => {
				self.compile_while(node)?;
			}
			PhysicalPlan::For(node) => {
				self.compile_for(node)?;
			}
			PhysicalPlan::Break => {
				self.compile_break()?;
			}
			PhysicalPlan::Continue => {
				self.compile_continue()?;
			}

			// User-defined functions
			PhysicalPlan::DefineFunction(node) => {
				let mut body_compiler = InstructionCompiler::new();
				for plan in node.body {
					body_compiler.compile_plan(plan)?;
				}
				body_compiler.emit(Instruction::Halt);
				let compiled_func = CompiledFunctionDef {
					name: node.name,
					parameters: node.parameters,
					return_type: node.return_type,
					body: body_compiler.instructions,
				};
				self.emit(Instruction::DefineFunction(compiled_func));
			}
			PhysicalPlan::CallFunction(node) => {
				let arity = node.arguments.len();
				for arg in &node.arguments {
					self.compile_expression(arg);
				}
				self.emit(Instruction::Call {
					name: node.name,
					arity: arity as u8,
					is_procedure_call: node.is_procedure_call,
				});
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Return(node) => {
				if let Some(expr) = node.value {
					self.compile_expression(&expr);
					self.emit(Instruction::ReturnValue);
				} else {
					self.emit(Instruction::ReturnVoid);
				}
			}

			// Closures
			PhysicalPlan::DefineClosure(node) => {
				let mut body_compiler = InstructionCompiler::new();
				for plan in node.body {
					body_compiler.compile_plan(plan)?;
				}
				body_compiler.emit(Instruction::Halt);
				let captures = scan_free_variables(&body_compiler.instructions, &node.parameters);
				let compiled_closure = CompiledClosureDef {
					parameters: node.parameters,
					body: body_compiler.instructions,
					captures,
				};
				self.emit(Instruction::DefineClosure(compiled_closure));
			}

			// Query operations — materialize to QueryPlan and emit
			PhysicalPlan::TableScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::TableVirtualScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableVirtualScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::ViewScan(node) => {
				self.emit(Instruction::Query(QueryPlan::ViewScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RingBufferScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RingBufferScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::FlowScan(node) => {
				self.emit(Instruction::Query(QueryPlan::FlowScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::DictionaryScan(node) => {
				self.emit(Instruction::Query(QueryPlan::DictionaryScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::SeriesScan(node) => {
				self.emit(Instruction::Query(QueryPlan::SeriesScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::IndexScan(node) => {
				self.emit(Instruction::Query(QueryPlan::IndexScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RowPointLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowPointLookup(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RowListLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowListLookup(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::RowRangeScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RowRangeScan(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Aggregate(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Aggregate(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Distinct(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Distinct(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Assert(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Assert(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Filter(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Filter(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::JoinInner(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::JoinInner(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::JoinLeft(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::JoinLeft(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::JoinNatural(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::JoinNatural(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Take(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Take(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Sort(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Sort(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Map(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Map(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Extend(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Extend(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Patch(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Patch(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Apply(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Apply(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::InlineData(node) => {
				self.emit(Instruction::Query(QueryPlan::InlineData(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Generator(node) => {
				self.emit(Instruction::Query(QueryPlan::Generator(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Window(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Window(node))));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Variable(node) => {
				self.emit(Instruction::Query(QueryPlan::Variable(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Environment(node) => {
				self.emit(Instruction::Query(QueryPlan::Environment(node)));
				self.emit(Instruction::Emit);
			}
			PhysicalPlan::Scalarize(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Scalarize(node))));
				self.emit(Instruction::Emit);
			}
		}
		Ok(())
	}

	fn compile_conditional(&mut self, node: physical::ConditionalNode<'_>) -> Result<()> {
		let mut end_patches: Vec<usize> = Vec::new();

		// IF cond THEN body
		let false_jump = self.emit_conditional_jump(node.condition);
		self.emit(Instruction::EnterScope(ScopeType::Conditional));
		self.scope_depth += 1;
		self.compile_plan(crate::bump::BumpBox::into_inner(node.then_branch))?;
		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		let end_jump = self.emit(Instruction::Jump(0));
		end_patches.push(end_jump);

		let else_if_start = self.current_addr();
		self.patch_jump_if_false_pop(false_jump, else_if_start);

		// ELSE IF branches
		for else_if in node.else_ifs {
			let false_jump = self.emit_conditional_jump(else_if.condition);
			self.emit(Instruction::EnterScope(ScopeType::Conditional));
			self.scope_depth += 1;
			self.compile_plan(crate::bump::BumpBox::into_inner(else_if.then_branch))?;
			self.scope_depth -= 1;
			self.emit(Instruction::ExitScope);
			let end_jump = self.emit(Instruction::Jump(0));
			end_patches.push(end_jump);

			let next_start = self.current_addr();
			self.patch_jump_if_false_pop(false_jump, next_start);
		}

		// ELSE branch
		if let Some(else_branch) = node.else_branch {
			self.emit(Instruction::EnterScope(ScopeType::Conditional));
			self.scope_depth += 1;
			self.compile_plan(crate::bump::BumpBox::into_inner(else_branch))?;
			self.scope_depth -= 1;
			self.emit(Instruction::ExitScope);
		}

		let end_addr = self.current_addr();
		for patch_idx in end_patches {
			self.patch_jump(patch_idx, end_addr);
		}

		self.emit(Instruction::Nop);
		Ok(())
	}

	fn compile_loop(&mut self, node: physical::LoopNode<'_>) -> Result<()> {
		let loop_start = self.current_addr();

		self.emit(Instruction::EnterScope(ScopeType::Loop));
		self.scope_depth += 1;

		self.loop_stack.push(LoopContext {
			continue_addr: loop_start,
			break_patches: Vec::new(),
			scope_depth: self.scope_depth,
		});

		for plan in node.body {
			self.compile_plan(plan)?;
		}

		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		self.emit(Instruction::Jump(loop_start));

		let loop_end = self.current_addr();
		self.emit(Instruction::Nop);

		let ctx = self.loop_stack.pop().unwrap();
		for patch_idx in ctx.break_patches {
			self.patch_break_or_continue(patch_idx, loop_end);
		}
		Ok(())
	}

	fn compile_while(&mut self, node: physical::WhileNode<'_>) -> Result<()> {
		let condition_addr = self.current_addr();
		let false_jump = self.emit_conditional_jump(node.condition);

		self.emit(Instruction::EnterScope(ScopeType::Loop));
		self.scope_depth += 1;

		self.loop_stack.push(LoopContext {
			continue_addr: condition_addr,
			break_patches: Vec::new(),
			scope_depth: self.scope_depth,
		});

		for plan in node.body {
			self.compile_plan(plan)?;
		}

		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		self.emit(Instruction::Jump(condition_addr));

		let loop_end = self.current_addr();
		self.emit(Instruction::Nop);

		self.patch_jump_if_false_pop(false_jump, loop_end);

		let ctx = self.loop_stack.pop().unwrap();
		for patch_idx in ctx.break_patches {
			self.patch_break_or_continue(patch_idx, loop_end);
		}
		Ok(())
	}

	fn compile_for(&mut self, node: physical::ForNode<'_>) -> Result<()> {
		self.compile_plan_for_iterable(crate::bump::BumpBox::into_inner(node.iterable))?;
		self.emit(Instruction::ForInit {
			variable_name: node.variable_name.clone(),
		});

		let for_next_addr = self.current_addr();
		let for_next_idx = self.emit(Instruction::ForNext {
			variable_name: node.variable_name,
			addr: 0,
		});

		self.emit(Instruction::EnterScope(ScopeType::Loop));
		self.scope_depth += 1;

		self.loop_stack.push(LoopContext {
			continue_addr: for_next_addr,
			break_patches: Vec::new(),
			scope_depth: self.scope_depth,
		});

		for plan in node.body {
			self.compile_plan(plan)?;
		}

		self.scope_depth -= 1;
		self.emit(Instruction::ExitScope);
		self.emit(Instruction::Jump(for_next_addr));

		let loop_end = self.current_addr();
		self.emit(Instruction::Nop);

		self.patch_for_next(for_next_idx, loop_end);

		let ctx = self.loop_stack.pop().unwrap();
		for patch_idx in ctx.break_patches {
			self.patch_break_or_continue(patch_idx, loop_end);
		}
		Ok(())
	}

	/// Compile a plan that will be used as an iterable (for FOR loops).
	/// Query plans emit a Query instruction (no Emit); non-query plans delegate to compile_plan.
	fn compile_plan_for_iterable(&mut self, plan: PhysicalPlan<'_>) -> Result<()> {
		match plan {
			// Query leaf nodes — emit directly without Emit
			PhysicalPlan::TableScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableScan(node)));
			}
			PhysicalPlan::TableVirtualScan(node) => {
				self.emit(Instruction::Query(QueryPlan::TableVirtualScan(node)));
			}
			PhysicalPlan::ViewScan(node) => {
				self.emit(Instruction::Query(QueryPlan::ViewScan(node)));
			}
			PhysicalPlan::RingBufferScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RingBufferScan(node)));
			}
			PhysicalPlan::FlowScan(node) => {
				self.emit(Instruction::Query(QueryPlan::FlowScan(node)));
			}
			PhysicalPlan::DictionaryScan(node) => {
				self.emit(Instruction::Query(QueryPlan::DictionaryScan(node)));
			}
			PhysicalPlan::SeriesScan(node) => {
				self.emit(Instruction::Query(QueryPlan::SeriesScan(node)));
			}
			PhysicalPlan::IndexScan(node) => {
				self.emit(Instruction::Query(QueryPlan::IndexScan(node)));
			}
			PhysicalPlan::RowPointLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowPointLookup(node)));
			}
			PhysicalPlan::RowListLookup(node) => {
				self.emit(Instruction::Query(QueryPlan::RowListLookup(node)));
			}
			PhysicalPlan::RowRangeScan(node) => {
				self.emit(Instruction::Query(QueryPlan::RowRangeScan(node)));
			}
			PhysicalPlan::InlineData(node) => {
				self.emit(Instruction::Query(QueryPlan::InlineData(node)));
			}
			PhysicalPlan::Generator(node) => {
				self.emit(Instruction::Query(QueryPlan::Generator(node)));
			}
			PhysicalPlan::Variable(node) => {
				self.emit(Instruction::Query(QueryPlan::Variable(node)));
			}
			PhysicalPlan::Environment(node) => {
				self.emit(Instruction::Query(QueryPlan::Environment(node)));
			}
			// Query recursive nodes — materialize then emit
			PhysicalPlan::Aggregate(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Aggregate(node))));
			}
			PhysicalPlan::Distinct(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Distinct(node))));
			}
			PhysicalPlan::Assert(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Assert(node))));
			}
			PhysicalPlan::Filter(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Filter(node))));
			}
			PhysicalPlan::JoinInner(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::JoinInner(node))));
			}
			PhysicalPlan::JoinLeft(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::JoinLeft(node))));
			}
			PhysicalPlan::JoinNatural(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::JoinNatural(node))));
			}
			PhysicalPlan::Take(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Take(node))));
			}
			PhysicalPlan::Sort(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Sort(node))));
			}
			PhysicalPlan::Map(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Map(node))));
			}
			PhysicalPlan::Extend(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Extend(node))));
			}
			PhysicalPlan::Patch(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Patch(node))));
			}
			PhysicalPlan::Apply(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Apply(node))));
			}
			PhysicalPlan::Window(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Window(node))));
			}
			PhysicalPlan::Scalarize(node) => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Scalarize(node))));
			}
			// Non-query plans (DDL, DML, control flow) — delegate to full compile_plan
			other => {
				self.compile_plan(other)?;
			}
		}
		Ok(())
	}

	fn compile_append(&mut self, node: physical::AppendPhysicalNode<'_>) -> Result<()> {
		match node {
			physical::AppendPhysicalNode::IntoVariable {
				target,
				source,
			} => {
				match source {
					physical::AppendPhysicalSource::Statement(plans) => {
						// Compile source plans as query (no Emit - we want the value on stack)
						for plan in plans {
							self.compile_plan_for_iterable(plan)?;
						}
					}
					physical::AppendPhysicalSource::Inline(inline) => {
						self.emit(Instruction::Query(QueryPlan::InlineData(inline)));
					}
				}
				self.emit(Instruction::Append {
					target,
				});
				Ok(())
			}
			physical::AppendPhysicalNode::Query {
				left,
				right,
			} => {
				self.emit(Instruction::Query(materialize_query_plan(PhysicalPlan::Append(
					physical::AppendPhysicalNode::Query {
						left,
						right,
					},
				))));
				self.emit(Instruction::Emit);
				Ok(())
			}
		}
	}

	fn compile_break(&mut self) -> Result<()> {
		let loop_ctx = self.loop_stack.last_mut().ok_or(RqlError::BreakOutsideLoop)?;
		let exit_scopes = self.scope_depth - loop_ctx.scope_depth;
		let idx = self.emit(Instruction::Break {
			exit_scopes,
			addr: 0,
		});
		self.loop_stack.last_mut().unwrap().break_patches.push(idx);
		Ok(())
	}

	fn compile_continue(&mut self) -> Result<()> {
		let loop_ctx = self.loop_stack.last().ok_or(RqlError::ContinueOutsideLoop)?;
		let exit_scopes = self.scope_depth - loop_ctx.scope_depth;
		let continue_addr = loop_ctx.continue_addr;
		self.emit(Instruction::Continue {
			exit_scopes,
			addr: continue_addr,
		});
		Ok(())
	}

	fn patch_jump(&mut self, idx: usize, addr: Addr) {
		if let Instruction::Jump(ref mut target) = self.instructions[idx] {
			*target = addr;
		}
	}

	fn patch_jump_if_false_pop(&mut self, idx: usize, addr: Addr) {
		if let Instruction::JumpIfFalsePop(ref mut target) = self.instructions[idx] {
			*target = addr;
		}
	}

	fn patch_break_or_continue(&mut self, idx: usize, addr: Addr) {
		match &mut self.instructions[idx] {
			Instruction::Break {
				addr: target,
				..
			} => {
				*target = addr;
			}
			Instruction::Continue {
				addr: target,
				..
			} => {
				*target = addr;
			}
			_ => {}
		}
	}

	fn patch_for_next(&mut self, idx: usize, addr: Addr) {
		if let Instruction::ForNext {
			addr: ref mut target,
			..
		} = self.instructions[idx]
		{
			*target = addr;
		}
	}
}
