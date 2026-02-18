// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod alter;
pub mod append;
pub mod create;
pub mod function;
pub mod mutate;
pub mod query;
pub mod resolver;
pub mod row_predicate;
pub mod variable;

use std::fmt::{Display, Formatter};

use query::window::WindowNode;
use reifydb_catalog::catalog::{
	Catalog, ringbuffer::RingBufferColumnToCreate, subscription::SubscriptionColumnToCreate,
	table::TableColumnToCreate, view::ViewColumnToCreate,
};
use reifydb_core::{
	common::{IndexType, JoinType},
	interface::{
		catalog::policy::{ColumnPolicyKind, ColumnSaturationPolicy},
		resolved::{ResolvedColumn, ResolvedIndex, ResolvedPrimitive},
	},
	sort::{SortDirection, SortKey},
};
use reifydb_transaction::transaction::{AsTransaction, command::CommandTransaction, query::QueryTransaction};
use reifydb_type::{error::diagnostic::ast::unsupported_ast_node, fragment::Fragment, return_error};
use tracing::instrument;

use crate::{
	ast::{
		ast::{Ast, AstInfix, AstPolicy, AstPolicyKind, AstStatement, AstType, InfixOperator},
		identifier::{
			MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier,
			MaybeQualifiedIndexIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedSequenceIdentifier, MaybeQualifiedTableIdentifier,
			MaybeQualifiedTransactionalViewIdentifier,
		},
	},
	bump::{Bump, BumpBox, BumpFragment, BumpVec},
	expression::{AliasExpression, Expression},
	plan::logical::alter::{flow::AlterFlowNode, table::AlterTableNode, view::AlterViewNode},
};

pub(crate) struct Compiler<'bump> {
	pub catalog: Catalog,
	pub bump: &'bump Bump,
}

/// Compile AST to logical plan using any transaction type that implements IntoTransaction
#[instrument(name = "rql::compile::logical", level = "trace", skip(bump, catalog, tx, ast))]
pub fn compile_logical<'b, T: AsTransaction>(
	bump: &'b Bump,
	catalog: &Catalog,
	tx: &mut T,
	ast: AstStatement<'b>,
) -> crate::Result<BumpVec<'b, LogicalPlan<'b>>> {
	Compiler {
		catalog: catalog.clone(),
		bump,
	}
	.compile(ast, tx)
}

#[instrument(name = "rql::compile::logical_query", level = "trace", skip(bump, catalog, tx, ast))]
pub fn compile_logical_query<'b>(
	bump: &'b Bump,
	catalog: &Catalog,
	tx: &mut QueryTransaction,
	ast: AstStatement<'b>,
) -> crate::Result<BumpVec<'b, LogicalPlan<'b>>> {
	compile_logical(bump, catalog, tx, ast)
}

#[instrument(name = "rql::compile::logical_command", level = "trace", skip(bump, catalog, tx, ast))]
pub fn compile_logical_command<'b>(
	bump: &'b Bump,
	catalog: &Catalog,
	tx: &mut CommandTransaction,
	ast: AstStatement<'b>,
) -> crate::Result<BumpVec<'b, LogicalPlan<'b>>> {
	compile_logical(bump, catalog, tx, ast)
}

impl<'bump> Compiler<'bump> {
	pub fn compile<T: AsTransaction>(
		&self,
		ast: AstStatement<'bump>,
		tx: &mut T,
	) -> crate::Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		if ast.is_empty() {
			return Ok(BumpVec::new_in(self.bump));
		}

		let ast_len = ast.len();
		let has_pipes = ast.has_pipes;
		let ast_vec = ast.nodes; // Extract the inner Vec

		// Note: UPDATE and DELETE no longer use pipeline syntax - they have self-contained syntax

		// Check if this is a piped query that should be wrapped in
		// Pipeline
		if has_pipes && ast_len > 1 {
			// This uses pipe operators - create a Pipeline operator
			let mut pipeline_nodes = BumpVec::with_capacity_in(ast_len, self.bump);
			for node in ast_vec {
				pipeline_nodes.push(self.compile_single(node, tx)?);
			}
			let mut result = BumpVec::with_capacity_in(1, self.bump);
			result.push(LogicalPlan::Pipeline(PipelineNode {
				steps: pipeline_nodes,
			}));
			return Ok(result);
		}

		// Normal compilation (not piped)
		let mut result = BumpVec::with_capacity_in(ast_len, self.bump);
		for node in ast_vec {
			result.push(self.compile_single(node, tx)?);
		}
		Ok(result)
	}

	// Helper to compile a single AST operator
	pub fn compile_single<T: AsTransaction>(
		&self,
		node: Ast<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		match node {
			Ast::Create(node) => self.compile_create(node, tx),
			Ast::Alter(node) => self.compile_alter(node, tx),
			Ast::Delete(node) => self.compile_delete(node, tx),
			Ast::Insert(node) => self.compile_insert(node, tx),
			Ast::Update(node) => self.compile_update(node, tx),
			Ast::Append(node) => self.compile_append(node, tx),
			Ast::If(node) => self.compile_if(node, tx),
			Ast::Loop(node) => self.compile_loop(node, tx),
			Ast::While(node) => self.compile_while(node, tx),
			Ast::For(node) => self.compile_for(node, tx),
			Ast::Break(_) => Ok(LogicalPlan::Break),
			Ast::Continue(_) => Ok(LogicalPlan::Continue),
			Ast::Let(node) => self.compile_let(node, tx),
			Ast::StatementExpression(node) => {
				// Compile the inner expression and wrap it in a MAP
				self.compile_scalar_as_map(BumpBox::into_inner(node.expression))
			}
			Ast::Prefix(node) => {
				// Prefix operations as statements - wrap in MAP
				self.compile_scalar_as_map(Ast::Prefix(node))
			}
			Ast::Infix(infix_node) => {
				match infix_node.operator {
					// Assignment operations - variable assignment with = operator
					InfixOperator::Assign(_) => {
						// This is a variable assignment statement
						self.compile_infix(infix_node)
					}
					// Expression-like operations - wrap in MAP
					InfixOperator::Add(_)
					| InfixOperator::Subtract(_)
					| InfixOperator::Multiply(_)
					| InfixOperator::Divide(_)
					| InfixOperator::Rem(_)
					| InfixOperator::Equal(_)
					| InfixOperator::NotEqual(_)
					| InfixOperator::GreaterThan(_)
					| InfixOperator::LessThan(_)
					| InfixOperator::GreaterThanEqual(_)
					| InfixOperator::LessThanEqual(_)
					| InfixOperator::And(_)
					| InfixOperator::Or(_)
					| InfixOperator::Xor(_)
					| InfixOperator::Call(_)
					| InfixOperator::As(_)
					| InfixOperator::TypeAscription(_)
					| InfixOperator::In(_)
					| InfixOperator::NotIn(_) => self.compile_scalar_as_map(Ast::Infix(infix_node)),

					// Statement-like operations - compile directly
					InfixOperator::Arrow(_)
					| InfixOperator::AccessTable(_)
					| InfixOperator::AccessNamespace(_) => self.compile_infix(infix_node),
				}
			}
			Ast::Aggregate(node) => self.compile_aggregate(node),
			Ast::Assert(node) => self.compile_assert(node),
			Ast::Filter(node) => self.compile_filter(node),
			Ast::From(node) => self.compile_from(node, tx),
			Ast::Join(node) => self.compile_join(node, tx),
			Ast::Take(node) => self.compile_take(node),
			Ast::Sort(node) => self.compile_sort(node),
			Ast::Distinct(node) => self.compile_distinct(node),
			Ast::Map(node) => self.compile_map(node),
			Ast::Extend(node) => self.compile_extend(node),
			Ast::Patch(node) => self.compile_patch(node),
			Ast::Apply(node) => self.compile_apply(node),
			Ast::Window(node) => self.compile_window(node),
			Ast::Identifier(ref id) => {
				return_error!(unsupported_ast_node(
					id.token.fragment.to_owned(),
					"standalone identifier"
				))
			}
			// Auto-wrap scalar expressions into MAP constructs
			Ast::Literal(_) | Ast::Variable(_) => self.compile_scalar_as_map(node),
			// Function calls: check if it's potentially a user-defined function
			Ast::CallFunction(call_node) => {
				// If no namespaces, treat as potential user-defined function call
				if call_node.function.namespaces.is_empty() {
					self.compile_call_function(call_node)
				} else {
					// Namespaced function calls are always built-in functions
					self.compile_scalar_as_map(Ast::CallFunction(call_node))
				}
			}
			Ast::Block(_) => {
				// Blocks are handled by their parent constructs (IF, LOOP, etc.)
				return_error!(unsupported_ast_node(
					node.token().fragment.to_owned(),
					"standalone block"
				))
			}
			Ast::DefFunction(node) => self.compile_def_function(node, tx),
			Ast::Return(node) => self.compile_return(node),
			node => {
				let node_type =
					format!("{:?}", node).split('(').next().unwrap_or("Unknown").to_string();
				return_error!(unsupported_ast_node(node.token().fragment.to_owned(), &node_type))
			}
		}
	}

	// Helper to wrap a scalar expression in a MAP { "value": expression }
	// Instead of creating synthetic AST nodes (which would require a bump allocator),
	// this directly compiles the scalar and wraps the result in a MapNode.
	fn compile_scalar_as_map(&self, scalar_node: Ast<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		use crate::expression::{AliasExpression, ExpressionCompiler, IdentExpression};

		let fragment = scalar_node.token().fragment.to_owned();
		let expr = ExpressionCompiler::compile(scalar_node)?;
		let alias_expr = AliasExpression {
			alias: IdentExpression(Fragment::internal("value")),
			expression: Box::new(expr),
			fragment,
		};

		Ok(LogicalPlan::Map(MapNode {
			map: vec![crate::expression::Expression::Alias(alias_expr)],
		}))
	}

	fn compile_infix(&self, node: AstInfix<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		match node.operator {
			InfixOperator::Assign(_token) => {
				// This is a variable assignment statement
				// Extract the variable name from the left side
				let variable = match BumpBox::into_inner(node.left) {
					crate::ast::ast::Ast::Variable(var) => var,
					_ => {
						return_error!(unsupported_ast_node(
							node.token.fragment.to_owned(),
							"assignment to non-variable"
						))
					}
				};

				// Convert the right side to an expression
				let expr = crate::expression::ExpressionCompiler::compile(BumpBox::into_inner(
					node.right,
				))?;
				let value = AssignValue::Expression(expr);

				// Extract variable name (remove $ prefix if present)
				let name_text = variable.token.fragment.text();
				let clean_name = if name_text.starts_with('$') {
					&name_text[1..]
				} else {
					name_text
				};

				Ok(LogicalPlan::Assign(AssignNode {
					name: BumpFragment::internal(self.bump, clean_name),
					value,
				}))
			}
			_ => {
				// Other infix operations are not supported as standalone statements
				return_error!(unsupported_ast_node(
					node.token.fragment.to_owned(),
					"infix operation as statement"
				))
			}
		}
	}
}

#[derive(Debug)]
pub enum LogicalPlan<'bump> {
	CreateDeferredView(CreateDeferredViewNode<'bump>),
	CreateTransactionalView(CreateTransactionalViewNode<'bump>),
	CreateNamespace(CreateNamespaceNode<'bump>),
	CreateSequence(CreateSequenceNode<'bump>),
	CreateTable(CreateTableNode<'bump>),
	CreateRingBuffer(CreateRingBufferNode<'bump>),
	CreateDictionary(CreateDictionaryNode<'bump>),
	CreateSumType(CreateSumTypeNode<'bump>),
	CreateFlow(CreateFlowNode<'bump>),
	CreateIndex(CreateIndexNode<'bump>),
	CreateSubscription(CreateSubscriptionNode<'bump>),
	// Alter
	AlterSequence(AlterSequenceNode<'bump>),
	AlterTable(AlterTableNode<'bump>),
	AlterView(AlterViewNode<'bump>),
	AlterFlow(AlterFlowNode<'bump>),
	// Mutate
	DeleteTable(DeleteTableNode<'bump>),
	DeleteRingBuffer(DeleteRingBufferNode<'bump>),
	InsertTable(InsertTableNode<'bump>),
	InsertRingBuffer(InsertRingBufferNode<'bump>),
	InsertDictionary(InsertDictionaryNode<'bump>),
	Update(UpdateTableNode<'bump>),
	UpdateRingBuffer(UpdateRingBufferNode<'bump>),
	// Variable assignment
	Declare(DeclareNode<'bump>),
	Assign(AssignNode<'bump>),
	Append(AppendNode<'bump>),
	// Control flow
	Conditional(ConditionalNode<'bump>),
	Loop(LoopNode<'bump>),
	While(WhileNode<'bump>),
	For(ForNode<'bump>),
	Break,
	Continue,
	// Query
	Aggregate(AggregateNode),
	Distinct(DistinctNode<'bump>),
	Assert(AssertNode),
	Filter(FilterNode),
	JoinInner(JoinInnerNode<'bump>),
	JoinLeft(JoinLeftNode<'bump>),
	JoinNatural(JoinNaturalNode<'bump>),
	Take(TakeNode),
	Order(OrderNode),
	Map(MapNode),
	Extend(ExtendNode),
	Patch(PatchNode),
	Apply(ApplyNode<'bump>),
	InlineData(InlineDataNode),
	PrimitiveScan(PrimitiveScanNode),
	Window(WindowNode),
	Generator(GeneratorNode<'bump>),
	VariableSource(VariableSourceNode<'bump>),
	Environment(EnvironmentNode),
	// Auto-scalarization for 1x1 frames in scalar contexts
	Scalarize(ScalarizeNode<'bump>),
	// Pipeline wrapper for piped operations
	Pipeline(PipelineNode<'bump>),
	// User-defined functions
	DefineFunction(function::DefineFunctionNode<'bump>),
	Return(function::ReturnNode),
	CallFunction(function::CallFunctionNode<'bump>),
}

#[derive(Debug)]
pub struct PipelineNode<'bump> {
	pub steps: BumpVec<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct ScalarizeNode<'bump> {
	pub input: BumpBox<'bump, LogicalPlan<'bump>>,
	pub fragment: BumpFragment<'bump>,
}

#[derive(Debug)]
pub enum LetValue<'bump> {
	Expression(Expression),                        // scalar/column expression
	Statement(BumpVec<'bump, LogicalPlan<'bump>>), // query pipeline as logical plans
	EmptyFrame,                                    // LET $x = [] → empty Frame
}

impl<'bump> Display for LetValue<'bump> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			LetValue::Expression(expr) => write!(f, "{}", expr),
			LetValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
			LetValue::EmptyFrame => write!(f, "EmptyFrame"),
		}
	}
}

#[derive(Debug)]
pub enum AssignValue<'bump> {
	Expression(Expression),                        // scalar/column expression
	Statement(BumpVec<'bump, LogicalPlan<'bump>>), // query pipeline as logical plans
}

impl<'bump> Display for AssignValue<'bump> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			AssignValue::Expression(expr) => write!(f, "{}", expr),
			AssignValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug)]
pub struct DeclareNode<'bump> {
	pub name: BumpFragment<'bump>,
	pub value: LetValue<'bump>,
}

#[derive(Debug)]
pub struct AssignNode<'bump> {
	pub name: BumpFragment<'bump>,
	pub value: AssignValue<'bump>,
}

#[derive(Debug)]
pub struct ConditionalNode<'bump> {
	pub condition: Expression,
	pub then_branch: BumpBox<'bump, LogicalPlan<'bump>>,
	pub else_ifs: Vec<ElseIfBranch<'bump>>,
	pub else_branch: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct ElseIfBranch<'bump> {
	pub condition: Expression,
	pub then_branch: BumpBox<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct LoopNode<'bump> {
	pub body: Vec<BumpVec<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct WhileNode<'bump> {
	pub condition: Expression,
	pub body: Vec<BumpVec<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct ForNode<'bump> {
	pub variable_name: BumpFragment<'bump>,
	pub iterable: BumpVec<'bump, LogicalPlan<'bump>>,
	pub body: Vec<BumpVec<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyDef<'bump> {
	pub columns: Vec<PrimaryKeyColumn<'bump>>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyColumn<'bump> {
	pub column: BumpFragment<'bump>,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub struct CreateDeferredViewNode<'bump> {
	pub view: MaybeQualifiedDeferredViewIdentifier<'bump>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: BumpVec<'bump, LogicalPlan<'bump>>,
	pub primary_key: Option<PrimaryKeyDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'bump> {
	pub view: MaybeQualifiedTransactionalViewIdentifier<'bump>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: BumpVec<'bump, LogicalPlan<'bump>>,
	pub primary_key: Option<PrimaryKeyDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateNamespaceNode<'bump> {
	pub segments: Vec<BumpFragment<'bump>>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode<'bump> {
	pub sequence: MaybeQualifiedSequenceIdentifier<'bump>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode<'bump> {
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
	pub primary_key: Option<PrimaryKeyDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateRingBufferNode<'bump> {
	pub ringbuffer: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
	pub primary_key: Option<PrimaryKeyDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateDictionaryNode<'bump> {
	pub dictionary: MaybeQualifiedDictionaryIdentifier<'bump>,
	pub if_not_exists: bool,
	pub value_type: AstType<'bump>,
	pub id_type: AstType<'bump>,
}

#[derive(Debug)]
pub struct CreateSumTypeNode<'bump> {
	pub name: crate::ast::identifier::MaybeQualifiedSumTypeIdentifier<'bump>,
	pub if_not_exists: bool,
	pub variants: Vec<crate::ast::ast::AstVariantDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateFlowNode<'bump> {
	pub flow: MaybeQualifiedFlowIdentifier<'bump>,
	pub if_not_exists: bool,
	pub as_clause: BumpVec<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct AlterSequenceNode<'bump> {
	pub sequence: MaybeQualifiedSequenceIdentifier<'bump>,
	pub column: MaybeQualifiedColumnIdentifier<'bump>,
	pub value: Expression,
}

#[derive(Debug)]
pub struct CreateIndexNode<'bump> {
	pub index_type: IndexType,
	pub index: MaybeQualifiedIndexIdentifier<'bump>,
	pub columns: Vec<IndexColumn<'bump>>,
	pub filter: Vec<Expression>,
	pub map: Option<Expression>,
}

#[derive(Debug)]
pub struct CreateSubscriptionNode<'bump> {
	pub columns: Vec<SubscriptionColumnToCreate>,
	pub as_clause: BumpVec<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct IndexColumn<'bump> {
	pub column: BumpFragment<'bump>,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub struct DeleteTableNode<'bump> {
	pub target: Option<MaybeQualifiedTableIdentifier<'bump>>,
	pub input: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct DeleteRingBufferNode<'bump> {
	pub target: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub input: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct InsertTableNode<'bump> {
	pub target: MaybeQualifiedTableIdentifier<'bump>,
	pub source: BumpBox<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct InsertRingBufferNode<'bump> {
	pub target: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub source: BumpBox<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct InsertDictionaryNode<'bump> {
	pub target: MaybeQualifiedDictionaryIdentifier<'bump>,
	pub source: BumpBox<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct UpdateTableNode<'bump> {
	pub target: Option<MaybeQualifiedTableIdentifier<'bump>>,
	pub input: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct UpdateRingBufferNode<'bump> {
	pub target: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub input: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct AggregateNode {
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

#[derive(Debug)]
pub struct DistinctNode<'bump> {
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'bump>>,
}

#[derive(Debug)]
pub struct AssertNode {
	pub condition: Expression,
	pub message: Option<String>,
}

#[derive(Debug)]
pub struct FilterNode {
	pub condition: Expression,
}

#[derive(Debug)]
pub struct JoinInnerNode<'bump> {
	pub with: BumpVec<'bump, LogicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Option<BumpFragment<'bump>>,
}

#[derive(Debug)]
pub struct JoinLeftNode<'bump> {
	pub with: BumpVec<'bump, LogicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Option<BumpFragment<'bump>>,
}

#[derive(Debug)]
pub struct JoinNaturalNode<'bump> {
	pub with: BumpVec<'bump, LogicalPlan<'bump>>,
	pub join_type: JoinType,
	pub alias: Option<BumpFragment<'bump>>,
}

#[derive(Debug)]
pub struct TakeNode {
	pub take: usize,
}

#[derive(Debug)]
pub struct OrderNode {
	pub by: Vec<SortKey>,
}

#[derive(Debug)]
pub struct MapNode {
	pub map: Vec<Expression>,
}

#[derive(Debug)]
pub struct ExtendNode {
	pub extend: Vec<Expression>,
}

#[derive(Debug)]
pub struct PatchNode {
	pub assignments: Vec<Expression>,
}

#[derive(Debug)]
pub struct ApplyNode<'bump> {
	pub operator: BumpFragment<'bump>,
	pub arguments: Vec<Expression>,
}

#[derive(Debug)]
pub struct InlineDataNode {
	pub rows: Vec<Vec<AliasExpression>>,
}

#[derive(Debug)]
pub struct PrimitiveScanNode {
	pub source: ResolvedPrimitive,
	pub columns: Option<Vec<ResolvedColumn>>,
	pub index: Option<ResolvedIndex>,
}

#[derive(Debug)]
pub struct GeneratorNode<'bump> {
	pub name: BumpFragment<'bump>,
	pub expressions: Vec<Expression>,
}

#[derive(Debug)]
pub struct VariableSourceNode<'bump> {
	pub name: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct EnvironmentNode {}

#[derive(Debug)]
pub enum AppendNode<'bump> {
	IntoVariable {
		target: BumpFragment<'bump>,
		source: AppendSourcePlan<'bump>,
	},
	Query {
		with: BumpVec<'bump, LogicalPlan<'bump>>,
	},
}

#[derive(Debug)]
pub enum AppendSourcePlan<'bump> {
	Statement(BumpVec<'bump, LogicalPlan<'bump>>),
	Inline(InlineDataNode),
}

pub(crate) fn convert_policy(ast: &AstPolicy) -> ColumnPolicyKind {
	use ColumnPolicyKind::*;

	match ast.policy {
		AstPolicyKind::Saturation => {
			if ast.value.is_literal_none() {
				return Saturation(ColumnSaturationPolicy::None);
			}
			let ident = ast.value.as_identifier().text();
			match ident {
				"error" => Saturation(ColumnSaturationPolicy::Error),
				// "saturate" => Some(Saturation(Saturate)),
				// "wrap" => Some(Saturation(Wrap)),
				// "zero" => Some(Saturation(Zero)),
				_ => unimplemented!(),
			}
		}
		AstPolicyKind::Default => unimplemented!(),
		AstPolicyKind::NotNone => unimplemented!(),
	}
}
