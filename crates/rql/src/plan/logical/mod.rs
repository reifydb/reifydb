// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod alter;
pub mod append;
pub mod create;
pub mod drop;
pub mod function;
pub mod mutate;
pub mod query;
pub mod resolver;
pub mod row_predicate;
pub mod scripting;
pub mod series_predicate;
pub mod variable;

use std::fmt::{Display, Formatter};

use query::window::WindowNode;
use reifydb_catalog::catalog::{
	Catalog, ringbuffer::RingBufferColumnToCreate, series::SeriesColumnToCreate,
	subscription::SubscriptionColumnToCreate, table::TableColumnToCreate, view::ViewColumnToCreate,
};
use reifydb_core::{
	common::{IndexType, JoinType},
	interface::{
		catalog::{property::ColumnPropertyKind, series::TimestampPrecision},
		resolved::{ResolvedColumn, ResolvedIndex, ResolvedPrimitive},
	},
	sort::{SortDirection, SortKey},
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction, query::QueryTransaction};
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	ast::{
		ast::{Ast, AstInfix, AstProcedureParam, AstStatement, AstType, AstVariantDef, InfixOperator},
		identifier::{
			MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier,
			MaybeQualifiedIndexIdentifier, MaybeQualifiedProcedureIdentifier,
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSequenceIdentifier,
			MaybeQualifiedSeriesIdentifier, MaybeQualifiedSumTypeIdentifier, MaybeQualifiedTableIdentifier,
			MaybeQualifiedTransactionalViewIdentifier, MaybeQualifiedViewIdentifier,
		},
	},
	bump::{Bump, BumpBox, BumpFragment, BumpVec},
	diagnostic::AstError,
	expression::{AliasExpression, Expression, ExpressionCompiler, IdentExpression},
	plan::logical::alter::{flow::AlterFlowNode, table::AlterTableNode},
};

pub(crate) struct Compiler<'bump> {
	pub catalog: Catalog,
	pub bump: &'bump Bump,
}

/// Compile AST to logical plan using any transaction type that implements IntoTransaction
#[instrument(name = "rql::compile::logical", level = "trace", skip(bump, catalog, tx, ast))]
pub fn compile_logical<'b>(
	bump: &'b Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
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
	compile_logical(bump, catalog, &mut Transaction::Query(tx), ast)
}

#[instrument(name = "rql::compile::logical_command", level = "trace", skip(bump, catalog, tx, ast))]
pub fn compile_logical_command<'b>(
	bump: &'b Bump,
	catalog: &Catalog,
	tx: &mut CommandTransaction,
	ast: AstStatement<'b>,
) -> crate::Result<BumpVec<'b, LogicalPlan<'b>>> {
	compile_logical(bump, catalog, &mut Transaction::Command(tx), ast)
}

impl<'bump> Compiler<'bump> {
	pub fn compile(
		&self,
		ast: AstStatement<'bump>,
		tx: &mut Transaction<'_>,
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
	pub fn compile_single(&self, node: Ast<'bump>, tx: &mut Transaction<'_>) -> crate::Result<LogicalPlan<'bump>> {
		match node {
			Ast::Create(node) => self.compile_create(node, tx),
			Ast::Drop(node) => self.compile_drop(node),
			Ast::Alter(node) => self.compile_alter(node, tx),
			Ast::Delete(node) => self.compile_delete(node, tx),
			Ast::Insert(node) => self.compile_insert(node, tx),
			Ast::Update(node) => self.compile_update(node, tx),
			Ast::Append(node) => self.compile_append(node, tx),
			Ast::If(node) => self.compile_if(node, tx),
			Ast::Match(node) => self.compile_match(node, tx),
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
					// Variable calls ($f(args)) — route through CallFunction for VM execution
					InfixOperator::Call(_) if matches!(*infix_node.left, Ast::Variable(_)) => {
						let Ast::Variable(var) = BumpBox::into_inner(infix_node.left) else {
							unreachable!()
						};
						let right = BumpBox::into_inner(infix_node.right);
						let args_nodes = match right {
							Ast::Tuple(tuple) => tuple.nodes,
							other => vec![other],
						};
						let mut arguments = Vec::new();
						for arg in args_nodes {
							arguments.push(ExpressionCompiler::compile(arg)?);
						}
						Ok(LogicalPlan::CallFunction(function::CallFunctionNode {
							name: var.token.fragment,
							arguments,
							is_procedure_call: false,
						}))
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
					InfixOperator::AccessTable(_) | InfixOperator::AccessNamespace(_) => {
						self.compile_infix(infix_node)
					}
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
				return Err(AstError::UnsupportedAstNode {
					node_type: "standalone identifier".to_string(),
					fragment: id.token.fragment.to_owned(),
				}
				.into());
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
				return Err(AstError::UnsupportedAstNode {
					node_type: "standalone block".to_string(),
					fragment: node.token().fragment.to_owned(),
				}
				.into());
			}
			Ast::DefFunction(node) => self.compile_def_function(node, tx),
			Ast::Return(node) => self.compile_return(node),
			Ast::Closure(node) => self.compile_closure(node, tx),
			Ast::Call(call_node) => self.compile_call(call_node),
			Ast::Dispatch(node) => self.compile_dispatch(node, tx),
			Ast::Grant(node) => Ok(LogicalPlan::Grant(GrantNode {
				role: node.role,
				user: node.user,
			})),
			Ast::Revoke(node) => Ok(LogicalPlan::Revoke(RevokeNode {
				role: node.role,
				user: node.user,
			})),
			Ast::Identity(_) | Ast::Require(_) => {
				// Identity and Require are expression-level constructs, not standalone statements.
				// They appear inside policy bodies and pipe chains, not as top-level plan nodes.
				self.compile_scalar_as_map(node)
			}
			Ast::Migrate(node) => Ok(LogicalPlan::Migrate(MigrateNode {
				target: node.target,
			})),
			Ast::RollbackMigration(node) => Ok(LogicalPlan::RollbackMigration(RollbackMigrationNode {
				target: node.target,
			})),
			node => {
				let node_type =
					format!("{:?}", node).split('(').next().unwrap_or("Unknown").to_string();
				return Err(AstError::UnsupportedAstNode {
					node_type: node_type.to_string(),
					fragment: node.token().fragment.to_owned(),
				}
				.into());
			}
		}
	}

	// Helper to wrap a scalar expression in a MAP { "value": expression }
	// Instead of creating synthetic AST nodes (which would require a bump allocator),
	// this directly compiles the scalar and wraps the result in a MapNode.
	fn compile_scalar_as_map(&self, scalar_node: Ast<'bump>) -> crate::Result<LogicalPlan<'bump>> {
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
						return Err(AstError::UnsupportedAstNode {
							node_type: "assignment to non-variable".to_string(),
							fragment: node.token.fragment.to_owned(),
						}
						.into());
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
				return Err(AstError::UnsupportedAstNode {
					node_type: "infix operation as statement".to_string(),
					fragment: node.token.fragment.to_owned(),
				}
				.into());
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
	CreatePrimaryKey(CreatePrimaryKeyNode<'bump>),
	CreateColumnProperty(CreateColumnPropertyNode<'bump>),
	CreateProcedure(CreateProcedureNode<'bump>),
	CreateSeries(CreateSeriesNode<'bump>),
	CreateEvent(CreateEventNode<'bump>),
	CreateTag(CreateTagNode<'bump>),

	CreateMigration(CreateMigrationNode),
	Migrate(MigrateNode),
	RollbackMigration(RollbackMigrationNode),
	Dispatch(DispatchNode<'bump>),
	// Drop
	DropNamespace(DropNamespaceNode<'bump>),
	DropTable(DropTableNode<'bump>),
	DropView(DropViewNode<'bump>),
	DropRingBuffer(DropRingBufferNode<'bump>),
	DropDictionary(DropDictionaryNode<'bump>),
	DropSumType(DropSumTypeNode<'bump>),
	DropFlow(DropFlowNode<'bump>),
	DropSubscription(DropSubscriptionNode<'bump>),
	DropSeries(DropSeriesNode<'bump>),
	// Alter
	AlterSequence(AlterSequenceNode<'bump>),
	AlterFlow(AlterFlowNode<'bump>),
	AlterTable(AlterTableNode<'bump>),
	// Mutate
	DeleteTable(DeleteTableNode<'bump>),
	DeleteRingBuffer(DeleteRingBufferNode<'bump>),
	InsertTable(InsertTableNode<'bump>),
	InsertRingBuffer(InsertRingBufferNode<'bump>),
	InsertDictionary(InsertDictionaryNode<'bump>),
	InsertSeries(InsertSeriesNode<'bump>),
	DeleteSeries(DeleteSeriesNode<'bump>),
	Update(UpdateTableNode<'bump>),
	UpdateRingBuffer(UpdateRingBufferNode<'bump>),
	UpdateSeries(UpdateSeriesNode<'bump>),
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
	// Closures
	DefineClosure(DefineClosureNode<'bump>),
	// Auth/Permissions
	CreateUser(CreateUserNode<'bump>),
	CreateRole(CreateRoleNode<'bump>),
	Grant(GrantNode<'bump>),
	Revoke(RevokeNode<'bump>),
	DropUser(DropUserNode<'bump>),
	DropRole(DropRoleNode<'bump>),
	CreateAuthentication(CreateAuthenticationNode<'bump>),
	DropAuthentication(DropAuthenticationNode<'bump>),
	CreatePolicy(CreatePolicyNode<'bump>),
	AlterPolicy(AlterPolicyNode<'bump>),
	DropPolicy(DropPolicyNode<'bump>),
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
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'bump> {
	pub view: MaybeQualifiedTransactionalViewIdentifier<'bump>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: BumpVec<'bump, LogicalPlan<'bump>>,
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
}

#[derive(Debug)]
pub struct CreateRingBufferNode<'bump> {
	pub ringbuffer: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
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
	pub name: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub if_not_exists: bool,
	pub variants: Vec<AstVariantDef<'bump>>,
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
pub struct InsertSeriesNode<'bump> {
	pub target: MaybeQualifiedSeriesIdentifier<'bump>,
	pub source: BumpBox<'bump, LogicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct DeleteSeriesNode<'bump> {
	pub target: MaybeQualifiedSeriesIdentifier<'bump>,
	pub input: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
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
pub struct UpdateSeriesNode<'bump> {
	pub target: MaybeQualifiedSeriesIdentifier<'bump>,
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

#[derive(Debug)]
pub struct DefineClosureNode<'bump> {
	pub parameters: Vec<function::FunctionParameter<'bump>>,
	pub body: Vec<BumpVec<'bump, LogicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct CreatePrimaryKeyNode<'bump> {
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub columns: Vec<PrimaryKeyColumn<'bump>>,
}

#[derive(Debug)]
pub struct CreateColumnPropertyNode<'bump> {
	pub column: MaybeQualifiedColumnIdentifier<'bump>,
	pub properties: Vec<ColumnPropertyKind>,
}

#[derive(Debug)]
pub struct CreateProcedureNode<'bump> {
	pub procedure: MaybeQualifiedProcedureIdentifier<'bump>,
	pub params: Vec<AstProcedureParam<'bump>>,
	pub body_source: String,
	/// Set when this procedure is created via CREATE HANDLER (event binding)
	pub on_event: Option<crate::ast::identifier::MaybeQualifiedSumTypeIdentifier<'bump>>,
	/// Variant name for event-triggered procedures
	pub on_variant: Option<BumpFragment<'bump>>,
}

// === Drop nodes ===

#[derive(Debug)]
pub struct DropNamespaceNode<'bump> {
	pub segments: Vec<BumpFragment<'bump>>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropTableNode<'bump> {
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropViewNode<'bump> {
	pub view: MaybeQualifiedViewIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropRingBufferNode<'bump> {
	pub ringbuffer: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropDictionaryNode<'bump> {
	pub dictionary: MaybeQualifiedDictionaryIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropSumTypeNode<'bump> {
	pub sumtype: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropFlowNode<'bump> {
	pub flow: MaybeQualifiedFlowIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct DropSubscriptionNode<'bump> {
	pub identifier: BumpFragment<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

// === Auth/Permissions logical plan nodes ===

#[derive(Debug)]
pub struct CreateUserNode<'bump> {
	pub name: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct CreateRoleNode<'bump> {
	pub name: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct GrantNode<'bump> {
	pub role: BumpFragment<'bump>,
	pub user: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct RevokeNode<'bump> {
	pub role: BumpFragment<'bump>,
	pub user: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct DropUserNode<'bump> {
	pub name: BumpFragment<'bump>,
	pub if_exists: bool,
}

#[derive(Debug)]
pub struct DropRoleNode<'bump> {
	pub name: BumpFragment<'bump>,
	pub if_exists: bool,
}

#[derive(Debug)]
pub struct CreateAuthenticationNode<'bump> {
	pub user: BumpFragment<'bump>,
	pub entries: Vec<crate::ast::ast::AstAuthenticationEntry<'bump>>,
}

#[derive(Debug)]
pub struct DropAuthenticationNode<'bump> {
	pub user: BumpFragment<'bump>,
	pub if_exists: bool,
	pub method: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct CreatePolicyNode<'bump> {
	pub name: Option<BumpFragment<'bump>>,
	pub target_type: crate::ast::ast::AstPolicyTargetType,
	pub scope: crate::ast::ast::AstPolicyScope<'bump>,
	pub operations: Vec<crate::ast::ast::AstPolicyOperationEntry<'bump>>,
}

#[derive(Debug)]
pub struct AlterPolicyNode<'bump> {
	pub target_type: crate::ast::ast::AstPolicyTargetType,
	pub name: BumpFragment<'bump>,
	pub action: crate::ast::ast::AstAlterPolicyAction,
}

#[derive(Debug)]
pub struct DropPolicyNode<'bump> {
	pub target_type: crate::ast::ast::AstPolicyTargetType,
	pub name: BumpFragment<'bump>,
	pub if_exists: bool,
}

#[derive(Debug)]
pub struct DropSeriesNode<'bump> {
	pub series: MaybeQualifiedSeriesIdentifier<'bump>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug)]
pub struct CreateSeriesNode<'bump> {
	pub series: MaybeQualifiedSeriesIdentifier<'bump>,
	pub columns: Vec<SeriesColumnToCreate>,
	pub tag: Option<MaybeQualifiedSumTypeIdentifier<'bump>>,
	pub precision: TimestampPrecision,
}

#[derive(Debug)]
pub struct CreateEventNode<'bump> {
	pub name: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variants: Vec<AstVariantDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateTagNode<'bump> {
	pub name: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variants: Vec<AstVariantDef<'bump>>,
}

#[derive(Debug)]
pub struct CreateMigrationNode {
	pub name: String,
	pub body_source: String,
	pub rollback_body_source: Option<String>,
}

#[derive(Debug)]
pub struct MigrateNode {
	pub target: Option<String>,
}

#[derive(Debug)]
pub struct RollbackMigrationNode {
	pub target: Option<String>,
}

#[derive(Debug)]
pub struct DispatchNode<'bump> {
	pub on_event: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variant: BumpFragment<'bump>,
	pub fields: Vec<(BumpFragment<'bump>, Expression)>,
}
