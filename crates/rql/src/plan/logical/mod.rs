// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod alter;
mod create;
mod mutate;
pub mod query;
pub mod resolver;
pub mod row_predicate;
mod variable;

use async_recursion::async_recursion;
use query::window::WindowNode;
use reifydb_catalog::{
	CatalogQueryTransaction,
	store::{ringbuffer::create::RingBufferColumnToCreate, table::TableColumnToCreate, view::ViewColumnToCreate},
};
use reifydb_core::{
	IndexType, JoinType, SortDirection, SortKey,
	interface::{
		ColumnPolicyKind, ColumnSaturationPolicy,
		resolved::{ResolvedColumn, ResolvedIndex, ResolvedSource},
	},
	return_error,
};
use reifydb_type::{Fragment, diagnostic::ast::unsupported_ast_node};
use tracing::instrument;

use crate::{
	ast::{
		Ast, AstDataType, AstInfix, AstLiteral, AstLiteralText, AstMap, AstPolicy, AstPolicyKind, AstStatement,
		InfixOperator, Token, TokenKind,
		identifier::{
			MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
			MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier,
			MaybeQualifiedIndexIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedSequenceIdentifier, MaybeQualifiedTableIdentifier,
			MaybeQualifiedTransactionalViewIdentifier,
		},
		tokenize::{Keyword, Literal, Operator},
	},
	expression::{AliasExpression, Expression},
	plan::logical::alter::{AlterFlowNode, AlterTableNode, AlterViewNode},
};

struct Compiler {}

#[instrument(name = "rql::compile::logical", level = "trace", skip(tx, ast))]
pub async fn compile_logical<T: CatalogQueryTransaction>(
	tx: &mut T,
	ast: AstStatement,
) -> crate::Result<Vec<LogicalPlan>> {
	Compiler::compile(ast, tx).await
}

impl Compiler {
	#[async_recursion]
	async fn compile<T: CatalogQueryTransaction + Send>(
		ast: AstStatement,
		tx: &mut T,
	) -> crate::Result<Vec<LogicalPlan>> {
		if ast.is_empty() {
			return Ok(vec![]);
		}

		let ast_len = ast.len();
		let has_pipes = ast.has_pipes;
		let ast_vec = ast.nodes; // Extract the inner Vec

		// Check if this is a pipeline ending with UPDATE or DELETE
		let is_update_pipeline = ast_len > 1 && matches!(ast_vec.last(), Some(Ast::Update(_)));
		let is_delete_pipeline = ast_len > 1 && matches!(ast_vec.last(), Some(Ast::Delete(_)));

		if is_update_pipeline || is_delete_pipeline {
			// Build pipeline: compile all nodes except the last one
			// into a pipeline
			let mut pipeline_nodes = Vec::new();

			for (i, node) in ast_vec.into_iter().enumerate() {
				if i == ast_len - 1 {
					// Last operator is UPDATE or DELETE
					match node {
						Ast::Update(update_ast) => {
							// Build the pipeline as
							// input to update
							let input = if !pipeline_nodes.is_empty() {
								Some(Box::new(Self::build_pipeline(pipeline_nodes)?))
							} else {
								None
							};

							// If target is None, we can't determine table vs ring buffer
							let Some(unresolved) = &update_ast.target else {
								return Ok(vec![LogicalPlan::Update(
									UpdateTableNode {
										target: None,
										input,
									},
								)]);
							};

							// Check if target is a table or ring buffer
							use crate::ast::identifier::{
								MaybeQualifiedRingBufferIdentifier,
								MaybeQualifiedTableIdentifier,
							};

							// Check in the catalog whether the target is a table or ring
							// buffer
							let namespace_name = unresolved
								.namespace
								.as_ref()
								.map(|n| n.text())
								.unwrap_or("default");
							let target_name = unresolved.name.text();

							// Try to find namespace
							if let Some(ns) =
								tx.find_namespace_by_name(namespace_name).await?
							{
								let namespace_id = ns.id;

								// Check if it's a ring buffer first
								if tx.find_ringbuffer_by_name(namespace_id, target_name)
									.await?
									.is_some()
								{
									let mut target =
										MaybeQualifiedRingBufferIdentifier::new(
											unresolved.name.clone(),
										);
									if let Some(ns) = unresolved.namespace.clone() {
										target = target.with_namespace(ns);
									}
									return Ok(vec![
										LogicalPlan::UpdateRingBuffer(
											UpdateRingBufferNode {
												target,
												input,
											},
										),
									]);
								}
							}

							// Default to table update
							let mut target = MaybeQualifiedTableIdentifier::new(
								unresolved.name.clone(),
							);
							if let Some(ns) = unresolved.namespace.clone() {
								target = target.with_namespace(ns);
							}
							return Ok(vec![LogicalPlan::Update(UpdateTableNode {
								target: Some(target),
								input,
							})]);
						}
						Ast::Delete(delete_ast) => {
							// Build the pipeline as
							// input to delete
							let input = if !pipeline_nodes.is_empty() {
								Some(Box::new(Self::build_pipeline(pipeline_nodes)?))
							} else {
								None
							};

							// Check if target is a table or ring buffer
							if let Some(unresolved) = &delete_ast.target {
								use crate::ast::identifier::{
									MaybeQualifiedRingBufferIdentifier,
									MaybeQualifiedTableIdentifier,
								};

								// Check in the catalog whether the target is a table or
								// ring buffer
								let namespace_name = unresolved
									.namespace
									.as_ref()
									.map(|n| n.text())
									.unwrap_or("default");
								let target_name = unresolved.name.text();

								// Try to find namespace
								if let Some(ns) = tx
									.find_namespace_by_name(namespace_name)
									.await?
								{
									let namespace_id = ns.id;

									// Check if it's a ring buffer first
									if tx.find_ringbuffer_by_name(
										namespace_id,
										target_name,
									)
									.await?
									.is_some()
									{
										let mut target = MaybeQualifiedRingBufferIdentifier::new(unresolved.name.clone());
										if let Some(ns) =
											unresolved.namespace.clone()
										{
											target = target
												.with_namespace(ns);
										}
										return Ok(vec![
											LogicalPlan::DeleteRingBuffer(
												DeleteRingBufferNode {
													target,
													input,
												},
											),
										]);
									}
								}

								// Default to table delete
								let mut target = MaybeQualifiedTableIdentifier::new(
									unresolved.name.clone(),
								);
								if let Some(ns) = unresolved.namespace.clone() {
									target = target.with_namespace(ns);
								}
								return Ok(vec![LogicalPlan::DeleteTable(
									DeleteTableNode {
										target: Some(target),
										input,
									},
								)]);
							} else {
								// No target specified - use DeleteTable with None
								return Ok(vec![LogicalPlan::DeleteTable(
									DeleteTableNode {
										target: None,
										input,
									},
								)]);
							}
						}
						_ => unreachable!(),
					}
				} else {
					// Add to pipeline
					pipeline_nodes.push(Compiler::compile_single(node, tx).await?);
				}
			}
			unreachable!("Pipeline should have been handled above");
		}

		// Check if this is a piped query that should be wrapped in
		// Pipeline
		if has_pipes && ast_len > 1 {
			// This uses pipe operators - create a Pipeline operator
			let mut pipeline_nodes = Vec::new();
			for node in ast_vec {
				pipeline_nodes.push(Self::compile_single(node, tx).await?);
			}
			return Ok(vec![LogicalPlan::Pipeline(PipelineNode {
				steps: pipeline_nodes,
			})]);
		}

		// Normal compilation (not piped)
		let mut result = Vec::with_capacity(ast_len);
		for node in ast_vec {
			result.push(Self::compile_single(node, tx).await?);
		}
		Ok(result)
	}

	// Helper to compile a single AST operator
	#[async_recursion]
	async fn compile_single<T: CatalogQueryTransaction + Send>(
		node: Ast,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		match node {
			Ast::Create(node) => Self::compile_create(node, tx).await,
			Ast::Alter(node) => Self::compile_alter(node, tx).await,
			Ast::Delete(node) => Self::compile_delete(node, tx).await,
			Ast::Insert(node) => Self::compile_insert(node, tx).await,
			Ast::Update(node) => Self::compile_update(node, tx).await,
			Ast::If(node) => Self::compile_if(node, tx).await,
			Ast::Let(node) => Self::compile_let(node, tx).await,
			Ast::StatementExpression(node) => {
				// Compile the inner expression and wrap it in a MAP
				let map_node = Self::wrap_scalar_in_map(*node.expression.clone());
				Self::compile_map(map_node, tx)
			}
			Ast::Prefix(node) => {
				// Prefix operations as statements - wrap in MAP
				let map_node = Self::wrap_scalar_in_map(Ast::Prefix(node));
				Self::compile_map(map_node, tx)
			}
			Ast::Infix(ref infix_node) => {
				match infix_node.operator {
					// Assignment operations - check if it's a valid variable assignment
					InfixOperator::Assign(ref token) => {
						// Only allow variable assignments with := operator, not = operator
						if matches!(token.kind, TokenKind::Operator(Operator::ColonEqual)) {
							// This is a valid variable assignment statement
							Self::compile_infix(infix_node.clone(), tx)
						} else {
							// This is a = operator, treat as expression comparison
							let map_node = Self::wrap_scalar_in_map(node);
							Self::compile_map(map_node, tx)
						}
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
					| InfixOperator::NotIn(_) => {
						let wrapped_map = Self::wrap_scalar_in_map(node);
						Self::compile_map(wrapped_map, tx)
					}

					// Statement-like operations - compile directly
					InfixOperator::Arrow(_)
					| InfixOperator::AccessTable(_)
					| InfixOperator::AccessNamespace(_) => Self::compile_infix(infix_node.clone(), tx),
				}
			}
			Ast::Aggregate(node) => Self::compile_aggregate(node, tx),
			Ast::Filter(node) => Self::compile_filter(node, tx),
			Ast::From(node) => Self::compile_from(node, tx).await,
			Ast::Join(node) => Self::compile_join(node, tx).await,
			Ast::Merge(node) => Self::compile_merge(node, tx).await,
			Ast::Take(node) => Self::compile_take(node, tx),
			Ast::Sort(node) => Self::compile_sort(node, tx),
			Ast::Distinct(node) => Self::compile_distinct(node, tx),
			Ast::Map(node) => Self::compile_map(node, tx),
			Ast::Extend(node) => Self::compile_extend(node, tx),
			Ast::Apply(node) => Self::compile_apply(node),
			Ast::Window(node) => Self::compile_window(node, tx),
			Ast::Identifier(ref id) => {
				return_error!(unsupported_ast_node(id.token.fragment.clone(), "standalone identifier"))
			}
			// Auto-wrap scalar expressions into MAP constructs
			Ast::Literal(_) | Ast::Variable(_) | Ast::CallFunction(_) => {
				let wrapped_map = Self::wrap_scalar_in_map(node);
				Self::compile_map(wrapped_map, tx)
			}
			node => {
				let node_type =
					format!("{:?}", node).split('(').next().unwrap_or("Unknown").to_string();
				return_error!(unsupported_ast_node(node.token().fragment.clone(), &node_type))
			}
		}
	}

	// Helper to wrap scalar expressions in MAP { "value": expression }
	fn wrap_scalar_in_map(scalar_node: Ast) -> crate::ast::AstMap {
		let scalar_fragment = scalar_node.token().fragment.clone();

		// Create synthetic tokens for the MAP structure
		let map_token = Token {
			kind: TokenKind::Keyword(Keyword::Map),
			fragment: scalar_fragment.clone(),
		};

		let key_token = Token {
			kind: TokenKind::Literal(Literal::Text),
			fragment: Fragment::internal("value"),
		};

		let colon_token = Token {
			kind: TokenKind::Operator(Operator::Colon),
			fragment: scalar_fragment.clone(),
		};

		// Create the key-value pair: "value": scalar_node
		let key_literal = Ast::Literal(AstLiteral::Text(AstLiteralText(key_token.clone())));
		let key_value_pair = Ast::Infix(AstInfix {
			token: key_token,
			left: Box::new(key_literal),
			operator: InfixOperator::TypeAscription(colon_token),
			right: Box::new(scalar_node),
		});

		AstMap {
			token: map_token,
			nodes: vec![key_value_pair],
		}
	}

	fn compile_infix<T: CatalogQueryTransaction>(node: AstInfix, _tx: &mut T) -> crate::Result<LogicalPlan> {
		match node.operator {
			InfixOperator::Assign(token) => {
				// Only allow variable assignments with := operator, not = operator
				if !matches!(token.kind, TokenKind::Operator(Operator::ColonEqual)) {
					return_error!(unsupported_ast_node(
						node.token.fragment,
						"variable assignment must use := operator"
					))
				}

				// This is a variable assignment statement
				// Extract the variable name from the left side
				let variable = match *node.left {
					crate::ast::Ast::Variable(var) => var,
					_ => {
						return_error!(unsupported_ast_node(
							node.token.fragment,
							"assignment to non-variable"
						))
					}
				};

				// Convert the right side to an expression
				let expr = crate::expression::ExpressionCompiler::compile(*node.right)?;
				let value = AssignValue::Expression(expr);

				// Extract variable name (remove $ prefix if present)
				let name_text = variable.token.fragment.text();
				let clean_name = if name_text.starts_with('$') {
					&name_text[1..]
				} else {
					name_text
				};

				Ok(LogicalPlan::Assign(AssignNode {
					name: Fragment::internal(clean_name),
					value,
				}))
			}
			_ => {
				// Other infix operations are not supported as standalone statements
				return_error!(unsupported_ast_node(node.token.fragment, "infix operation as statement"))
			}
		}
	}

	fn build_pipeline(plans: Vec<LogicalPlan>) -> crate::Result<LogicalPlan> {
		// The pipeline should be properly structured with inputs
		// For now, we'll wrap them in a special Pipeline plan
		// that the physical compiler can handle
		if plans.is_empty() {
			panic!("Empty pipeline");
		}

		// Return a Pipeline logical plan that contains all the steps
		Ok(LogicalPlan::Pipeline(PipelineNode {
			steps: plans,
		}))
	}
}

#[derive(Debug)]
pub enum LogicalPlan {
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateNamespace(CreateNamespaceNode),
	CreateSequence(CreateSequenceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateDictionary(CreateDictionaryNode),
	CreateFlow(CreateFlowNode),
	CreateIndex(CreateIndexNode),
	// Alter
	AlterSequence(AlterSequenceNode),
	AlterTable(AlterTableNode),
	AlterView(AlterViewNode),
	AlterFlow(AlterFlowNode),
	// Mutate
	DeleteTable(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),
	// Variable assignment
	Declare(DeclareNode),
	Assign(AssignNode),
	// Control flow
	Conditional(ConditionalNode),
	// Query
	Aggregate(AggregateNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Merge(MergeNode),
	Take(TakeNode),
	Order(OrderNode),
	Map(MapNode),
	Extend(ExtendNode),
	Apply(ApplyNode),
	InlineData(InlineDataNode),
	SourceScan(SourceScanNode),
	Window(WindowNode),
	Generator(GeneratorNode),
	VariableSource(VariableSourceNode),
	Environment(EnvironmentNode),
	// Auto-scalarization for 1x1 frames in scalar contexts
	Scalarize(ScalarizeNode),
	// Pipeline wrapper for piped operations
	Pipeline(PipelineNode),
}

#[derive(Debug)]
pub struct PipelineNode {
	pub steps: Vec<LogicalPlan>,
}

#[derive(Debug)]
pub struct ScalarizeNode {
	pub input: Box<LogicalPlan>,
	pub fragment: Fragment,
}

#[derive(Debug)]
pub enum LetValue {
	Expression(Expression),      // scalar/column expression
	Statement(Vec<LogicalPlan>), // query pipeline as logical plans
}

impl std::fmt::Display for LetValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			LetValue::Expression(expr) => write!(f, "{}", expr),
			LetValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug)]
pub enum AssignValue {
	Expression(Expression),      // scalar/column expression
	Statement(Vec<LogicalPlan>), // query pipeline as logical plans
}

impl std::fmt::Display for AssignValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AssignValue::Expression(expr) => write!(f, "{}", expr),
			AssignValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug)]
pub struct DeclareNode {
	pub name: Fragment,
	pub value: LetValue,
	pub mutable: bool,
}

#[derive(Debug)]
pub struct AssignNode {
	pub name: Fragment,
	pub value: AssignValue,
}

#[derive(Debug)]
pub struct ConditionalNode {
	pub condition: Expression,
	pub then_branch: Box<LogicalPlan>,
	pub else_ifs: Vec<ElseIfBranch>,
	pub else_branch: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct ElseIfBranch {
	pub condition: Expression,
	pub then_branch: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyDef {
	pub columns: Vec<PrimaryKeyColumn>,
}

#[derive(Debug, Clone)]
pub struct PrimaryKeyColumn {
	pub column: Fragment,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub struct CreateDeferredViewNode {
	pub view: MaybeQualifiedDeferredViewIdentifier,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Vec<LogicalPlan>,
	pub primary_key: Option<PrimaryKeyDef>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode {
	pub view: MaybeQualifiedTransactionalViewIdentifier,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Vec<LogicalPlan>,
	pub primary_key: Option<PrimaryKeyDef>,
}

#[derive(Debug)]
pub struct CreateNamespaceNode {
	pub namespace: Fragment,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode {
	pub sequence: MaybeQualifiedSequenceIdentifier,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode {
	pub table: MaybeQualifiedTableIdentifier,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
	pub primary_key: Option<PrimaryKeyDef>,
}

#[derive(Debug)]
pub struct CreateRingBufferNode {
	pub ringbuffer: MaybeQualifiedRingBufferIdentifier,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
	pub primary_key: Option<PrimaryKeyDef>,
}

#[derive(Debug)]
pub struct CreateDictionaryNode {
	pub dictionary: MaybeQualifiedDictionaryIdentifier,
	pub if_not_exists: bool,
	pub value_type: AstDataType,
	pub id_type: AstDataType,
}

#[derive(Debug)]
pub struct CreateFlowNode {
	pub flow: MaybeQualifiedFlowIdentifier,
	pub if_not_exists: bool,
	pub as_clause: Vec<LogicalPlan>,
}

#[derive(Debug)]
pub struct AlterSequenceNode {
	pub sequence: MaybeQualifiedSequenceIdentifier,
	pub column: MaybeQualifiedColumnIdentifier,
	pub value: Expression,
}

#[derive(Debug)]
pub struct CreateIndexNode {
	pub index_type: IndexType,
	pub index: MaybeQualifiedIndexIdentifier,
	pub columns: Vec<IndexColumn>,
	pub filter: Vec<Expression>,
	pub map: Option<Expression>,
}

#[derive(Debug)]
pub struct IndexColumn {
	pub column: Fragment,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub struct DeleteTableNode {
	pub target: Option<MaybeQualifiedTableIdentifier>,
	pub input: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct DeleteRingBufferNode {
	pub target: MaybeQualifiedRingBufferIdentifier,
	pub input: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct InsertTableNode {
	pub target: MaybeQualifiedTableIdentifier,
}

#[derive(Debug)]
pub struct InsertRingBufferNode {
	pub target: MaybeQualifiedRingBufferIdentifier,
}

#[derive(Debug)]
pub struct InsertDictionaryNode {
	pub target: MaybeQualifiedDictionaryIdentifier,
}

#[derive(Debug)]
pub struct UpdateTableNode {
	pub target: Option<MaybeQualifiedTableIdentifier>,
	pub input: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct UpdateRingBufferNode {
	pub target: MaybeQualifiedRingBufferIdentifier,
	pub input: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct AggregateNode {
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

#[derive(Debug)]
pub struct DistinctNode {
	pub columns: Vec<MaybeQualifiedColumnIdentifier>,
}

#[derive(Debug)]
pub struct FilterNode {
	pub condition: Expression,
}

#[derive(Debug)]
pub struct JoinInnerNode {
	pub with: Vec<LogicalPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug)]
pub struct JoinLeftNode {
	pub with: Vec<LogicalPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug)]
pub struct JoinNaturalNode {
	pub with: Vec<LogicalPlan>,
	pub join_type: JoinType,
	pub alias: Option<Fragment>,
}

#[derive(Debug)]
pub struct MergeNode {
	pub with: Vec<LogicalPlan>,
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
pub struct ApplyNode {
	pub operator: Fragment,
	pub arguments: Vec<Expression>,
}

#[derive(Debug)]
pub struct InlineDataNode {
	pub rows: Vec<Vec<AliasExpression>>,
}

#[derive(Debug)]
pub struct SourceScanNode {
	pub source: ResolvedSource,
	pub columns: Option<Vec<ResolvedColumn>>,
	pub index: Option<ResolvedIndex>,
}

#[derive(Debug)]
pub struct GeneratorNode {
	pub name: Fragment,
	pub expressions: Vec<Expression>,
}

#[derive(Debug)]
pub struct VariableSourceNode {
	pub name: Fragment,
}

#[derive(Debug)]
pub struct EnvironmentNode {}

pub(crate) fn convert_policy(ast: &AstPolicy) -> ColumnPolicyKind {
	use ColumnPolicyKind::*;

	match ast.policy {
		AstPolicyKind::Saturation => {
			if ast.value.is_literal_undefined() {
				return Saturation(ColumnSaturationPolicy::Undefined);
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
		AstPolicyKind::NotUndefined => unimplemented!(),
	}
}
