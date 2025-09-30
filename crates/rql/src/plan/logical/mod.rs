// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod alter;
mod create;
mod mutate;
mod query;
pub mod resolver;

use reifydb_catalog::{
	CatalogQueryTransaction, ring_buffer::create::RingBufferColumnToCreate, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	IndexType, JoinType, SortDirection, SortKey,
	interface::{
		ColumnPolicyKind, ColumnSaturationPolicy,
		expression::{AliasExpression, Expression},
		identifier,
		resolved::{ResolvedColumn, ResolvedIndex, ResolvedSource},
	},
	return_error,
};
use reifydb_type::{Fragment, diagnostic::ast::unsupported_ast_node};

use crate::{
	ast::{
		Ast, AstPolicy, AstPolicyKind, AstStatement,
		identifier::{
			MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
			MaybeQualifiedIndexIdentifier, MaybeQualifiedRingBufferIdentifier,
			MaybeQualifiedSequenceIdentifier, MaybeQualifiedTableIdentifier,
			MaybeQualifiedTransactionalViewIdentifier,
		},
	},
	plan::logical::alter::{AlterTableNode, AlterViewNode},
};

struct Compiler {}

pub fn compile_logical<'a, 't, T: CatalogQueryTransaction>(
	tx: &'t mut T,
	ast: AstStatement<'a>,
) -> crate::Result<Vec<LogicalPlan<'a>>> {
	Compiler::compile(ast, tx)
}

impl Compiler {
	fn compile<'a, 't, T: CatalogQueryTransaction>(
		ast: AstStatement<'a>,
		tx: &mut T,
	) -> crate::Result<Vec<LogicalPlan<'a>>> {
		if ast.is_empty() {
			return Ok(vec![]);
		}

		let ast_len = ast.len();
		let has_pipes = ast.has_pipes;
		let ast_vec = ast.nodes; // Extract the inner Vec

		// Check if this is a pipeline ending with UPDATE or DELETE
		let is_update_pipeline = ast_len > 1 && matches!(ast_vec.last(), Some(Ast::AstUpdate(_)));
		let is_delete_pipeline = ast_len > 1 && matches!(ast_vec.last(), Some(Ast::AstDelete(_)));

		if is_update_pipeline || is_delete_pipeline {
			// Build pipeline: compile all nodes except the last one
			// into a pipeline
			let mut pipeline_nodes = Vec::new();

			for (i, node) in ast_vec.into_iter().enumerate() {
				if i == ast_len - 1 {
					// Last node is UPDATE or DELETE
					match node {
						Ast::AstUpdate(update_ast) => {
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
							if let Some(ns) = tx.find_namespace_by_name(namespace_name)? {
								let namespace_id = ns.id;

								// Check if it's a ring buffer first
								if tx.find_ring_buffer_by_name(
									namespace_id,
									target_name,
								)?
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
						Ast::AstDelete(delete_ast) => {
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
								if let Some(ns) =
									tx.find_namespace_by_name(namespace_name)?
								{
									let namespace_id = ns.id;

									// Check if it's a ring buffer first
									if tx.find_ring_buffer_by_name(
										namespace_id,
										target_name,
									)?
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
					pipeline_nodes.push(Compiler::compile_single(node, tx)?);
				}
			}
			unreachable!("Pipeline should have been handled above");
		}

		// Check if this is a piped query that should be wrapped in
		// Pipeline
		if has_pipes && ast_len > 1 {
			// This uses pipe operators - create a Pipeline node
			let mut pipeline_nodes = Vec::new();
			for node in ast_vec {
				pipeline_nodes.push(Self::compile_single(node, tx)?);
			}
			return Ok(vec![LogicalPlan::Pipeline(PipelineNode {
				steps: pipeline_nodes,
			})]);
		}

		// Normal compilation (not piped)
		let mut result = Vec::with_capacity(ast_len);
		for node in ast_vec {
			result.push(Self::compile_single(node, tx)?);
		}
		Ok(result)
	}

	// Helper to compile a single AST node
	fn compile_single<'a, 't, T: CatalogQueryTransaction>(
		node: Ast<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		match node {
			Ast::Create(node) => Self::compile_create(node, tx),
			Ast::Alter(node) => Self::compile_alter(node, tx),
			Ast::AstDelete(node) => Self::compile_delete(node, tx),
			Ast::AstInsert(node) => Self::compile_insert(node, tx),
			Ast::AstUpdate(node) => Self::compile_update(node, tx),
			Ast::Aggregate(node) => Self::compile_aggregate(node, tx),
			Ast::Filter(node) => Self::compile_filter(node, tx),
			Ast::From(node) => Self::compile_from(node, tx),
			Ast::Join(node) => Self::compile_join(node, tx),
			Ast::Take(node) => Self::compile_take(node, tx),
			Ast::Sort(node) => Self::compile_sort(node, tx),
			Ast::Distinct(node) => Self::compile_distinct(node, tx),
			Ast::Map(node) => Self::compile_map(node, tx),
			Ast::Extend(node) => Self::compile_extend(node, tx),
			Ast::Apply(node) => Self::compile_apply(node),
			Ast::Identifier(ref id) => {
				return_error!(unsupported_ast_node(id.clone(), "standalone identifier"))
			}
			node => {
				let node_type =
					format!("{:?}", node).split('(').next().unwrap_or("Unknown").to_string();
				return_error!(unsupported_ast_node(node.token().fragment.clone(), &node_type))
			}
		}
	}

	fn build_pipeline<'a>(plans: Vec<LogicalPlan<'a>>) -> crate::Result<LogicalPlan<'a>> {
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
pub enum LogicalPlan<'a> {
	CreateDeferredView(CreateDeferredViewNode<'a>),
	CreateTransactionalView(CreateTransactionalViewNode<'a>),
	CreateNamespace(CreateNamespaceNode<'a>),
	CreateSequence(CreateSequenceNode<'a>),
	CreateTable(CreateTableNode<'a>),
	CreateRingBuffer(CreateRingBufferNode<'a>),
	CreateIndex(CreateIndexNode<'a>),
	// Alter
	AlterSequence(AlterSequenceNode<'a>),
	AlterTable(AlterTableNode<'a>),
	AlterView(AlterViewNode<'a>),
	// Mutate
	DeleteTable(DeleteTableNode<'a>),
	DeleteRingBuffer(DeleteRingBufferNode<'a>),
	InsertTable(InsertTableNode<'a>),
	InsertRingBuffer(InsertRingBufferNode<'a>),
	Update(UpdateTableNode<'a>),
	UpdateRingBuffer(UpdateRingBufferNode<'a>),
	// Query
	Aggregate(AggregateNode<'a>),
	Distinct(DistinctNode<'a>),
	Filter(FilterNode<'a>),
	JoinInner(JoinInnerNode<'a>),
	JoinLeft(JoinLeftNode<'a>),
	JoinNatural(JoinNaturalNode<'a>),
	Take(TakeNode),
	Order(OrderNode),
	Map(MapNode<'a>),
	Extend(ExtendNode<'a>),
	Apply(ApplyNode<'a>),
	InlineData(InlineDataNode<'a>),
	SourceScan(SourceScanNode<'a>),
	// Pipeline wrapper for piped operations
	Pipeline(PipelineNode<'a>),
}

#[derive(Debug)]
pub struct PipelineNode<'a> {
	pub steps: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateDeferredViewNode<'a> {
	pub view: MaybeQualifiedDeferredViewIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'a> {
	pub view: MaybeQualifiedTransactionalViewIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateNamespaceNode<'a> {
	pub namespace: Fragment<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode<'a> {
	pub sequence: MaybeQualifiedSequenceIdentifier<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode<'a> {
	pub table: MaybeQualifiedTableIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug)]
pub struct CreateRingBufferNode<'a> {
	pub ring_buffer: MaybeQualifiedRingBufferIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

#[derive(Debug)]
pub struct AlterSequenceNode<'a> {
	pub sequence: MaybeQualifiedSequenceIdentifier<'a>,
	pub column: MaybeQualifiedColumnIdentifier<'a>,
	pub value: Expression<'a>,
}

#[derive(Debug)]
pub struct CreateIndexNode<'a> {
	pub index_type: IndexType,
	pub index: MaybeQualifiedIndexIdentifier<'a>,
	pub columns: Vec<IndexColumn<'a>>,
	pub filter: Vec<Expression<'a>>,
	pub map: Option<Expression<'a>>,
}

#[derive(Debug)]
pub struct IndexColumn<'a> {
	pub column: Fragment<'a>,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub struct DeleteTableNode<'a> {
	pub target: Option<MaybeQualifiedTableIdentifier<'a>>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct DeleteRingBufferNode<'a> {
	pub target: MaybeQualifiedRingBufferIdentifier<'a>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct InsertTableNode<'a> {
	pub target: MaybeQualifiedTableIdentifier<'a>,
}

#[derive(Debug)]
pub struct InsertRingBufferNode<'a> {
	pub target: MaybeQualifiedRingBufferIdentifier<'a>,
}

#[derive(Debug)]
pub struct UpdateTableNode<'a> {
	pub target: Option<MaybeQualifiedTableIdentifier<'a>>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct UpdateRingBufferNode<'a> {
	pub target: MaybeQualifiedRingBufferIdentifier<'a>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct AggregateNode<'a> {
	pub by: Vec<Expression<'a>>,
	pub map: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct DistinctNode<'a> {
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'a>>,
}

#[derive(Debug)]
pub struct FilterNode<'a> {
	pub condition: Expression<'a>,
}

#[derive(Debug)]
pub struct JoinInnerNode<'a> {
	pub with: Vec<LogicalPlan<'a>>,
	pub on: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct JoinLeftNode<'a> {
	pub with: Vec<LogicalPlan<'a>>,
	pub on: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct JoinNaturalNode<'a> {
	pub with: Vec<LogicalPlan<'a>>,
	pub join_type: JoinType,
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
pub struct MapNode<'a> {
	pub map: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct ExtendNode<'a> {
	pub extend: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct ApplyNode<'a> {
	pub operator_name: Fragment<'a>,
	pub arguments: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct InlineDataNode<'a> {
	pub rows: Vec<Vec<AliasExpression<'a>>>,
}

#[derive(Debug)]
pub struct SourceScanNode<'a> {
	pub source: ResolvedSource<'a>,
	pub columns: Option<Vec<ResolvedColumn<'a>>>,
	pub index: Option<ResolvedIndex<'a>>,
}

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
