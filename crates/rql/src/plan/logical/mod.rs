// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod alter;
mod create;
mod mutate;
mod query;
pub mod resolver;

use std::rc::Rc;

use identifier::{
	ColumnIdentifier, DeferredViewIdentifier, NamespaceIdentifier, RingBufferIdentifier, SequenceIdentifier,
	TableIdentifier, TransactionalViewIdentifier,
};
use reifydb_catalog::{
	CatalogQueryTransaction, ring_buffer::create::RingBufferColumnToCreate, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	IndexType, JoinType, SortDirection, SortKey,
	interface::{
		ColumnPolicyKind, ColumnSaturationPolicy, NamespaceDef, RingBufferDef, TableDef,
		expression::{AliasExpression, Expression},
		identifier,
		resolved::{ResolvedColumn, ResolvedIndex, ResolvedSource},
	},
	return_error,
};
use reifydb_type::{Fragment, diagnostic::ast::unsupported_ast_node};

use self::resolver::IdentifierResolver;
use crate::{
	ast::{Ast, AstPolicy, AstPolicyKind, AstStatement},
	plan::{
		logical::alter::{AlterTableNode, AlterViewNode},
		physical::PhysicalPlan,
	},
};

struct Compiler {}

pub fn compile_logical<'a, 't, T: CatalogQueryTransaction>(
	tx: &'t mut T,
	ast: AstStatement<'a>,
	default_namespace: &'static str,
) -> crate::Result<Vec<LogicalPlan<'a>>> {
	let mut resolver = IdentifierResolver::new(tx, default_namespace);
	Compiler::compile(ast, &mut resolver)
}

impl Compiler {
	fn compile<'a, 't, T: CatalogQueryTransaction>(
		ast: AstStatement<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
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
								return Ok(vec![LogicalPlan::Update(UpdateNode {
									target: None,
									input,
								})]);
							};

							// Try to resolve as table first (most common case)
							match resolver.resolve_source_as_table(
								unresolved.namespace.as_ref(),
								&unresolved.name,
								true,
							) {
								Ok(target) => {
									return Ok(vec![LogicalPlan::Update(
										UpdateNode {
											target: Some(target),
											input,
										},
									)]);
								}
								Err(table_error) => {
									// Table not found, try ring buffer
									match resolver.resolve_source_as_ring_buffer(
										unresolved.namespace.as_ref(),
										&unresolved.name,
										true,
									) {
										Ok(target) => {
											return Ok(vec![LogicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
												target,
												input,
											})]);
										}
										// Ring buffer also not found, return
										// the table error
										Err(_) => return Err(table_error),
									}
								}
							}
						}
						Ast::AstDelete(delete_ast) => {
							// Build the pipeline as
							// input to delete
							let input = if !pipeline_nodes.is_empty() {
								Some(Box::new(Self::build_pipeline(pipeline_nodes)?))
							} else {
								None
							};

							// Resolve to either TableIdentifier or RingBufferIdentifier
							let target = if let Some(unresolved) = &delete_ast.target {
								let source_id = resolver
									.resolve_unresolved_source(&unresolved)?;

								// Determine if it's a table or ring buffer
								match source_id {
									identifier::SourceIdentifier::Table(
										table_id,
									) => Some(DeleteTarget::Table(table_id)),
									identifier::SourceIdentifier::RingBuffer(
										ring_buffer_id,
									) => Some(DeleteTarget::RingBuffer(
										ring_buffer_id,
									)),
									_ => {
										// Source is not a table or ring buffer
										return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
											namespace: unresolved.namespace.as_ref().map(|n| n.text()).unwrap_or(resolver.default_namespace()).to_string(),
											name: unresolved.name.text().to_string(),
											fragment: unresolved.name.clone().into_owned(),
										}).into());
									}
								}
							} else {
								None
							};

							return Ok(vec![LogicalPlan::Delete(DeleteNode {
								target,
								input,
							})]);
						}
						_ => unreachable!(),
					}
				} else {
					// Add to pipeline
					pipeline_nodes.push(Compiler::compile_single(node, resolver)?);
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
				pipeline_nodes.push(Self::compile_single(node, resolver)?);
			}
			return Ok(vec![LogicalPlan::Pipeline(PipelineNode {
				steps: pipeline_nodes,
			})]);
		}

		// Normal compilation (not piped)
		let mut result = Vec::with_capacity(ast_len);
		for node in ast_vec {
			result.push(Self::compile_single(node, resolver)?);
		}
		Ok(result)
	}

	// Helper to compile a single AST node
	fn compile_single<'a, 't, T: CatalogQueryTransaction>(
		node: Ast<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		match node {
			Ast::Create(node) => Self::compile_create(node, resolver),
			Ast::Alter(node) => Self::compile_alter(node, resolver),
			Ast::AstDelete(node) => Self::compile_delete(node, resolver),
			Ast::AstInsert(node) => Self::compile_insert(node, resolver),
			Ast::AstUpdate(node) => Self::compile_update(node, resolver),
			Ast::Aggregate(node) => Self::compile_aggregate(node, resolver),
			Ast::Filter(node) => Self::compile_filter(node, resolver),
			Ast::From(node) => Self::compile_from(node, resolver),
			Ast::Join(node) => Self::compile_join(node, resolver),
			Ast::Take(node) => Self::compile_take(node, resolver),
			Ast::Sort(node) => Self::compile_sort(node, resolver),
			Ast::Distinct(node) => Self::compile_distinct(node, resolver),
			Ast::Map(node) => Self::compile_map(node, resolver),
			Ast::Extend(node) => Self::compile_extend(node, resolver),
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
	Delete(DeleteNode<'a>),
	InsertTable(InsertTableNode<'a>),
	InsertRingBuffer(InsertRingBufferNode<'a>),
	Update(UpdateNode<'a>),
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
	pub view: DeferredViewIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'a> {
	pub view: TransactionalViewIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateNamespaceNode<'a> {
	pub namespace: NamespaceIdentifier<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode<'a> {
	pub sequence: SequenceIdentifier<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode<'a> {
	pub table: TableIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug)]
pub struct CreateRingBufferNode<'a> {
	pub ring_buffer: RingBufferIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

#[derive(Debug)]
pub struct AlterSequenceNode<'a> {
	pub sequence: SequenceIdentifier<'a>,
	pub column: ColumnIdentifier<'a>,
	pub value: Expression<'a>,
}

#[derive(Debug)]
pub struct CreateIndexNode<'a> {
	pub index_type: IndexType,
	pub index: identifier::IndexIdentifier<'a>,
	pub columns: Vec<IndexColumn<'a>>,
	pub filter: Vec<Expression<'a>>,
	pub map: Option<Expression<'a>>,
}

#[derive(Debug)]
pub struct IndexColumn<'a> {
	pub column: ColumnIdentifier<'a>,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub enum DeleteTarget<'a> {
	Table(TableIdentifier<'a>),
	RingBuffer(RingBufferIdentifier<'a>),
}

#[derive(Debug)]
pub struct DeleteNode<'a> {
	pub target: Option<DeleteTarget<'a>>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct InsertTableNode<'a> {
	pub target: TableIdentifier<'a>,
}

#[derive(Debug)]
pub struct InsertRingBufferNode<'a> {
	pub target: RingBufferIdentifier<'a>,
}

#[derive(Debug)]
pub struct UpdateNode<'a> {
	pub target: Option<TableIdentifier<'a>>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct UpdateRingBufferNode<'a> {
	pub target: RingBufferIdentifier<'a>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct AggregateNode<'a> {
	pub by: Vec<Expression<'a>>,
	pub map: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct DistinctNode<'a> {
	pub columns: Vec<ColumnIdentifier<'a>>,
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
	pub source: Rc<ResolvedSource<'a>>,
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

/// Extract table information from a physical plan tree
/// Returns (namespace, table) if a unique table can be identified
pub fn extract_table_from_plan(plan: &PhysicalPlan) -> Option<(NamespaceDef, TableDef)> {
	match plan {
		PhysicalPlan::TableScan(scan) => Some((scan.namespace.clone(), scan.table.clone())),
		PhysicalPlan::Filter(filter) => extract_table_from_plan(&filter.input),
		PhysicalPlan::Map(map) => map.input.as_ref().and_then(|input| extract_table_from_plan(input)),
		PhysicalPlan::Extend(extend) => extend.input.as_ref().and_then(|input| extract_table_from_plan(input)),
		PhysicalPlan::Aggregate(agg) => extract_table_from_plan(&agg.input),
		PhysicalPlan::Sort(sort) => extract_table_from_plan(&sort.input),
		PhysicalPlan::Take(take) => extract_table_from_plan(&take.input),
		PhysicalPlan::JoinInner(join) => {
			// Check both sides, prefer table over inline data
			let left = extract_table_from_plan(&join.left);
			let right = extract_table_from_plan(&join.right);

			match (left, right) {
				(Some(table), None) | (None, Some(table)) => Some(table),
				(Some(left_table), Some(_right_table)) => {
					// Multiple tables - ambiguous, caller
					// should handle For now, return
					// the left table
					Some(left_table)
				}
				(None, None) => None,
			}
		}
		PhysicalPlan::JoinLeft(join) => {
			// For left join, the left side is the primary table
			extract_table_from_plan(&join.left)
		}
		PhysicalPlan::JoinNatural(join) => {
			// Check both sides, prefer table over inline data
			let left = extract_table_from_plan(&join.left);
			let right = extract_table_from_plan(&join.right);

			match (left, right) {
				(Some(table), None) | (None, Some(table)) => Some(table),
				(Some(left_table), Some(_right_table)) => {
					// Multiple tables - ambiguous
					Some(left_table)
				}
				(None, None) => None,
			}
		}
		PhysicalPlan::InlineData(_) => None,
		PhysicalPlan::ViewScan(_) => None, // Views are not directly
		// deleteable for now
		_ => None,
	}
}

/// Returns (namespace, ring_buffer) if a unique ring buffer can be identified
pub fn extract_ring_buffer_from_plan(plan: &PhysicalPlan) -> Option<(NamespaceDef, RingBufferDef)> {
	match plan {
		PhysicalPlan::RingBufferScan(scan) => Some((scan.namespace.clone(), scan.ring_buffer.clone())),
		PhysicalPlan::Filter(filter) => extract_ring_buffer_from_plan(&filter.input),
		PhysicalPlan::Map(map) => map.input.as_ref().and_then(|input| extract_ring_buffer_from_plan(input)),
		PhysicalPlan::Extend(extend) => {
			extend.input.as_ref().and_then(|input| extract_ring_buffer_from_plan(input))
		}
		PhysicalPlan::Aggregate(agg) => extract_ring_buffer_from_plan(&agg.input),
		PhysicalPlan::Sort(sort) => extract_ring_buffer_from_plan(&sort.input),
		PhysicalPlan::Take(take) => extract_ring_buffer_from_plan(&take.input),
		PhysicalPlan::JoinInner(join) => {
			// Check both sides, prefer ring buffer over inline data
			let left = extract_ring_buffer_from_plan(&join.left);
			let right = extract_ring_buffer_from_plan(&join.right);

			match (left, right) {
				(Some(ring_buffer), None) | (None, Some(ring_buffer)) => Some(ring_buffer),
				(Some(left_rb), Some(_right_rb)) => {
					// Multiple ring buffers - ambiguous, return the left one
					Some(left_rb)
				}
				(None, None) => None,
			}
		}
		PhysicalPlan::JoinLeft(join) => {
			// For left join, the left side is the primary ring buffer
			extract_ring_buffer_from_plan(&join.left)
		}
		PhysicalPlan::JoinNatural(join) => {
			// Check both sides, prefer ring buffer over inline data
			let left = extract_ring_buffer_from_plan(&join.left);
			let right = extract_ring_buffer_from_plan(&join.right);

			match (left, right) {
				(Some(ring_buffer), None) | (None, Some(ring_buffer)) => Some(ring_buffer),
				(Some(left_rb), Some(_right_rb)) => {
					// Multiple ring buffers - ambiguous
					Some(left_rb)
				}
				(None, None) => None,
			}
		}
		PhysicalPlan::InlineData(_) => None,
		PhysicalPlan::TableScan(_) => None,
		PhysicalPlan::ViewScan(_) => None,
		_ => None,
	}
}
