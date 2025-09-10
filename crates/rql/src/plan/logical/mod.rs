// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod alter;
mod create;
mod mutate;
mod query;

use reifydb_catalog::{table::TableColumnToCreate, view::ViewColumnToCreate};
use reifydb_core::{
	IndexType, JoinType, SortDirection, SortKey,
	diagnostic::ast::{empty_pipeline_error, unsupported_ast_node},
	interface::{
		ColumnPolicyKind, ColumnSaturationPolicy, SchemaDef, TableDef,
		expression::{AliasExpression, Expression},
	},
	return_error,
};
use reifydb_type::Fragment;

use crate::{
	ast::{Ast, AstPolicy, AstPolicyKind, AstStatement},
	plan::{
		logical::alter::{AlterTableNode, AlterViewNode},
		physical::PhysicalPlan,
	},
};

struct Compiler {}

pub fn compile_logical<'a>(
	ast: AstStatement<'a>,
) -> crate::Result<Vec<LogicalPlan<'a>>> {
	Compiler::compile(ast)
}

impl Compiler {
	fn compile<'a>(
		ast: AstStatement<'a>,
	) -> crate::Result<Vec<LogicalPlan<'a>>> {
		if ast.is_empty() {
			return Ok(vec![]);
		}

		let ast_len = ast.len();
		let has_pipes = ast.has_pipes;
		let ast_vec = ast.nodes; // Extract the inner Vec

		// Check if this is a pipeline ending with UPDATE or DELETE
		let is_update_pipeline = ast_len > 1
			&& matches!(ast_vec.last(), Some(Ast::AstUpdate(_)));
		let is_delete_pipeline = ast_len > 1
			&& matches!(ast_vec.last(), Some(Ast::AstDelete(_)));

		if is_update_pipeline || is_delete_pipeline {
			// Build pipeline: compile all nodes except the last one
			// into a chain
			let mut chain_nodes = Vec::new();

			for (i, node) in ast_vec.into_iter().enumerate() {
				if i == ast_len - 1 {
					// Last node is UPDATE or DELETE
					match node {
						Ast::AstUpdate(update_ast) => {
							// Build the pipeline as
							// input to update
							let input =
								if !chain_nodes
									.is_empty(
									) {
									Some(Box::new(Self::build_chain(chain_nodes)?))
								} else {
									None
								};

							return Ok(vec![LogicalPlan::Update(UpdateNode {
								schema: update_ast.schema.map(|s| s.fragment()),
								table: update_ast.table.map(|t| t.fragment()),
								input})]);
						}
						Ast::AstDelete(delete_ast) => {
							// Build the pipeline as
							// input to delete
							let input =
								if !chain_nodes
									.is_empty(
									) {
									Some(Box::new(Self::build_chain(chain_nodes)?))
								} else {
									None
								};

							return Ok(vec![LogicalPlan::Delete(DeleteNode {
								schema: delete_ast.schema.map(|s| s.fragment()),
								table: delete_ast.table.map(|t| t.fragment()),
								input})]);
						}
						_ => unreachable!(),
					}
				} else {
					// Add to pipeline
					chain_nodes.push(Self::compile_single(
						node,
					)?);
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
				pipeline_nodes
					.push(Self::compile_single(node)?);
			}
			return Ok(vec![LogicalPlan::Pipeline(PipelineNode {
				steps: pipeline_nodes,
			})]);
		}

		// Normal compilation (not piped)
		let mut result = Vec::with_capacity(ast_len);
		for node in ast_vec {
			result.push(Self::compile_single(node)?);
		}
		Ok(result)
	}

	// Helper to compile a single AST node
	fn compile_single(node: Ast) -> crate::Result<LogicalPlan> {
		match node {
			Ast::Create(node) => Self::compile_create(node),
			Ast::Alter(node) => Self::compile_alter(node),
			Ast::AstDelete(node) => Self::compile_delete(node),
			Ast::AstInsert(node) => Self::compile_insert(node),
			Ast::AstUpdate(node) => Self::compile_update(node),
			Ast::Aggregate(node) => Self::compile_aggregate(node),
			Ast::Filter(node) => Self::compile_filter(node),
			Ast::From(node) => Self::compile_from(node),
			Ast::Join(node) => Self::compile_join(node),
			Ast::Take(node) => Self::compile_take(node),
			Ast::Sort(node) => Self::compile_sort(node),
			Ast::Distinct(node) => Self::compile_distinct(node),
			Ast::Map(node) => Self::compile_map(node),
			Ast::Extend(node) => Self::compile_extend(node),
			Ast::Identifier(ref id) => {
				return_error!(unsupported_ast_node(
					id.0.fragment.clone(),
					"standalone identifier"
				))
			}
			node => {
				let node_type = format!("{:?}", node)
					.split('(')
					.next()
					.unwrap_or("Unknown")
					.to_string();
				return_error!(unsupported_ast_node(
					node.token().fragment.clone(),
					&node_type
				))
			}
		}
	}

	fn build_chain<'a>(
		plans: Vec<LogicalPlan<'a>>,
	) -> crate::Result<LogicalPlan<'a>> {
		// The pipeline should be properly structured with inputs
		// For now, we'll wrap them in a special Pipeline plan
		// that the physical compiler can handle
		if plans.is_empty() {
			return_error!(empty_pipeline_error());
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
	CreateSchema(CreateSchemaNode<'a>),
	CreateSequence(CreateSequenceNode<'a>),
	CreateTable(CreateTableNode<'a>),
	CreateIndex(CreateIndexNode<'a>),
	// Alter
	AlterSequence(AlterSequenceNode<'a>),
	AlterTable(AlterTableNode<'a>),
	AlterView(AlterViewNode<'a>),
	// Mutate
	Delete(DeleteNode<'a>),
	Insert(InsertNode<'a>),
	Update(UpdateNode<'a>),
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
	pub schema: Fragment<'a>,
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'a> {
	pub schema: Fragment<'a>,
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan<'a>>,
}

#[derive(Debug)]
pub struct CreateSchemaNode<'a> {
	pub schema: Fragment<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode<'a> {
	pub schema: Fragment<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode<'a> {
	pub schema: Fragment<'a>,
	pub table: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug)]
pub struct AlterSequenceNode<'a> {
	pub schema: Option<Fragment<'a>>,
	pub table: Fragment<'a>,
	pub column: Fragment<'a>,
	pub value: Expression<'a>,
}

#[derive(Debug)]
pub struct CreateIndexNode<'a> {
	pub index_type: IndexType,
	pub name: Fragment<'a>,
	pub schema: Fragment<'a>,
	pub table: Fragment<'a>,
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
pub struct DeleteNode<'a> {
	pub schema: Option<Fragment<'a>>,
	pub table: Option<Fragment<'a>>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct InsertNode<'a> {
	pub schema: Option<Fragment<'a>>,
	pub table: Fragment<'a>,
}

#[derive(Debug)]
pub struct UpdateNode<'a> {
	pub schema: Option<Fragment<'a>>,
	pub table: Option<Fragment<'a>>,
	pub input: Option<Box<LogicalPlan<'a>>>,
}

#[derive(Debug)]
pub struct AggregateNode<'a> {
	pub by: Vec<Expression<'a>>,
	pub map: Vec<Expression<'a>>,
}

#[derive(Debug)]
pub struct DistinctNode<'a> {
	pub columns: Vec<Fragment<'a>>,
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
pub struct InlineDataNode<'a> {
	pub rows: Vec<Vec<AliasExpression<'a>>>,
}

#[derive(Debug)]
pub struct SourceScanNode<'a> {
	pub schema: Fragment<'a>,
	pub source: Fragment<'a>,
	pub index_name: Option<Fragment<'a>>,
}

pub(crate) fn convert_policy(ast: &AstPolicy) -> ColumnPolicyKind {
	use ColumnPolicyKind::*;

	match ast.policy {
		AstPolicyKind::Saturation => {
			if ast.value.is_literal_undefined() {
				return Saturation(
					ColumnSaturationPolicy::Undefined,
				);
			}
			let ident = ast.value.as_identifier().value();
			match ident {
				"error" => Saturation(
					ColumnSaturationPolicy::Error,
				),
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
/// Returns (schema, table) if a unique table can be identified
pub fn extract_table_from_plan(
	plan: &PhysicalPlan,
) -> Option<(SchemaDef, TableDef)> {
	match plan {
		PhysicalPlan::TableScan(scan) => {
			Some((scan.schema.clone(), scan.table.clone()))
		}
		PhysicalPlan::Filter(filter) => {
			extract_table_from_plan(&filter.input)
		}
		PhysicalPlan::Map(map) => map
			.input
			.as_ref()
			.and_then(|input| extract_table_from_plan(input)),
		PhysicalPlan::Extend(extend) => extend
			.input
			.as_ref()
			.and_then(|input| extract_table_from_plan(input)),
		PhysicalPlan::Aggregate(agg) => {
			extract_table_from_plan(&agg.input)
		}
		PhysicalPlan::Sort(sort) => {
			extract_table_from_plan(&sort.input)
		}
		PhysicalPlan::Take(take) => {
			extract_table_from_plan(&take.input)
		}
		PhysicalPlan::JoinInner(join) => {
			// Check both sides, prefer table over inline data
			let left = extract_table_from_plan(&join.left);
			let right = extract_table_from_plan(&join.right);

			match (left, right) {
				(Some(table), None) | (None, Some(table)) => {
					Some(table)
				}
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
				(Some(table), None) | (None, Some(table)) => {
					Some(table)
				}
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
