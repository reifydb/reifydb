// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod alter;
mod create;
mod mutate;
mod query;

use reifydb_catalog::{table::TableColumnToCreate, view::ViewColumnToCreate};
use reifydb_core::{
	IndexType, JoinType, OwnedFragment, SortDirection, SortKey,
	interface::{
		ColumnPolicyKind, ColumnSaturationPolicy, SchemaDef, TableDef,
		expression::{AliasExpression, Expression},
	},
};

use crate::{
	ast::{Ast, AstPolicy, AstPolicyKind, AstStatement},
	plan::physical::PhysicalPlan,
};

struct Compiler {}

pub fn compile_logical(ast: AstStatement) -> crate::Result<Vec<LogicalPlan>> {
	Compiler::compile(ast)
}

impl Compiler {
	fn compile(ast: AstStatement) -> crate::Result<Vec<LogicalPlan>> {
		if ast.is_empty() {
			return Ok(vec![]);
		}

		let ast_len = ast.len();
		let ast_vec = ast.0; // Extract the inner Vec

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
								input,
							})]);
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
								input,
							})]);
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

		// Normal compilation (not a pipeline)
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
			node => unimplemented!("{:?}", node),
		}
	}

	fn build_chain(plans: Vec<LogicalPlan>) -> crate::Result<LogicalPlan> {
		// The pipeline should be properly structured with inputs
		// For now, we'll wrap them in a special Pipeline plan
		// that the physical compiler can handle
		if plans.is_empty() {
			panic!("Empty pipeline");
		}

		// Return a Chain logical plan that contains all the steps
		Ok(LogicalPlan::Chain(ChainedNode {
			steps: plans,
		}))
	}
}

#[derive(Debug)]
pub enum LogicalPlan {
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateSchema(CreateSchemaNode),
	CreateSequence(CreateSequenceNode),
	CreateTable(CreateTableNode),
	CreateIndex(CreateIndexNode),
	// Alter
	AlterSequence(AlterSequenceNode),
	// Mutate
	Delete(DeleteNode),
	Insert(InsertNode),
	Update(UpdateNode),
	// Query
	Aggregate(AggregateNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Take(TakeNode),
	Order(OrderNode),
	Map(MapNode),
	Extend(ExtendNode),
	InlineData(InlineDataNode),
	SourceScan(SourceScanNode),
	// Chain wrapper for chained operations
	Chain(ChainedNode),
}

#[derive(Debug)]
pub struct ChainedNode {
	pub steps: Vec<LogicalPlan>,
}

#[derive(Debug)]
pub struct CreateDeferredViewNode {
	pub schema: OwnedFragment,
	pub view: OwnedFragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode {
	pub schema: OwnedFragment,
	pub view: OwnedFragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Vec<LogicalPlan>,
}

#[derive(Debug)]
pub struct CreateSchemaNode {
	pub schema: OwnedFragment,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode {
	pub schema: OwnedFragment,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode {
	pub schema: OwnedFragment,
	pub table: OwnedFragment,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug)]
pub struct AlterSequenceNode {
	pub schema: Option<OwnedFragment>,
	pub table: OwnedFragment,
	pub column: OwnedFragment,
	pub value: Expression,
}

#[derive(Debug)]
pub struct CreateIndexNode {
	pub index_type: IndexType,
	pub name: OwnedFragment,
	pub schema: OwnedFragment,
	pub table: OwnedFragment,
	pub columns: Vec<IndexColumn>,
	pub filter: Vec<Expression>,
	pub map: Option<Expression>,
}

#[derive(Debug)]
pub struct IndexColumn {
	pub column: OwnedFragment,
	pub order: Option<SortDirection>,
}

#[derive(Debug)]
pub struct DeleteNode {
	pub schema: Option<OwnedFragment>,
	pub table: Option<OwnedFragment>,
	pub input: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct InsertNode {
	pub schema: Option<OwnedFragment>,
	pub table: OwnedFragment,
}

#[derive(Debug)]
pub struct UpdateNode {
	pub schema: Option<OwnedFragment>,
	pub table: Option<OwnedFragment>,
	pub input: Option<Box<LogicalPlan>>,
}

#[derive(Debug)]
pub struct AggregateNode {
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

#[derive(Debug)]
pub struct DistinctNode {
	pub columns: Vec<OwnedFragment>,
}

#[derive(Debug)]
pub struct FilterNode {
	pub condition: Expression,
}

#[derive(Debug)]
pub struct JoinInnerNode {
	pub with: Vec<LogicalPlan>,
	pub on: Vec<Expression>,
}

#[derive(Debug)]
pub struct JoinLeftNode {
	pub with: Vec<LogicalPlan>,
	pub on: Vec<Expression>,
}

#[derive(Debug)]
pub struct JoinNaturalNode {
	pub with: Vec<LogicalPlan>,
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
pub struct MapNode {
	pub map: Vec<Expression>,
}

#[derive(Debug)]
pub struct ExtendNode {
	pub extend: Vec<Expression>,
}

#[derive(Debug)]
pub struct InlineDataNode {
	pub rows: Vec<Vec<AliasExpression>>,
}

#[derive(Debug)]
pub struct SourceScanNode {
	pub schema: OwnedFragment,
	pub source: OwnedFragment,
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
