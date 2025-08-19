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
		ColumnPolicyKind, ColumnSaturationPolicy,
		expression::{AliasExpression, Expression},
	},
};

use crate::ast::{Ast, AstPolicy, AstPolicyKind, AstStatement};

struct Compiler {}

pub fn compile_logical(ast: AstStatement) -> crate::Result<Vec<LogicalPlan>> {
	Compiler::compile(ast)
}

impl Compiler {
	fn compile(ast: AstStatement) -> crate::Result<Vec<LogicalPlan>> {
		if ast.is_empty() {
			return Ok(vec![]);
		}

		let mut result = Vec::with_capacity(ast.len());
		for node in ast {
			match node {
				Ast::Create(node) => {
					result.push(Self::compile_create(node)?)
				}
				Ast::Alter(node) => {
					result.push(Self::compile_alter(node)?)
				}
				Ast::AstDelete(node) => {
					result.push(Self::compile_delete(node)?)
				}
				Ast::AstInsert(node) => {
					result.push(Self::compile_insert(node)?)
				}
				Ast::AstUpdate(node) => {
					result.push(Self::compile_update(node)?)
				}

				Ast::Aggregate(node) => result
					.push(Self::compile_aggregate(node)?),
				Ast::Filter(node) => {
					result.push(Self::compile_filter(node)?)
				}
				Ast::From(node) => {
					result.push(Self::compile_from(node)?)
				}
				Ast::Join(node) => {
					result.push(Self::compile_join(node)?)
				}
				Ast::Take(node) => {
					result.push(Self::compile_take(node)?)
				}
				Ast::Sort(node) => {
					result.push(Self::compile_sort(node)?)
				}
				Ast::Map(node) => {
					result.push(Self::compile_map(node)?)
				}
				node => unimplemented!("{:?}", node),
			}
		}
		Ok(result)
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
	Filter(FilterNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Take(TakeNode),
	Order(OrderNode),
	Map(MapNode),
	InlineData(InlineDataNode),
	SourceScan(SourceScanNode),
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
	pub table: OwnedFragment,
}

#[derive(Debug)]
pub struct InsertNode {
	pub schema: Option<OwnedFragment>,
	pub table: OwnedFragment,
}

#[derive(Debug)]
pub struct UpdateNode {
	pub schema: Option<OwnedFragment>,
	pub table: OwnedFragment,
}

#[derive(Debug)]
pub struct AggregateNode {
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
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
