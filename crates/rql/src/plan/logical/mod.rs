// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod create;
mod expression;
mod mutate;
mod query;

use crate::ast::{Ast, AstIdentifier, AstPolicy, AstPolicyKind, AstStatement};
use crate::expression::{Expression, KeyedExpression};
use reifydb_catalog::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
use reifydb_catalog::table::ColumnToCreate;
use crate::Error;
use reifydb_core::{Type, SortKey, Span};
use reifydb_core::diagnostic::parse::unrecognized_type;

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
                Ast::Create(node) => result.push(Self::compile_create(node)?),
                Ast::AstInsert(node) => result.push(Self::compile_insert(node)?),

                Ast::Aggregate(node) => result.push(Self::compile_aggregate(node)?),
                Ast::Filter(node) => result.push(Self::compile_filter(node)?),
                Ast::From(node) => result.push(Self::compile_from(node)?),
                Ast::Join(node) => result.push(Self::compile_join(node)?),
                Ast::Take(node) => result.push(Self::compile_take(node)?),
                Ast::Sort(node) => result.push(Self::compile_sort(node)?),
                Ast::Map(node) => result.push(Self::compile_map(node)?),
                node => unimplemented!("{:?}", node),
            }
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub enum LogicalPlan {
    CreateDeferredView(CreateDeferredViewNode),
    CreateSchema(CreateSchemaNode),
    CreateSequence(CreateSequenceNode),
    CreateTable(CreateTableNode),
    // Mutate
    Insert(InsertNode),
    // Query
    Aggregate(AggregateNode),
    Filter(FilterNode),
    JoinLeft(JoinLeftNode),
    Take(TakeNode),
    Order(OrderNode),
    Map(MapNode),
    InlineData(InlineDataNode),
    TableScan(TableScanNode),
}

#[derive(Debug)]
pub struct CreateDeferredViewNode {
    pub schema: Span,
    pub view: Span,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug)]
pub struct CreateSchemaNode {
    pub schema: Span,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSequenceNode {
    pub schema: Span,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTableNode {
    pub schema: Span,
    pub table: Span,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnToCreate>,
}

#[derive(Debug)]
pub struct InsertNode {
    pub schema: Option<Span>,
    pub table: Span,
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
pub struct JoinLeftNode {
    pub with: Vec<LogicalPlan>,
    pub on: Vec<Expression>,
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
    pub rows: Vec<Vec<KeyedExpression>>,
}

#[derive(Debug)]
pub struct TableScanNode {
    pub schema: Option<Span>,
    pub table: Span,
}

pub(crate) fn convert_data_type(ast: &AstIdentifier) -> crate::Result<Type> {
    Ok(match ast.value().to_ascii_lowercase().as_str() {
        "bool" => Type::Bool,
        "float4" => Type::Float4,
        "float8" => Type::Float8,
        "int1" => Type::Int1,
        "int2" => Type::Int2,
        "int4" => Type::Int4,
        "int8" => Type::Int8,
        "int16" => Type::Int16,
        "uint1" => Type::Uint1,
        "uint2" => Type::Uint2,
        "uint4" => Type::Uint4,
        "uint8" => Type::Uint8,
        "uint16" => Type::Uint16,
        "utf8" => Type::Utf8,
        "text" => Type::Utf8,
        "date" => Type::Date,
        "datetime" => Type::DateTime,
        "time" => Type::Time,
        "interval" => Type::Interval,
        _ => return Err(Error(unrecognized_type(ast.span.clone()))),
    })
}

pub(crate) fn convert_policy(ast: &AstPolicy) -> ColumnPolicyKind {
    use ColumnPolicyKind::*;

    match ast.policy {
        AstPolicyKind::Saturation => {
            if ast.value.is_literal_undefined() {
                return Saturation(ColumnSaturationPolicy::Undefined);
            }
            let ident = ast.value.as_identifier().value();
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
