// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL plan nodes (Create, Alter, Drop).

use reifydb_type::value::r#type::Type;

use crate::{
	plan::{
		Plan,
		node::{expr::PlanExpr, query::SortDirection},
		types::{CatalogColumn, Dictionary, Index, Namespace, RingBuffer, Sequence, Table, View},
	},
	token::span::Span,
};

/// Create DDL operation.
#[derive(Debug, Clone, Copy)]
pub enum CreateNode<'bump> {
	Namespace(CreateNamespaceNode<'bump>),
	Table(CreateTableNode<'bump>),
	View(CreateViewNode<'bump>),
	Index(CreateIndexNode<'bump>),
	Sequence(CreateSequenceNode<'bump>),
	RingBuffer(CreateRingBufferNode<'bump>),
	Dictionary(CreateDictionaryNode<'bump>),
}

impl<'bump> CreateNode<'bump> {
	/// Get the span of this create node.
	pub fn span(&self) -> Span {
		match self {
			CreateNode::Namespace(n) => n.span,
			CreateNode::Table(n) => n.span,
			CreateNode::View(n) => n.span,
			CreateNode::Index(n) => n.span,
			CreateNode::Sequence(n) => n.span,
			CreateNode::RingBuffer(n) => n.span,
			CreateNode::Dictionary(n) => n.span,
		}
	}
}

/// Create namespace.
#[derive(Debug, Clone, Copy)]
pub struct CreateNamespaceNode<'bump> {
	pub name: &'bump str,
	pub if_not_exists: bool,
	pub span: Span,
}

/// Create table.
#[derive(Debug, Clone, Copy)]
pub struct CreateTableNode<'bump> {
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [ColumnDefinition<'bump>],
	pub primary_key: Option<&'bump [&'bump str]>,
	pub if_not_exists: bool,
	pub span: Span,
}

/// Column definition for CREATE TABLE.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDefinition<'bump> {
	pub name: &'bump str,
	pub column_type: Type,
	pub nullable: bool,
	pub default: Option<&'bump PlanExpr<'bump>>,
	pub span: Span,
}

/// Create view.
#[derive(Debug, Clone, Copy)]
pub struct CreateViewNode<'bump> {
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub query: &'bump Plan<'bump>,
	pub if_not_exists: bool,
	pub span: Span,
}

/// Create index.
#[derive(Debug, Clone, Copy)]
pub struct CreateIndexNode<'bump> {
	pub table: &'bump Table<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [IndexColumnDef<'bump>],
	pub unique: bool,
	pub filter: Option<&'bump PlanExpr<'bump>>,
	pub span: Span,
}

/// Index column definition.
#[derive(Debug, Clone, Copy)]
pub struct IndexColumnDef<'bump> {
	pub column: &'bump CatalogColumn<'bump>,
	pub direction: SortDirection,
}

/// Create sequence.
#[derive(Debug, Clone, Copy)]
pub struct CreateSequenceNode<'bump> {
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub start: Option<i64>,
	pub increment: Option<i64>,
	pub if_not_exists: bool,
	pub span: Span,
}

/// Create ring buffer.
#[derive(Debug, Clone, Copy)]
pub struct CreateRingBufferNode<'bump> {
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub columns: &'bump [ColumnDefinition<'bump>],
	pub capacity: u64,
	pub if_not_exists: bool,
	pub span: Span,
}

/// Create dictionary.
#[derive(Debug, Clone, Copy)]
pub struct CreateDictionaryNode<'bump> {
	pub namespace: &'bump Namespace<'bump>,
	pub name: &'bump str,
	pub key_type: Type,
	pub value_type: Type,
	pub if_not_exists: bool,
	pub span: Span,
}

/// Alter DDL operation.
#[derive(Debug, Clone, Copy)]
pub enum AlterNode<'bump> {
	Table(AlterTableNode<'bump>),
	Sequence(AlterSequenceNode<'bump>),
}

impl<'bump> AlterNode<'bump> {
	/// Get the span of this alter node.
	pub fn span(&self) -> Span {
		match self {
			AlterNode::Table(n) => n.span,
			AlterNode::Sequence(n) => n.span,
		}
	}
}

/// Alter table.
#[derive(Debug, Clone, Copy)]
pub struct AlterTableNode<'bump> {
	pub table: &'bump Table<'bump>,
	pub action: AlterTableAction<'bump>,
	pub span: Span,
}

/// Alter table action.
#[derive(Debug, Clone, Copy)]
pub enum AlterTableAction<'bump> {
	AddColumn(ColumnDefinition<'bump>),
	DropColumn(&'bump str),
	RenameColumn {
		from: &'bump str,
		to: &'bump str,
	},
}

/// Alter sequence.
#[derive(Debug, Clone, Copy)]
pub struct AlterSequenceNode<'bump> {
	pub sequence: &'bump Sequence<'bump>,
	pub restart: Option<i64>,
	pub span: Span,
}

/// Drop DDL operation.
#[derive(Debug, Clone, Copy)]
pub struct DropNode<'bump> {
	pub target: DropTarget<'bump>,
	pub if_exists: bool,
	pub span: Span,
}

/// Drop target.
#[derive(Debug, Clone, Copy)]
pub enum DropTarget<'bump> {
	Namespace(&'bump str),
	Table(&'bump Table<'bump>),
	View(&'bump View<'bump>),
	Index(&'bump Index<'bump>),
	Sequence(&'bump Sequence<'bump>),
	RingBuffer(&'bump RingBuffer<'bump>),
	Dictionary(&'bump Dictionary<'bump>),
}
