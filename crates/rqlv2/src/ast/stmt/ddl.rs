// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DDL statement types (CREATE, ALTER, DROP).

use super::Expr;
use crate::token::Span;

/// CREATE statement.
#[derive(Debug, Clone, Copy)]
pub enum CreateStmt<'bump> {
	/// CREATE TABLE
	Table(CreateTable<'bump>),
	/// CREATE VIEW
	View(CreateView<'bump>),
	/// CREATE FLOW
	Flow(CreateFlow<'bump>),
	/// CREATE NAMESPACE
	Namespace(CreateNamespace<'bump>),
	/// CREATE INDEX
	Index(CreateIndex<'bump>),
	/// CREATE SEQUENCE
	Sequence(CreateSequence<'bump>),
}

impl<'bump> CreateStmt<'bump> {
	/// Get the span of this CREATE statement.
	pub fn span(&self) -> Span {
		match self {
			CreateStmt::Table(t) => t.span,
			CreateStmt::View(v) => v.span,
			CreateStmt::Flow(f) => f.span,
			CreateStmt::Namespace(n) => n.span,
			CreateStmt::Index(i) => i.span,
			CreateStmt::Sequence(s) => s.span,
		}
	}
}

/// CREATE TABLE
#[derive(Debug, Clone, Copy)]
pub struct CreateTable<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub columns: &'bump [ColumnDef<'bump>],
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateTable<'bump> {
	/// Create a new CREATE TABLE statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		columns: &'bump [ColumnDef<'bump>],
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			columns,
			if_not_exists,
			span,
		}
	}
}

/// Column definition in CREATE TABLE.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDef<'bump> {
	pub name: &'bump str,
	pub data_type: &'bump str,
	pub nullable: bool,
	pub default: Option<&'bump Expr<'bump>>,
	pub span: Span,
}

impl<'bump> ColumnDef<'bump> {
	/// Create a new column definition.
	pub fn new(
		name: &'bump str,
		data_type: &'bump str,
		nullable: bool,
		default: Option<&'bump Expr<'bump>>,
		span: Span,
	) -> Self {
		Self {
			name,
			data_type,
			nullable,
			default,
			span,
		}
	}
}

/// CREATE VIEW
#[derive(Debug, Clone, Copy)]
pub struct CreateView<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub query: &'bump [Expr<'bump>],
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateView<'bump> {
	/// Create a new CREATE VIEW statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		query: &'bump [Expr<'bump>],
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			query,
			if_not_exists,
			span,
		}
	}
}

/// CREATE FLOW
#[derive(Debug, Clone, Copy)]
pub struct CreateFlow<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub query: &'bump [Expr<'bump>],
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateFlow<'bump> {
	/// Create a new CREATE FLOW statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		query: &'bump [Expr<'bump>],
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			query,
			if_not_exists,
			span,
		}
	}
}

/// CREATE NAMESPACE
#[derive(Debug, Clone, Copy)]
pub struct CreateNamespace<'bump> {
	pub name: &'bump str,
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateNamespace<'bump> {
	/// Create a new CREATE NAMESPACE statement.
	pub fn new(name: &'bump str, if_not_exists: bool, span: Span) -> Self {
		Self {
			name,
			if_not_exists,
			span,
		}
	}
}

/// CREATE INDEX
#[derive(Debug, Clone, Copy)]
pub struct CreateIndex<'bump> {
	pub name: &'bump str,
	pub table: &'bump str,
	pub columns: &'bump [&'bump str],
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateIndex<'bump> {
	/// Create a new CREATE INDEX statement.
	pub fn new(
		name: &'bump str,
		table: &'bump str,
		columns: &'bump [&'bump str],
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			name,
			table,
			columns,
			if_not_exists,
			span,
		}
	}
}

/// CREATE SEQUENCE
#[derive(Debug, Clone, Copy)]
pub struct CreateSequence<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub start: Option<i64>,
	pub increment: Option<i64>,
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateSequence<'bump> {
	/// Create a new CREATE SEQUENCE statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		start: Option<i64>,
		increment: Option<i64>,
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			start,
			increment,
			if_not_exists,
			span,
		}
	}
}

/// ALTER statement.
#[derive(Debug, Clone, Copy)]
pub enum AlterStmt<'bump> {
	/// ALTER TABLE
	Table(AlterTable<'bump>),
	/// ALTER SEQUENCE
	Sequence(AlterSequence<'bump>),
}

impl<'bump> AlterStmt<'bump> {
	/// Get the span of this ALTER statement.
	pub fn span(&self) -> Span {
		match self {
			AlterStmt::Table(t) => t.span,
			AlterStmt::Sequence(s) => s.span,
		}
	}
}

/// ALTER TABLE
#[derive(Debug, Clone, Copy)]
pub struct AlterTable<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub action: AlterTableAction<'bump>,
	pub span: Span,
}

impl<'bump> AlterTable<'bump> {
	/// Create a new ALTER TABLE statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		action: AlterTableAction<'bump>,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			action,
			span,
		}
	}
}

/// ALTER TABLE action.
#[derive(Debug, Clone, Copy)]
pub enum AlterTableAction<'bump> {
	AddColumn(ColumnDef<'bump>),
	DropColumn(&'bump str),
	RenameColumn {
		from: &'bump str,
		to: &'bump str,
	},
}

/// ALTER SEQUENCE
#[derive(Debug, Clone, Copy)]
pub struct AlterSequence<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub restart: Option<i64>,
	pub span: Span,
}

impl<'bump> AlterSequence<'bump> {
	/// Create a new ALTER SEQUENCE statement.
	pub fn new(namespace: Option<&'bump str>, name: &'bump str, restart: Option<i64>, span: Span) -> Self {
		Self {
			namespace,
			name,
			restart,
			span,
		}
	}
}

/// DROP statement.
#[derive(Debug, Clone, Copy)]
pub struct DropStmt<'bump> {
	pub object_type: DropObjectType,
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub if_exists: bool,
	pub span: Span,
}

impl<'bump> DropStmt<'bump> {
	/// Create a new DROP statement.
	pub fn new(
		object_type: DropObjectType,
		namespace: Option<&'bump str>,
		name: &'bump str,
		if_exists: bool,
		span: Span,
	) -> Self {
		Self {
			object_type,
			namespace,
			name,
			if_exists,
			span,
		}
	}
}

/// Object type for DROP statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropObjectType {
	Table,
	View,
	Flow,
	Namespace,
	Index,
	Sequence,
}
