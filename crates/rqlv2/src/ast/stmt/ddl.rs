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
	/// CREATE DICTIONARY
	Dictionary(CreateDictionary<'bump>),
	/// CREATE RINGBUFFER
	RingBuffer(CreateRingBuffer<'bump>),
	/// CREATE SERIES
	Series(CreateSeries<'bump>),
	/// CREATE SUBSCRIPTION
	Subscription(CreateSubscription<'bump>),
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
			CreateStmt::Dictionary(d) => d.span,
			CreateStmt::RingBuffer(r) => r.span,
			CreateStmt::Series(s) => s.span,
			CreateStmt::Subscription(s) => s.span,
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
	pub policies: Option<PolicyBlock<'bump>>,
	pub auto_increment: bool,
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
			policies: None,
			auto_increment: false,
			span,
		}
	}

	/// Create a column definition with all fields.
	pub fn with_policies(
		name: &'bump str,
		data_type: &'bump str,
		nullable: bool,
		default: Option<&'bump Expr<'bump>>,
		policies: Option<PolicyBlock<'bump>>,
		auto_increment: bool,
		span: Span,
	) -> Self {
		Self {
			name,
			data_type,
			nullable,
			default,
			policies,
			auto_increment,
			span,
		}
	}
}

/// Policy block for column definitions.
///
/// Syntax: `POLICY { saturation error, default 0, not undefined }`
#[derive(Debug, Clone, Copy)]
pub struct PolicyBlock<'bump> {
	pub policies: &'bump [Policy<'bump>],
	pub span: Span,
}

impl<'bump> PolicyBlock<'bump> {
	/// Create a new policy block.
	pub fn new(policies: &'bump [Policy<'bump>], span: Span) -> Self {
		Self { policies, span }
	}
}

/// A single policy in a policy block.
#[derive(Debug, Clone, Copy)]
pub struct Policy<'bump> {
	pub kind: PolicyKind,
	pub value: &'bump Expr<'bump>,
	pub span: Span,
}

impl<'bump> Policy<'bump> {
	/// Create a new policy.
	pub fn new(kind: PolicyKind, value: &'bump Expr<'bump>, span: Span) -> Self {
		Self { kind, value, span }
	}
}

/// Kind of policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
	/// Saturation behavior (error or undefined)
	Saturation,
	/// Default value for the column
	Default,
	/// Column cannot be undefined
	NotUndefined,
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
	pub or_replace: bool,
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateFlow<'bump> {
	/// Create a new CREATE FLOW statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		query: &'bump [Expr<'bump>],
		or_replace: bool,
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			query,
			or_replace,
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
	pub namespace: Option<&'bump str>,
	pub table: &'bump str,
	pub columns: &'bump [IndexColumn<'bump>],
	pub unique: bool,
	pub span: Span,
}

impl<'bump> CreateIndex<'bump> {
	/// Create a new CREATE INDEX statement.
	pub fn new(
		name: &'bump str,
		namespace: Option<&'bump str>,
		table: &'bump str,
		columns: &'bump [IndexColumn<'bump>],
		unique: bool,
		span: Span,
	) -> Self {
		Self {
			name,
			namespace,
			table,
			columns,
			unique,
			span,
		}
	}
}

/// Index column with optional sort direction
#[derive(Debug, Clone, Copy)]
pub struct IndexColumn<'bump> {
	pub name: &'bump str,
	pub descending: bool,
	pub span: Span,
}

impl<'bump> IndexColumn<'bump> {
	/// Create a new index column.
	pub fn new(name: &'bump str, descending: bool, span: Span) -> Self {
		Self { name, descending, span }
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

/// CREATE DICTIONARY
#[derive(Debug, Clone, Copy)]
pub struct CreateDictionary<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub value_type: &'bump str,
	pub id_type: &'bump str,
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateDictionary<'bump> {
	/// Create a new CREATE DICTIONARY statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		value_type: &'bump str,
		id_type: &'bump str,
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			value_type,
			id_type,
			if_not_exists,
			span,
		}
	}
}

/// CREATE RINGBUFFER
#[derive(Debug, Clone, Copy)]
pub struct CreateRingBuffer<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub columns: &'bump [ColumnDef<'bump>],
	pub capacity: u64,
	pub if_not_exists: bool,
	pub span: Span,
}

impl<'bump> CreateRingBuffer<'bump> {
	/// Create a new CREATE RINGBUFFER statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		columns: &'bump [ColumnDef<'bump>],
		capacity: u64,
		if_not_exists: bool,
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			columns,
			capacity,
			if_not_exists,
			span,
		}
	}
}

/// CREATE SERIES
#[derive(Debug, Clone, Copy)]
pub struct CreateSeries<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub columns: &'bump [ColumnDef<'bump>],
	pub span: Span,
}

impl<'bump> CreateSeries<'bump> {
	/// Create a new CREATE SERIES statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		columns: &'bump [ColumnDef<'bump>],
		span: Span,
	) -> Self {
		Self {
			namespace,
			name,
			columns,
			span,
		}
	}
}

/// CREATE SUBSCRIPTION
#[derive(Debug, Clone, Copy)]
pub struct CreateSubscription<'bump> {
	pub columns: &'bump [ColumnDef<'bump>],
	pub query: Option<&'bump [Expr<'bump>]>,
	pub span: Span,
}

impl<'bump> CreateSubscription<'bump> {
	/// Create a new CREATE SUBSCRIPTION statement.
	pub fn new(
		columns: &'bump [ColumnDef<'bump>],
		query: Option<&'bump [Expr<'bump>]>,
		span: Span,
	) -> Self {
		Self {
			columns,
			query,
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
	/// ALTER VIEW
	View(AlterView<'bump>),
	/// ALTER FLOW
	Flow(AlterFlow<'bump>),
}

impl<'bump> AlterStmt<'bump> {
	/// Get the span of this ALTER statement.
	pub fn span(&self) -> Span {
		match self {
			AlterStmt::Table(t) => t.span,
			AlterStmt::Sequence(s) => s.span,
			AlterStmt::View(v) => v.span,
			AlterStmt::Flow(f) => f.span,
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

/// ALTER VIEW
#[derive(Debug, Clone, Copy)]
pub struct AlterView<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub action: AlterViewAction<'bump>,
	pub span: Span,
}

impl<'bump> AlterView<'bump> {
	/// Create a new ALTER VIEW statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		action: AlterViewAction<'bump>,
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

/// ALTER VIEW action.
#[derive(Debug, Clone, Copy)]
pub enum AlterViewAction<'bump> {
	/// CREATE PRIMARY KEY { columns }
	CreatePrimaryKey(&'bump [&'bump str]),
	/// DROP PRIMARY KEY
	DropPrimaryKey,
}

/// ALTER FLOW
#[derive(Debug, Clone, Copy)]
pub struct AlterFlow<'bump> {
	pub namespace: Option<&'bump str>,
	pub name: &'bump str,
	pub action: AlterFlowAction<'bump>,
	pub span: Span,
}

impl<'bump> AlterFlow<'bump> {
	/// Create a new ALTER FLOW statement.
	pub fn new(
		namespace: Option<&'bump str>,
		name: &'bump str,
		action: AlterFlowAction<'bump>,
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

/// ALTER FLOW action.
#[derive(Debug, Clone, Copy)]
pub enum AlterFlowAction<'bump> {
	/// RENAME TO new_name
	RenameTo(&'bump str),
	/// SET QUERY AS { pipeline }
	SetQuery(&'bump [Expr<'bump>]),
	/// PAUSE
	Pause,
	/// RESUME
	Resume,
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
	Dictionary,
	RingBuffer,
}
