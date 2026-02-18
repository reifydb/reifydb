// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[derive(Debug, Clone)]
pub enum Statement {
	Select(SelectStatement),
	Insert(InsertStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	CreateTable(CreateTableStatement),
}

#[derive(Debug, Clone)]
pub struct CteDefinition {
	pub name: String,
	pub query: SelectStatement,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
	pub ctes: Vec<CteDefinition>,
	pub distinct: bool,
	pub columns: Vec<SelectColumn>,
	pub from: Option<FromClause>,
	pub joins: Vec<JoinClause>,
	pub where_clause: Option<Expr>,
	pub group_by: Vec<Expr>,
	pub having: Option<Expr>,
	pub order_by: Vec<OrderByItem>,
	pub limit: Option<u64>,
	pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
	AllColumns,
	Expr {
		expr: Expr,
		alias: Option<String>,
	},
}

#[derive(Debug, Clone)]
pub enum FromClause {
	Table {
		name: String,
		schema: Option<String>,
	},
	Subquery(Box<SelectStatement>),
}

#[derive(Debug, Clone)]
pub struct JoinClause {
	pub join_type: JoinType,
	pub table: FromClause,
	pub table_alias: Option<String>,
	pub on: Expr,
}

#[derive(Debug, Clone)]
pub enum JoinType {
	Inner,
	Left,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
	pub expr: Expr,
	pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub enum OrderDirection {
	Asc,
	Desc,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
	pub table: String,
	pub schema: Option<String>,
	pub columns: Vec<String>,
	pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
	pub table: String,
	pub schema: Option<String>,
	pub assignments: Vec<(String, Expr)>,
	pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
	pub table: String,
	pub schema: Option<String>,
	pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
	pub table: String,
	pub schema: Option<String>,
	pub columns: Vec<ColumnDef>,
	pub primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
	pub name: String,
	pub data_type: SqlType,
	pub nullable: bool,
}

#[derive(Debug, Clone)]
pub enum SqlType {
	Int,
	Int2,
	Int4,
	Int8,
	Smallint,
	Integer,
	Bigint,
	Float4,
	Float8,
	Real,
	Double,
	Boolean,
	Bool,
	Varchar(Option<u64>),
	Char(Option<u64>),
	Text,
	Utf8,
	Blob,
}

#[derive(Debug, Clone)]
pub enum Expr {
	Identifier(String),
	QualifiedIdentifier(String, String),
	IntegerLiteral(i64),
	FloatLiteral(f64),
	StringLiteral(String),
	BoolLiteral(bool),
	Null,
	BinaryOp {
		left: Box<Expr>,
		op: BinaryOp,
		right: Box<Expr>,
	},
	UnaryOp {
		op: UnaryOp,
		expr: Box<Expr>,
	},
	FunctionCall {
		name: String,
		args: Vec<Expr>,
	},
	Between {
		expr: Box<Expr>,
		low: Box<Expr>,
		high: Box<Expr>,
		negated: bool,
	},
	InList {
		expr: Box<Expr>,
		list: Vec<Expr>,
		negated: bool,
	},
	IsNull {
		expr: Box<Expr>,
		negated: bool,
	},
	Cast {
		expr: Box<Expr>,
		data_type: SqlType,
	},
	Nested(Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
	Eq,
	NotEq,
	Lt,
	Gt,
	LtEq,
	GtEq,
	And,
	Or,
	Add,
	Sub,
	Mul,
	Div,
	Mod,
}

#[derive(Debug, Clone)]
pub enum UnaryOp {
	Not,
	Neg,
}
