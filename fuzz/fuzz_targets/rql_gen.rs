// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use arbitrary::{Arbitrary, Unstructured};
use std::cell::Cell;
use std::fmt;

// --- Depth-limited Arbitrary infrastructure ---

thread_local! {
    static ARBITRARY_DEPTH: Cell<usize> = const { Cell::new(0) };
}

const ARB_MAX_DEPTH: usize = 4;

struct DepthGuard;

impl DepthGuard {
    fn new() -> Self {
        ARBITRARY_DEPTH.with(|d| d.set(d.get() + 1));
        DepthGuard
    }
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        ARBITRARY_DEPTH.with(|d| d.set(d.get() - 1));
    }
}

fn current_depth() -> usize {
    ARBITRARY_DEPTH.with(|d| d.get())
}

fn short_vec<'a, T: Arbitrary<'a>>(u: &mut Unstructured<'a>, max: usize) -> arbitrary::Result<Vec<T>> {
    let len = u.int_in_range(0..=(max as u32))? as usize;
    (0..len).map(|_| T::arbitrary(u)).collect()
}

pub struct LimitedWriter {
    pub buf: String,
    limit: usize,
}

impl LimitedWriter {
    pub fn new(limit: usize) -> Self {
        Self { buf: String::with_capacity(1024), limit }
    }
}

impl fmt::Write for LimitedWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if self.buf.len() + s.len() > self.limit {
            return Err(fmt::Error);
        }
        self.buf.push_str(s);
        Ok(())
    }
}

#[derive(Debug)]
pub struct RqlInput {
    pub stmts: Vec<RqlStatement>,
}

impl<'a> Arbitrary<'a> for RqlInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(RqlInput { stmts: short_vec(u, 8)? })
    }
}

impl fmt::Display for RqlInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, s) in self.stmts.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{s}")?;
        }
        Ok(())
    }
}
#[derive(Debug, Arbitrary)]
pub enum RqlStatement {
    Query(RqlQuery),
    Dml(RqlDml),
    Ddl(RqlDdl),
    ControlFlow(RqlControlFlow),
    LetBinding(RqlLetBinding),
    Expression(RqlExpr),
    EdgeCase(RqlEdgeCase),
}

impl fmt::Display for RqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(q) => write!(f, "{q}"),
            Self::Dml(d) => write!(f, "{d}"),
            Self::Ddl(d) => write!(f, "{d}"),
            Self::ControlFlow(c) => write!(f, "{c}"),
            Self::LetBinding(l) => write!(f, "{l}"),
            Self::Expression(e) => write_expr(f, e, 0),
            Self::EdgeCase(e) => write!(f, "{e}"),
        }
    }
}
#[derive(Debug)]
pub enum RqlQuery {
    From(RqlTableRef),
    FromInline(Vec<RqlInlineRow>),
    FromVar(RqlVarName),
    Filter(RqlExpr),
    Map(Vec<RqlMapItem>),
    Sort(Vec<RqlSortItem>),
    Take { count: u16, offset: Option<u16> },
    Distinct,
    Aggregate(Vec<RqlAggItem>),
    Extend(Vec<RqlMapItem>),
    Join(RqlJoin),
    Pipeline(Vec<RqlQuery>),
}

impl<'a> Arbitrary<'a> for RqlQuery {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let _guard = DepthGuard::new();
        let depth = current_depth();

        if depth >= ARB_MAX_DEPTH {
            // Only non-recursive leaf variants to prevent unbounded recursion
            match u.int_in_range(0..=3u32)? {
                0 => Ok(RqlQuery::From(RqlTableRef::arbitrary(u)?)),
                1 => Ok(RqlQuery::FromVar(RqlVarName::arbitrary(u)?)),
                2 => Ok(RqlQuery::Distinct),
                _ => Ok(RqlQuery::Take {
                    count: u16::arbitrary(u)?,
                    offset: Option::<u16>::arbitrary(u)?,
                }),
            }
        } else {
            match u.int_in_range(0..=11u32)? {
                0 => Ok(RqlQuery::From(RqlTableRef::arbitrary(u)?)),
                1 => Ok(RqlQuery::FromInline(short_vec(u, 4)?)),
                2 => Ok(RqlQuery::FromVar(RqlVarName::arbitrary(u)?)),
                3 => Ok(RqlQuery::Filter(RqlExpr::arbitrary(u)?)),
                4 => Ok(RqlQuery::Map(short_vec(u, 4)?)),
                5 => Ok(RqlQuery::Sort(short_vec(u, 4)?)),
                6 => Ok(RqlQuery::Take {
                    count: u16::arbitrary(u)?,
                    offset: Option::<u16>::arbitrary(u)?,
                }),
                7 => Ok(RqlQuery::Distinct),
                8 => Ok(RqlQuery::Aggregate(short_vec(u, 4)?)),
                9 => Ok(RqlQuery::Extend(short_vec(u, 4)?)),
                10 => Ok(RqlQuery::Join(RqlJoin::arbitrary(u)?)),
                _ => Ok(RqlQuery::Pipeline(short_vec(u, 4)?)),
            }
        }
    }
}

impl fmt::Display for RqlQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::From(t) => write!(f, "FROM {t}"),
            Self::FromInline(rows) => {
                f.write_str("FROM [")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{row}")?;
                }
                f.write_str("]")
            }
            Self::FromVar(v) => write!(f, "FROM {v}"),
            Self::Filter(e) => {
                f.write_str("FILTER ")?;
                write_expr(f, e, 0)
            }
            Self::Map(items) => {
                f.write_str("MAP ")?;
                write_comma_sep(f, items)
            }
            Self::Sort(items) => {
                f.write_str("SORT ")?;
                write_comma_sep(f, items)
            }
            Self::Take { count, offset } => {
                write!(f, "TAKE {count}")?;
                if let Some(off) = offset {
                    write!(f, " OFFSET {off}")?;
                }
                Ok(())
            }
            Self::Distinct => f.write_str("DISTINCT"),
            Self::Aggregate(items) => {
                f.write_str("AGGREGATE ")?;
                write_comma_sep(f, items)
            }
            Self::Extend(items) => {
                f.write_str("EXTEND ")?;
                write_comma_sep(f, items)
            }
            Self::Join(j) => write!(f, "{j}"),
            Self::Pipeline(stages) => {
                for (i, s) in stages.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" | ")?;
                    }
                    write!(f, "{s}")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct RqlInlineRow {
    pub fields: Vec<(RqlIdent, RqlExpr)>,
}

impl<'a> Arbitrary<'a> for RqlInlineRow {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(RqlInlineRow { fields: short_vec(u, 4)? })
    }
}

impl fmt::Display for RqlInlineRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, (k, v)) in self.fields.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{k}: ")?;
            write_expr(f, v, 0)?;
        }
        f.write_str("}")
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlMapItem {
    pub expr: RqlExpr,
    pub alias: Option<RqlIdent>,
}

impl fmt::Display for RqlMapItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_expr(f, &self.expr, 0)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlSortItem {
    pub expr: RqlExpr,
    pub desc: bool,
}

impl fmt::Display for RqlSortItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_expr(f, &self.expr, 0)?;
        if self.desc {
            f.write_str(" DESC")?;
        }
        Ok(())
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlAggItem {
    pub func: RqlAggFunc,
    pub expr: RqlExpr,
    pub alias: Option<RqlIdent>,
}

impl fmt::Display for RqlAggItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.func)?;
        f.write_str("(")?;
        write_expr(f, &self.expr, 0)?;
        f.write_str(")")?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Arbitrary)]
pub enum RqlAggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl fmt::Display for RqlAggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        })
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlJoin {
    pub kind: RqlJoinKind,
    pub table: RqlTableRef,
    pub on: Option<RqlExpr>,
}

impl fmt::Display for RqlJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} JOIN {}", self.kind, self.table)?;
        if let Some(on) = &self.on {
            f.write_str(" ON ")?;
            write_expr(f, on, 0)?;
        }
        Ok(())
    }
}

#[derive(Debug, Arbitrary)]
pub enum RqlJoinKind {
    Inner,
    Left,
    Natural,
}

impl fmt::Display for RqlJoinKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Inner => "INNER",
            Self::Left => "LEFT",
            Self::Natural => "NATURAL",
        })
    }
}
#[derive(Debug)]
pub enum RqlDml {
    Insert {
        table: RqlTableRef,
        rows: Vec<RqlInlineRow>,
    },
    Update {
        table: RqlTableRef,
        sets: Vec<(RqlIdent, RqlExpr)>,
        filter: Option<RqlExpr>,
    },
    Delete {
        table: RqlTableRef,
        filter: Option<RqlExpr>,
    },
}

impl<'a> Arbitrary<'a> for RqlDml {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        match u.int_in_range(0..=2u32)? {
            0 => Ok(RqlDml::Insert {
                table: RqlTableRef::arbitrary(u)?,
                rows: short_vec(u, 4)?,
            }),
            1 => Ok(RqlDml::Update {
                table: RqlTableRef::arbitrary(u)?,
                sets: short_vec(u, 4)?,
                filter: if bool::arbitrary(u)? { Some(RqlExpr::arbitrary(u)?) } else { None },
            }),
            _ => Ok(RqlDml::Delete {
                table: RqlTableRef::arbitrary(u)?,
                filter: if bool::arbitrary(u)? { Some(RqlExpr::arbitrary(u)?) } else { None },
            }),
        }
    }
}

impl fmt::Display for RqlDml {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert { table, rows } => {
                write!(f, "INSERT INTO {table} [")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{row}")?;
                }
                f.write_str("]")
            }
            Self::Update { table, sets, filter } => {
                write!(f, "UPDATE {table} SET ")?;
                for (i, (k, v)) in sets.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{k} = ")?;
                    write_expr(f, v, 0)?;
                }
                if let Some(filt) = filter {
                    f.write_str(" WHERE ")?;
                    write_expr(f, filt, 0)?;
                }
                Ok(())
            }
            Self::Delete { table, filter } => {
                write!(f, "DELETE {table}")?;
                if let Some(filt) = filter {
                    f.write_str(" WHERE ")?;
                    write_expr(f, filt, 0)?;
                }
                Ok(())
            }
        }
    }
}
#[derive(Debug, Arbitrary)]
pub enum RqlDdl {
    CreateTable {
        table: RqlTableRef,
        columns: Vec<RqlColumnDef>,
    },
    CreateNamespace(RqlIdent),
    DropTable(RqlTableRef),
    DropNamespace(RqlIdent),
    AlterTableAddColumn {
        table: RqlTableRef,
        column: RqlColumnDef,
    },
    AlterTableRenameColumn {
        table: RqlTableRef,
        from: RqlIdent,
        to: RqlIdent,
    },
    CreateIndex {
        unique: bool,
        table: RqlTableRef,
        columns: Vec<RqlIdent>,
    },
}

impl fmt::Display for RqlDdl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTable { table, columns } => {
                write!(f, "CREATE TABLE {table} (")?;
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{col}")?;
                }
                f.write_str(")")
            }
            Self::CreateNamespace(name) => write!(f, "CREATE NAMESPACE {name}"),
            Self::DropTable(t) => write!(f, "DROP TABLE {t}"),
            Self::DropNamespace(n) => write!(f, "DROP NAMESPACE {n}"),
            Self::AlterTableAddColumn { table, column } => {
                write!(f, "ALTER TABLE {table} ADD COLUMN {column}")
            }
            Self::AlterTableRenameColumn { table, from, to } => {
                write!(f, "ALTER TABLE {table} RENAME {from} TO {to}")
            }
            Self::CreateIndex { unique, table, columns } => {
                f.write_str("CREATE ")?;
                if *unique {
                    f.write_str("UNIQUE ")?;
                }
                write!(f, "INDEX ON {table} (")?;
                write_comma_sep(f, columns)?;
                f.write_str(")")
            }
        }
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlColumnDef {
    pub name: RqlIdent,
    pub ty: RqlType,
}

impl fmt::Display for RqlColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.ty)
    }
}
#[derive(Debug)]
pub enum RqlControlFlow {
    If {
        cond: RqlExpr,
        then_body: Vec<RqlStatement>,
        else_body: Option<Vec<RqlStatement>>,
    },
    While {
        cond: RqlExpr,
        body: Vec<RqlStatement>,
    },
    Loop {
        body: Vec<RqlStatement>,
    },
    For {
        var: RqlVarName,
        iter: RqlExpr,
        body: Vec<RqlStatement>,
    },
    Break,
    Continue,
    Return(Option<RqlExpr>),
}

impl<'a> Arbitrary<'a> for RqlControlFlow {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let _guard = DepthGuard::new();
        let depth = current_depth();

        if depth >= ARB_MAX_DEPTH {
            // Only leaf variants to prevent unbounded recursion
            match u.int_in_range(0..=2u32)? {
                0 => Ok(RqlControlFlow::Break),
                1 => Ok(RqlControlFlow::Continue),
                _ => Ok(RqlControlFlow::Return(None)),
            }
        } else {
            match u.int_in_range(0..=6u32)? {
                0 => Ok(RqlControlFlow::If {
                    cond: RqlExpr::arbitrary(u)?,
                    then_body: short_vec(u, 4)?,
                    else_body: if bool::arbitrary(u)? { Some(short_vec(u, 4)?) } else { None },
                }),
                1 => Ok(RqlControlFlow::While {
                    cond: RqlExpr::arbitrary(u)?,
                    body: short_vec(u, 4)?,
                }),
                2 => Ok(RqlControlFlow::Loop {
                    body: short_vec(u, 4)?,
                }),
                3 => Ok(RqlControlFlow::For {
                    var: RqlVarName::arbitrary(u)?,
                    iter: RqlExpr::arbitrary(u)?,
                    body: short_vec(u, 4)?,
                }),
                4 => Ok(RqlControlFlow::Break),
                5 => Ok(RqlControlFlow::Continue),
                _ => Ok(RqlControlFlow::Return(
                    if bool::arbitrary(u)? { Some(RqlExpr::arbitrary(u)?) } else { None },
                )),
            }
        }
    }
}

impl fmt::Display for RqlControlFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::If { cond, then_body, else_body } => {
                f.write_str("IF ")?;
                write_expr(f, cond, 0)?;
                f.write_str(" { ")?;
                write_stmts(f, then_body)?;
                f.write_str(" }")?;
                if let Some(els) = else_body {
                    f.write_str(" ELSE { ")?;
                    write_stmts(f, els)?;
                    f.write_str(" }")?;
                }
                Ok(())
            }
            Self::While { cond, body } => {
                f.write_str("WHILE ")?;
                write_expr(f, cond, 0)?;
                f.write_str(" { ")?;
                write_stmts(f, body)?;
                f.write_str(" }")
            }
            Self::Loop { body } => {
                f.write_str("LOOP { ")?;
                write_stmts(f, body)?;
                f.write_str(" }")
            }
            Self::For { var, iter, body } => {
                write!(f, "FOR {var} IN ")?;
                write_expr(f, iter, 0)?;
                f.write_str(" { ")?;
                write_stmts(f, body)?;
                f.write_str(" }")
            }
            Self::Break => f.write_str("BREAK"),
            Self::Continue => f.write_str("CONTINUE"),
            Self::Return(e) => {
                f.write_str("RETURN")?;
                if let Some(e) = e {
                    f.write_str(" ")?;
                    write_expr(f, e, 0)?;
                }
                Ok(())
            }
        }
    }
}
#[derive(Debug, Arbitrary)]
pub struct RqlLetBinding {
    pub var: RqlVarName,
    pub value: RqlExpr,
}

impl fmt::Display for RqlLetBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LET {} = ", self.var)?;
        write_expr(f, &self.value, 0)
    }
}
#[derive(Debug, Arbitrary)]
pub enum RqlEdgeCase {
    DeepParens(Box<RqlExpr>),
    EmptyBraces,
    OperatorSoup(Vec<RqlBinaryOp>),
    UnicodeIdent(u8),
    EmptyInput,
    ManyCommas(u8),
    TrailingSemicolons(u8),
    CommentLine,
    BacktickIdent(RqlIdent),
}

impl fmt::Display for RqlEdgeCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeepParens(e) => {
                f.write_str("((((((")?;
                write_expr(f, e, 0)?;
                f.write_str("))))))")
            }
            Self::EmptyBraces => f.write_str("{}"),
            Self::OperatorSoup(ops) => {
                f.write_str("1")?;
                for op in ops.iter().take(8) {
                    write!(f, " {op} 1")?;
                }
                Ok(())
            }
            Self::UnicodeIdent(n) => {
                let names = ["café", "naïve", "über", "résumé"];
                let name = names[(*n as usize) % names.len()];
                write!(f, "FROM {name}")
            }
            Self::EmptyInput => Ok(()),
            Self::ManyCommas(n) => {
                let count = (*n as usize % 16) + 1;
                f.write_str("MAP ")?;
                for i in 0..count {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str("a")?;
                }
                Ok(())
            }
            Self::TrailingSemicolons(n) => {
                let count = (*n as usize % 8) + 1;
                f.write_str("FROM a")?;
                for _ in 0..count {
                    f.write_str(";")?;
                }
                Ok(())
            }
            Self::CommentLine => f.write_str("# this is a comment\nFROM a"),
            Self::BacktickIdent(id) => write!(f, "FROM `{id}`"),
        }
    }
}
#[derive(Debug)]
pub enum RqlExpr {
    Literal(RqlLiteral),
    Ident(RqlIdent),
    Variable(RqlVarName),
    BinaryOp {
        left: Box<RqlExpr>,
        op: RqlBinaryOp,
        right: Box<RqlExpr>,
    },
    UnaryOp {
        op: RqlUnaryOp,
        operand: Box<RqlExpr>,
    },
    FunctionCall {
        name: RqlIdent,
        args: Vec<RqlExpr>,
    },
    Between {
        expr: Box<RqlExpr>,
        low: Box<RqlExpr>,
        high: Box<RqlExpr>,
    },
    InList {
        expr: Box<RqlExpr>,
        list: Vec<RqlExpr>,
        negated: bool,
    },
    Cast {
        expr: Box<RqlExpr>,
        ty: RqlType,
    },
    Paren(Box<RqlExpr>),
    List(Vec<RqlExpr>),
    InlineObject(Vec<(RqlIdent, RqlExpr)>),
    FieldAccess {
        object: Box<RqlExpr>,
        field: RqlIdent,
    },
    Wildcard,
}

impl<'a> Arbitrary<'a> for RqlExpr {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let _guard = DepthGuard::new();
        let depth = current_depth();

        if depth >= ARB_MAX_DEPTH {
            // Only leaf variants to prevent unbounded recursion
            match u.int_in_range(0..=3u32)? {
                0 => Ok(RqlExpr::Literal(RqlLiteral::arbitrary(u)?)),
                1 => Ok(RqlExpr::Ident(RqlIdent::arbitrary(u)?)),
                2 => Ok(RqlExpr::Variable(RqlVarName::arbitrary(u)?)),
                _ => Ok(RqlExpr::Wildcard),
            }
        } else {
            match u.int_in_range(0..=13u32)? {
                0 => Ok(RqlExpr::Literal(RqlLiteral::arbitrary(u)?)),
                1 => Ok(RqlExpr::Ident(RqlIdent::arbitrary(u)?)),
                2 => Ok(RqlExpr::Variable(RqlVarName::arbitrary(u)?)),
                3 => Ok(RqlExpr::BinaryOp {
                    left: Box::new(RqlExpr::arbitrary(u)?),
                    op: RqlBinaryOp::arbitrary(u)?,
                    right: Box::new(RqlExpr::arbitrary(u)?),
                }),
                4 => Ok(RqlExpr::UnaryOp {
                    op: RqlUnaryOp::arbitrary(u)?,
                    operand: Box::new(RqlExpr::arbitrary(u)?),
                }),
                5 => Ok(RqlExpr::FunctionCall {
                    name: RqlIdent::arbitrary(u)?,
                    args: short_vec(u, 4)?,
                }),
                6 => Ok(RqlExpr::Between {
                    expr: Box::new(RqlExpr::arbitrary(u)?),
                    low: Box::new(RqlExpr::arbitrary(u)?),
                    high: Box::new(RqlExpr::arbitrary(u)?),
                }),
                7 => Ok(RqlExpr::InList {
                    expr: Box::new(RqlExpr::arbitrary(u)?),
                    list: short_vec(u, 4)?,
                    negated: bool::arbitrary(u)?,
                }),
                8 => Ok(RqlExpr::Cast {
                    expr: Box::new(RqlExpr::arbitrary(u)?),
                    ty: RqlType::arbitrary(u)?,
                }),
                9 => Ok(RqlExpr::Paren(Box::new(RqlExpr::arbitrary(u)?))),
                10 => Ok(RqlExpr::List(short_vec(u, 4)?)),
                11 => Ok(RqlExpr::InlineObject(short_vec(u, 4)?)),
                12 => Ok(RqlExpr::FieldAccess {
                    object: Box::new(RqlExpr::arbitrary(u)?),
                    field: RqlIdent::arbitrary(u)?,
                }),
                _ => Ok(RqlExpr::Wildcard),
            }
        }
    }
}

const MAX_DEPTH: usize = 6;

fn write_expr(f: &mut fmt::Formatter<'_>, expr: &RqlExpr, depth: usize) -> fmt::Result {
    if depth > MAX_DEPTH {
        return f.write_str("1");
    }
    let d = depth + 1;
    match expr {
        RqlExpr::Literal(lit) => write!(f, "{lit}"),
        RqlExpr::Ident(id) => write!(f, "{id}"),
        RqlExpr::Variable(v) => write!(f, "{v}"),
        RqlExpr::BinaryOp { left, op, right } => {
            write_expr(f, left, d)?;
            write!(f, " {op} ")?;
            write_expr(f, right, d)
        }
        RqlExpr::UnaryOp { op, operand } => {
            write!(f, "{op}")?;
            write_expr(f, operand, d)
        }
        RqlExpr::FunctionCall { name, args } => {
            write!(f, "{name}(")?;
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write_expr(f, arg, d)?;
            }
            f.write_str(")")
        }
        RqlExpr::Between { expr, low, high } => {
            write_expr(f, expr, d)?;
            f.write_str(" BETWEEN ")?;
            write_expr(f, low, d)?;
            f.write_str(" AND ")?;
            write_expr(f, high, d)
        }
        RqlExpr::InList { expr, list, negated } => {
            write_expr(f, expr, d)?;
            if *negated {
                f.write_str(" NOT")?;
            }
            f.write_str(" IN [")?;
            for (i, item) in list.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write_expr(f, item, d)?;
            }
            f.write_str("]")
        }
        RqlExpr::Cast { expr, ty } => {
            f.write_str("CAST ")?;
            write_expr(f, expr, d)?;
            write!(f, " AS {ty}")
        }
        RqlExpr::Paren(inner) => {
            f.write_str("(")?;
            write_expr(f, inner, d)?;
            f.write_str(")")
        }
        RqlExpr::List(items) => {
            f.write_str("[")?;
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write_expr(f, item, d)?;
            }
            f.write_str("]")
        }
        RqlExpr::InlineObject(fields) => {
            f.write_str("{")?;
            for (i, (k, v)) in fields.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{k}: ")?;
                write_expr(f, v, d)?;
            }
            f.write_str("}")
        }
        RqlExpr::FieldAccess { object, field } => {
            write_expr(f, object, d)?;
            write!(f, ".{field}")
        }
        RqlExpr::Wildcard => f.write_str("*"),
    }
}
#[derive(Debug, Arbitrary)]
pub enum RqlLiteral {
    Int(i64),
    Float(f64),
    Text(RqlText),
    Bool(bool),
    None,
    Hex(u32),
    Binary(u8),
    Date { y: u8, m: u8, d: u8 },
}

impl fmt::Display for RqlLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(v) => {
                if v.is_nan() || v.is_infinite() {
                    f.write_str("0.0")
                } else {
                    write!(f, "{v}")
                }
            }
            Self::Text(t) => write!(f, "{t}"),
            Self::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            Self::None => f.write_str("NONE"),
            Self::Hex(n) => write!(f, "0x{n:X}"),
            Self::Binary(n) => write!(f, "0b{n:b}"),
            Self::Date { y, m, d } => {
                let year = 2000 + (*y as u16 % 100);
                let month = (*m % 12) + 1;
                let day = (*d % 28) + 1;
                write!(f, "@{year}-{month:02}-{day:02}")
            }
        }
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlText {
    choice: u8,
}

impl fmt::Display for RqlText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let texts = [
            "hello",
            "world",
            "",
            "it''s a test",
            "foo bar",
            "line1\\nline2",
            "special chars: @#$%",
            "123",
        ];
        let text = texts[self.choice as usize % texts.len()];
        write!(f, "'{text}'")
    }
}
#[derive(Debug, Arbitrary)]
pub enum RqlBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    Like,
}

impl fmt::Display for RqlBinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Xor => "XOR",
            Self::Like => "LIKE",
        })
    }
}

#[derive(Debug, Arbitrary)]
pub enum RqlUnaryOp {
    Neg,
    Not,
    Plus,
    Bang,
}

impl fmt::Display for RqlUnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Neg => "-",
            Self::Not => "NOT ",
            Self::Plus => "+",
            Self::Bang => "!",
        })
    }
}
#[derive(Debug, Arbitrary)]
pub struct RqlType {
    pub kind: RqlTypeKind,
    pub optional: bool,
}

impl fmt::Display for RqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if self.optional {
            f.write_str("?")?;
        }
        Ok(())
    }
}

#[derive(Debug, Arbitrary)]
pub enum RqlTypeKind {
    Bool,
    Int1,
    Int2,
    Int4,
    Int8,
    Int16,
    Uint1,
    Uint2,
    Uint4,
    Uint8,
    Uint16,
    Float4,
    Float8,
    Decimal,
    Text,
    Blob,
    Date,
    DateTime,
    Time,
    Duration,
    Uuid4,
    Uuid7,
    Any,
}

impl fmt::Display for RqlTypeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Bool => "bool",
            Self::Int1 => "int1",
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Uint1 => "uint1",
            Self::Uint2 => "uint2",
            Self::Uint4 => "uint4",
            Self::Uint8 => "uint8",
            Self::Uint16 => "uint16",
            Self::Float4 => "float4",
            Self::Float8 => "float8",
            Self::Decimal => "decimal",
            Self::Text => "text",
            Self::Blob => "blob",
            Self::Date => "date",
            Self::DateTime => "datetime",
            Self::Time => "time",
            Self::Duration => "duration",
            Self::Uuid4 => "uuid4",
            Self::Uuid7 => "uuid7",
            Self::Any => "any",
        })
    }
}
#[derive(Debug, Arbitrary)]
pub struct RqlIdent {
    choice: u8,
}

const IDENT_POOL: &[&str] = &[
    "a", "b", "c", "x", "y", "z",
    "id", "name", "age", "email", "status", "value",
    "users", "orders", "products", "items", "accounts", "events",
    "created_at", "updated_at", "count", "total", "price", "active",
];

impl fmt::Display for RqlIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = IDENT_POOL[self.choice as usize % IDENT_POOL.len()];
        f.write_str(name)
    }
}

#[derive(Debug, Arbitrary)]
pub struct RqlVarName {
    choice: u8,
}

const VAR_POOL: &[&str] = &[
    "$x", "$y", "$z", "$data", "$result", "$item", "$row", "$val",
    "$count", "$total", "$input", "$output",
];

impl fmt::Display for RqlVarName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = VAR_POOL[self.choice as usize % VAR_POOL.len()];
        f.write_str(name)
    }
}
#[derive(Debug, Arbitrary)]
pub struct RqlTableRef {
    pub namespace: Option<RqlIdent>,
    pub name: RqlIdent,
}

impl fmt::Display for RqlTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ns) = &self.namespace {
            write!(f, "{ns}::")?;
        }
        write!(f, "{}", self.name)
    }
}
fn write_comma_sep<T: fmt::Display>(f: &mut fmt::Formatter<'_>, items: &[T]) -> fmt::Result {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}

fn write_stmts(f: &mut fmt::Formatter<'_>, stmts: &[RqlStatement]) -> fmt::Result {
    for (i, s) in stmts.iter().enumerate() {
        if i > 0 {
            f.write_str("; ")?;
        }
        write!(f, "{s}")?;
    }
    Ok(())
}
