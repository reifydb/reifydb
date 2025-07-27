// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod layout;
mod span;

use crate::{OwnedSpan, Type};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct AliasExpression {
    pub alias: IdentExpression,
    pub expression: Box<Expression>,
    pub span: OwnedSpan,
}

impl Display for AliasExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.alias, f)
    }
}

#[derive(Debug, Clone)]
pub struct KeyedExpression {
    pub key: IdentExpression,
    pub expression: Box<Expression>,
}

impl Display for KeyedExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.key, f)
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    AccessTable(AccessTableExpression),

    Alias(AliasExpression),

    Keyed(KeyedExpression),

    Cast(CastExpression),

    Constant(ConstantExpression),

    Column(ColumnExpression),

    Add(AddExpression),

    Div(DivExpression),

    Call(CallExpression),

    Rem(RemExpression),

    Mul(MulExpression),

    Sub(SubExpression),

    Tuple(TupleExpression),

    Prefix(PrefixExpression),

    GreaterThan(GreaterThanExpression),

    GreaterThanEqual(GreaterThanEqualExpression),

    LessThan(LessThanExpression),

    LessThanEqual(LessThanEqualExpression),

    Equal(EqualExpression),

    NotEqual(NotEqualExpression),

    Type(DataTypeExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccessTableExpression {
    pub table: OwnedSpan,
    pub column: OwnedSpan,
}

impl AccessTableExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.table.clone(), self.column.clone()])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstantExpression {
    Undefined { span: OwnedSpan },
    Bool { span: OwnedSpan },
    // any number
    Number { span: OwnedSpan },
    // any textual representation can be String, Text, ...
    Text { span: OwnedSpan },
    // any temporal representation can be Date, Time, DateTime, ...
    Temporal { span: OwnedSpan },
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConstantExpression::Undefined { .. } => write!(f, "undefined"),
            ConstantExpression::Bool { span } => write!(f, "{}", span.fragment),
            ConstantExpression::Number { span } => write!(f, "{}", span.fragment),
            ConstantExpression::Text { span } => write!(f, "\"{}\"", span.fragment),
            ConstantExpression::Temporal { span } => write!(f, "{}", span.fragment),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CastExpression {
    pub span: OwnedSpan,
    pub expression: Box<Expression>,
    pub to: DataTypeExpression,
}

impl CastExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.span.clone(), self.expression.span(), self.to.span()])
    }

    pub fn lazy_span(&self) -> impl Fn() -> OwnedSpan + '_ {
        move || self.span()
    }
}

#[derive(Debug, Clone)]
pub struct DataTypeExpression {
    pub span: OwnedSpan,
    pub ty: Type,
}

impl DataTypeExpression {
    pub fn span(&self) -> OwnedSpan {
        self.span.clone()
    }

    pub fn lazy_span(&self) -> impl Fn() -> OwnedSpan + '_ {
        move || self.span()
    }
}

#[derive(Debug, Clone)]
pub struct AddExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct DivExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct SubExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct RemExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct MulExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone)]
pub struct GreaterThanExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl GreaterThanExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone)]
pub struct GreaterThanEqualExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl GreaterThanEqualExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone)]
pub struct LessThanExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl LessThanExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone)]
pub struct LessThanEqualExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl LessThanEqualExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone)]
pub struct EqualExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl EqualExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone)]
pub struct NotEqualExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl NotEqualExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone)]
pub struct ColumnExpression(pub OwnedSpan);

impl ColumnExpression {
    pub fn span(&self) -> OwnedSpan {
        self.0.clone()
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::AccessTable(AccessTableExpression { table: target, column: property }) => {
                write!(f, "{}.{}", target.fragment, property.fragment)
            }
            Expression::Alias(AliasExpression { alias, expression, .. }) => {
                write!(f, "{} as {}", expression, alias)
            }
            Expression::Keyed(KeyedExpression { key, expression, .. }) => {
                write!(f, "{}: {}", key, expression)
            }
            Expression::Cast(CastExpression { expression: expr, .. }) => write!(f, "{}", expr),
            Expression::Constant(span) => write!(f, "Constant({})", span),
            Expression::Column(ColumnExpression(span)) => write!(f, "Column({})", span.fragment),
            Expression::Add(AddExpression { left, right, .. }) => {
                write!(f, "({} + {})", left, right)
            }
            Expression::Div(DivExpression { left, right, .. }) => {
                write!(f, "({} / {})", left, right)
            }
            Expression::Call(call) => write!(f, "{}", call),
            Expression::Rem(RemExpression { left, right, .. }) => {
                write!(f, "({} % {})", left, right)
            }
            Expression::Mul(MulExpression { left, right, .. }) => {
                write!(f, "({} * {})", left, right)
            }
            Expression::Sub(SubExpression { left, right, .. }) => {
                write!(f, "({} - {})", left, right)
            }
            Expression::Tuple(tuple) => write!(f, "({})", tuple),
            Expression::Prefix(prefix) => write!(f, "{}", prefix),
            Expression::GreaterThan(GreaterThanExpression { left, right, .. }) => {
                write!(f, "({} > {})", left, right)
            }
            Expression::GreaterThanEqual(GreaterThanEqualExpression { left, right, .. }) => {
                write!(f, "({} >= {})", left, right)
            }
            Expression::LessThan(LessThanExpression { left, right, .. }) => {
                write!(f, "({} < {})", left, right)
            }
            Expression::LessThanEqual(LessThanEqualExpression { left, right, .. }) => {
                write!(f, "({} <= {})", left, right)
            }
            Expression::Equal(EqualExpression { left, right, .. }) => {
                write!(f, "({} == {})", left, right)
            }
            Expression::NotEqual(NotEqualExpression { left, right, .. }) => {
                write!(f, "({} != {})", left, right)
            }
            Expression::Type(DataTypeExpression { span, .. }) => write!(f, "{}", span.fragment),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CallExpression {
    pub func: IdentExpression,
    pub args: Vec<Expression>,
    pub span: OwnedSpan,
}

impl CallExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan {
            column: self.func.0.column,
            line: self.func.0.line,
            fragment: format!(
                "{}({})",
                self.func.0.fragment,
                self.args
                    .iter()
                    .map(|arg| arg.span().fragment.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }
}

impl Display for CallExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let args = self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", ");
        write!(f, "{}({})", self.func, args)
    }
}

#[derive(Debug, Clone)]
pub struct IdentExpression(pub OwnedSpan);

impl IdentExpression {
    pub fn name(&self) -> &str {
        &self.0.fragment
    }
}

impl Display for IdentExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.fragment)
    }
}

#[derive(Debug, Clone)]
pub enum PrefixOperator {
    Minus(OwnedSpan),
    Plus(OwnedSpan),
}

impl Display for PrefixOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrefixOperator::Minus(_) => write!(f, "-"),
            PrefixOperator::Plus(_) => write!(f, "+"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrefixExpression {
    pub operator: PrefixOperator,
    pub expression: Box<Expression>,
    pub span: OwnedSpan,
}

impl Display for PrefixExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.operator, self.expression)
    }
}

#[derive(Debug, Clone)]
pub struct TupleExpression {
    pub expressions: Vec<Expression>,
    pub span: OwnedSpan,
}

impl Display for TupleExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let items =
            self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
        write!(f, "({})", items)
    }
}