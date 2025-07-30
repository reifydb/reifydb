// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::{Ast, AstInfix, AstLiteral, InfixOperator, parse};
use crate::{ast, convert_data_type};

mod layout;
mod span;

use reifydb_core::{OwnedSpan, Type};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyedExpression {
    pub key: IdentExpression,
    pub expression: Box<Expression>,
}

impl Display for KeyedExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.key, f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    Between(BetweenExpression),

    And(AndExpression),

    Or(OrExpression),

    Xor(XorExpression),

    Type(DataTypeExpression),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessTableExpression {
    pub table: OwnedSpan,
    pub column: OwnedSpan,
}

impl AccessTableExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.table.clone(), self.column.clone()])
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MulExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BetweenExpression {
    pub value: Box<Expression>,
    pub lower: Box<Expression>,
    pub upper: Box<Expression>,
    pub span: OwnedSpan,
}

impl BetweenExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([
            self.value.span(),
            self.span.clone(),
            self.lower.span(),
            self.upper.span(),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AndExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl AndExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl OrExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub span: OwnedSpan,
}

impl XorExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.left.span(), self.span.clone(), self.right.span()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            Expression::Between(BetweenExpression { value, lower, upper, .. }) => {
                write!(f, "({} BETWEEN {} AND {})", value, lower, upper)
            }
            Expression::And(AndExpression { left, right, .. }) => {
                write!(f, "({} and {})", left, right)
            }
            Expression::Or(OrExpression { left, right, .. }) => {
                write!(f, "({} or {})", left, right)
            }
            Expression::Xor(XorExpression { left, right, .. }) => {
                write!(f, "({} xor {})", left, right)
            }
            Expression::Type(DataTypeExpression { span, .. }) => write!(f, "{}", span.fragment),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrefixOperator {
    Minus(OwnedSpan),
    Plus(OwnedSpan),
    Not(OwnedSpan),
}

impl PrefixOperator {
    pub fn span(&self) -> OwnedSpan {
        match self {
            PrefixOperator::Minus(span) => span.clone(),
            PrefixOperator::Plus(span) => span.clone(),
            PrefixOperator::Not(span) => span.clone(),
        }
    }
}

impl Display for PrefixOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrefixOperator::Minus(_) => write!(f, "-"),
            PrefixOperator::Plus(_) => write!(f, "+"),
            PrefixOperator::Not(_) => write!(f, "not"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixExpression {
    pub operator: PrefixOperator,
    pub expression: Box<Expression>,
    pub span: OwnedSpan,
}

impl PrefixExpression {
    pub fn span(&self) -> OwnedSpan {
        OwnedSpan::merge_all([self.operator.span(), self.expression.span()])
    }
}

impl Display for PrefixExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}{})", self.operator, self.expression)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub fn parse_expression(rql: &str) -> crate::Result<Vec<Expression>> {
    let statements = parse(rql)?;
    if statements.is_empty() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();
    for statement in statements {
        for ast in statement.0 {
            result.push(ExpressionCompiler::compile(ast)?);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_call_expression_compilation() {
        let result = parse_expression("func()").unwrap();
        assert_eq!(result.len(), 1);

        if let Expression::Call(call) = &result[0] {
            assert_eq!(call.func.0.fragment, "func");
            assert_eq!(call.args.len(), 0);
        } else {
            panic!("Expected Call expression");
        }
    }

    #[test]
    fn test_namespaced_function_call_expression_compilation() {
        let result = parse_expression("blob::hex('deadbeef')").unwrap();
        assert_eq!(result.len(), 1);

        if let Expression::Call(call) = &result[0] {
            assert_eq!(call.func.0.fragment, "blob::hex");
            assert_eq!(call.args.len(), 1);
        } else {
            panic!("Expected Call expression");
        }
    }

    #[test]
    fn test_deeply_nested_function_call_expression_compilation() {
        let result = parse_expression("ext::crypto::hash::sha256('data')").unwrap();
        assert_eq!(result.len(), 1);

        if let Expression::Call(call) = &result[0] {
            assert_eq!(call.func.0.fragment, "ext::crypto::hash::sha256");
            assert_eq!(call.args.len(), 1);
        } else {
            panic!("Expected Call expression");
        }
    }

    #[test]
    fn test_blob_constructor_end_to_end() {
        // Test all BLOB constructor expressions compile correctly
        let test_cases = vec![
            ("blob::hex('deadbeef')", "blob::hex"),
            ("blob::b64('SGVsbG8=')", "blob::b64"),
            ("blob::b64url('SGVsbG8')", "blob::b64url"),
            ("blob::utf8('Hello, World!')", "blob::utf8"),
        ];

        for (input, expected_func_name) in test_cases {
            let result = parse_expression(input).unwrap();
            assert_eq!(result.len(), 1, "Failed for input: {}", input);

            if let Expression::Call(call) = &result[0] {
                assert_eq!(
                    call.func.0.fragment, expected_func_name,
                    "Function name mismatch for: {}",
                    input
                );
                assert_eq!(call.args.len(), 1, "Argument count mismatch for: {}", input);

                // Verify the argument is a text constant
                if let Expression::Constant(const_expr) = &call.args[0] {
                    match const_expr {
                        ConstantExpression::Text { .. } => {
                            // Expected - this is a text constant
                        }
                        _ => panic!("Expected text constant argument for: {}", input),
                    }
                } else {
                    panic!("Expected constant expression argument for: {}", input);
                }
            } else {
                panic!("Expected Call expression for: {}", input);
            }
        }
    }
}

pub struct ExpressionCompiler {}

impl ExpressionCompiler {
    pub fn compile(ast: Ast) -> crate::Result<Expression> {
        match ast {
            Ast::Literal(literal) => match literal {
                AstLiteral::Boolean(_) => {
                    Ok(Expression::Constant(ConstantExpression::Bool { span: literal.span() }))
                }
                AstLiteral::Number(_) => {
                    Ok(Expression::Constant(ConstantExpression::Number { span: literal.span() }))
                }
                AstLiteral::Temporal(_) => {
                    Ok(Expression::Constant(ConstantExpression::Temporal { span: literal.span() }))
                }
                AstLiteral::Text(_) => {
                    Ok(Expression::Constant(ConstantExpression::Text { span: literal.span() }))
                }
                AstLiteral::Undefined(_) => {
                    Ok(Expression::Constant(ConstantExpression::Undefined { span: literal.span() }))
                }
            },
            Ast::Identifier(identifier) => {
                Ok(Expression::Column(ColumnExpression(identifier.span())))
            }
            Ast::CallFunction(call) => {
                // Build the full function name from namespace + function
                let full_name = if call.namespaces.is_empty() {
                    call.function.value().to_string()
                } else {
                    let namespace_path =
                        call.namespaces.iter().map(|id| id.value()).collect::<Vec<_>>().join("::");
                    format!("{}::{}", namespace_path, call.function.value())
                };

                // Compile arguments
                let mut arg_expressions = Vec::new();
                for arg_ast in call.arguments.nodes {
                    arg_expressions.push(Self::compile(arg_ast)?);
                }

                Ok(Expression::Call(CallExpression {
                    func: IdentExpression(reifydb_core::OwnedSpan::testing(&full_name)),
                    args: arg_expressions,
                    span: call.token.span,
                }))
            }
            Ast::Infix(ast) => Self::infix(ast),
            Ast::Between(between) => {
                let value = Self::compile(*between.value)?;
                let lower = Self::compile(*between.lower)?;
                let upper = Self::compile(*between.upper)?;

                Ok(Expression::Between(BetweenExpression {
                    value: Box::new(value),
                    lower: Box::new(lower),
                    upper: Box::new(upper),
                    span: between.token.span,
                }))
            }
            Ast::Tuple(tuple) => {
                let mut expressions = Vec::with_capacity(tuple.len());

                for ast in tuple.nodes {
                    expressions.push(Self::compile(ast)?);
                }

                Ok(Expression::Tuple(TupleExpression { expressions, span: tuple.token.span }))
            }
            Ast::Prefix(prefix) => {
                let (span, operator) = match prefix.operator {
                    ast::AstPrefixOperator::Plus(token) => {
                        (token.span.clone(), PrefixOperator::Plus(token.span))
                    }
                    ast::AstPrefixOperator::Negate(token) => {
                        (token.span.clone(), PrefixOperator::Minus(token.span))
                    }
                    ast::AstPrefixOperator::Not(token) => {
                        (token.span.clone(), PrefixOperator::Not(token.span))
                    }
                };

                Ok(Expression::Prefix(PrefixExpression {
                    operator,
                    expression: Box::new(Self::compile(*prefix.node)?),
                    span,
                }))
            }
            Ast::Cast(node) => {
                let mut tuple = node.tuple;
                let node = tuple.nodes.pop().unwrap();
                let node = node.as_identifier();
                let span = node.span.clone();
                let ty = convert_data_type(node)?;

                let expr = tuple.nodes.pop().unwrap();

                Ok(Expression::Cast(CastExpression {
                    span: tuple.token.span,
                    expression: Box::new(Self::compile(expr)?),
                    to: DataTypeExpression { span, ty },
                }))
            }
            ast => unimplemented!("{:?}", ast),
        }
    }

    fn infix(ast: AstInfix) -> crate::Result<Expression> {
        match ast.operator {
            InfixOperator::AccessTable(_) => {
                let Ast::Identifier(left) = *ast.left else { unimplemented!() };
                let Ast::Identifier(right) = *ast.right else { unimplemented!() };

                Ok(Expression::AccessTable(AccessTableExpression {
                    table: left.span(),
                    column: right.span(),
                }))
            }

            InfixOperator::Add(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;
                Ok(Expression::Add(AddExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Divide(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;
                Ok(Expression::Div(DivExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Subtract(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;
                Ok(Expression::Sub(SubExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Rem(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;
                Ok(Expression::Rem(RemExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Multiply(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;
                Ok(Expression::Mul(MulExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Call(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                let Expression::Column(ColumnExpression(span)) = left else { panic!() };
                let Expression::Tuple(tuple) = right else { panic!() };

                Ok(Expression::Call(CallExpression {
                    func: IdentExpression(span),
                    args: tuple.expressions,
                    span: token.span,
                }))
            }
            InfixOperator::GreaterThan(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::GreaterThan(GreaterThanExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::GreaterThanEqual(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::GreaterThanEqual(GreaterThanEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::LessThan(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::LessThan(LessThanExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::LessThanEqual(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::LessThanEqual(LessThanEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::Equal(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::Equal(EqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::NotEqual(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::NotEqual(NotEqualExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }
            InfixOperator::As(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::Alias(AliasExpression {
                    alias: IdentExpression(right.span()),
                    expression: Box::new(left),
                    span: token.span,
                }))
            }

            InfixOperator::And(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::And(AndExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }

            InfixOperator::Or(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::Or(OrExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }

            InfixOperator::Xor(token) => {
                let left = Self::compile(*ast.left)?;
                let right = Self::compile(*ast.right)?;

                Ok(Expression::Xor(XorExpression {
                    left: Box::new(left),
                    right: Box::new(right),
                    span: token.span,
                }))
            }

            operator => unimplemented!("not implemented: {operator:?}"),
            // InfixOperator::Arrow(_) => {}
            // InfixOperator::AccessPackage(_) => {}
            // InfixOperator::Assign(_) => {}
            // InfixOperator::Subtract(_) => {}
            // InfixOperator::Multiply(_) => {}
            // InfixOperator::Divide(_) => {}
            // InfixOperator::Rem(_) => {}
            // InfixOperator::TypeAscription(_) => {}
        }
    }
}
