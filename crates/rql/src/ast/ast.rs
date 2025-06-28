// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Literal, Token, TokenKind};
use reifydb_core::Kind;
use std::ops::Index;

#[derive(Debug)]
pub struct AstStatement(pub Vec<Ast>);

impl AstStatement {
    pub fn first_unchecked(&self) -> &Ast {
        self.0.first().unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Index<usize> for AstStatement {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl IntoIterator for AstStatement {
    type Item = Ast;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, PartialEq)]
pub enum Ast {
    Block(AstBlock),
    Cast(AstCast),
    Create(AstCreate),
    Describe(AstDescribe),
    Filter(AstFilter),
    From(AstFrom),
    AggregateBy(AstAggregateBy),
    Identifier(AstIdentifier),
    Infix(AstInfix),
    Insert(AstInsert),
    Limit(AstLimit),
    Literal(AstLiteral),
    Nop,
    OrderBy(AstOrderBy),
    Policy(AstPolicy),
    PolicyBlock(AstPolicyBlock),
    Prefix(AstPrefix),
    Select(AstSelect),
    Tuple(AstTuple),
    Kind(AstKind),
    Wildcard(AstWildcard),
}

impl Default for Ast {
    fn default() -> Self {
        Self::Nop
    }
}

impl Ast {
    pub fn token(&self) -> &Token {
        match self {
            Ast::Block(node) => &node.token,
            Ast::Cast(node) => &node.token,
            Ast::Create(node) => node.token(),
            Ast::Describe(node) => match node {
                AstDescribe::Query { token, .. } => token,
            },
            Ast::Filter(node) => &node.token,
            Ast::From(node) => node.token(),
            Ast::AggregateBy(node) => &node.token,
            Ast::Identifier(node) => &node.0,
            Ast::Infix(node) => &node.token,
            Ast::Insert(node) => &node.token,
            Ast::Limit(node) => &node.token,
            Ast::Literal(node) => match node {
                AstLiteral::Boolean(node) => &node.0,
                AstLiteral::Number(node) => &node.0,
                AstLiteral::Text(node) => &node.0,
                AstLiteral::Undefined(node) => &node.0,
            },
            Ast::Nop => unreachable!(),
            Ast::OrderBy(node) => &node.token,
            Ast::Policy(node) => &node.token,
            Ast::PolicyBlock(node) => &node.token,
            Ast::Prefix(node) => node.node.token(),
            Ast::Select(node) => &node.token,
            Ast::Tuple(node) => &node.token,
            Ast::Kind(node) => node.token(),
            Ast::Wildcard(node) => &node.0,
        }
    }

    pub fn value(&self) -> &str {
        self.token().value()
    }
}

impl Ast {
    pub fn is_aggregate_by(&self) -> bool {
        matches!(self, Ast::AggregateBy(_))
    }
    pub fn as_aggregate_by(&self) -> &AstAggregateBy {
        if let Ast::AggregateBy(result) = self { result } else { panic!("not aggregate by") }
    }

    pub fn is_block(&self) -> bool {
        matches!(self, Ast::Block(_))
    }
    pub fn as_block(&self) -> &AstBlock {
        if let Ast::Block(result) = self { result } else { panic!("not block") }
    }

    pub fn is_cast(&self) -> bool {
        matches!(self, Ast::Cast(_))
    }
    pub fn as_cast(&self) -> &AstCast {
        if let Ast::Cast(result) = self { result } else { panic!("not cast") }
    }

    pub fn is_create(&self) -> bool {
        matches!(self, Ast::Create(_))
    }
    pub fn as_create(&self) -> &AstCreate {
        if let Ast::Create(result) = self { result } else { panic!("not create") }
    }

    pub fn is_describe(&self) -> bool {
        matches!(self, Ast::Describe(_))
    }
    pub fn as_describe(&self) -> &AstDescribe {
        if let Ast::Describe(result) = self { result } else { panic!("not describe") }
    }

    pub fn is_filter(&self) -> bool {
        matches!(self, Ast::Filter(_))
    }
    pub fn as_filter(&self) -> &AstFilter {
        if let Ast::Filter(result) = self { result } else { panic!("not filter") }
    }

    pub fn is_from(&self) -> bool {
        matches!(self, Ast::From(_))
    }
    pub fn as_from(&self) -> &AstFrom {
        if let Ast::From(result) = self { result } else { panic!("not from") }
    }

    pub fn is_identifier(&self) -> bool {
        matches!(self, Ast::Identifier(_))
    }
    pub fn as_identifier(&self) -> &AstIdentifier {
        if let Ast::Identifier(result) = self { result } else { panic!("not identifier") }
    }

    pub fn is_infix(&self) -> bool {
        matches!(self, Ast::Infix(_))
    }
    pub fn as_infix(&self) -> &AstInfix {
        if let Ast::Infix(result) = self { result } else { panic!("not infix") }
    }

    pub fn is_insert(&self) -> bool {
        matches!(self, Ast::Insert(_))
    }
    pub fn as_insert(&self) -> &AstInsert {
        if let Ast::Insert(result) = self { result } else { panic!("not insert") }
    }

    pub fn is_limit(&self) -> bool {
        matches!(self, Ast::Limit(_))
    }
    pub fn as_limit(&self) -> &AstLimit {
        if let Ast::Limit(result) = self { result } else { panic!("not limit") }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, Ast::Literal(_))
    }

    pub fn as_literal(&self) -> &AstLiteral {
        if let Ast::Literal(result) = self { result } else { panic!("not literal") }
    }

    pub fn is_literal_boolean(&self) -> bool {
        matches!(self, Ast::Literal(AstLiteral::Boolean(_)))
    }

    pub fn as_literal_boolean(&self) -> &AstLiteralBoolean {
        if let Ast::Literal(AstLiteral::Boolean(result)) = self {
            result
        } else {
            panic!("not literal boolean")
        }
    }

    pub fn is_literal_number(&self) -> bool {
        matches!(self, Ast::Literal(AstLiteral::Number(_)))
    }

    pub fn as_literal_number(&self) -> &AstLiteralNumber {
        if let Ast::Literal(AstLiteral::Number(result)) = self {
            result
        } else {
            panic!("not literal number")
        }
    }

    pub fn is_literal_text(&self) -> bool {
        matches!(self, Ast::Literal(AstLiteral::Text(_)))
    }

    pub fn as_literal_text(&self) -> &AstLiteralText {
        if let Ast::Literal(AstLiteral::Text(result)) = self {
            result
        } else {
            panic!("not literal text")
        }
    }

    pub fn is_literal_undefined(&self) -> bool {
        matches!(self, Ast::Literal(AstLiteral::Undefined(_)))
    }

    pub fn as_literal_undefined(&self) -> &AstLiteralUndefined {
        if let Ast::Literal(AstLiteral::Undefined(result)) = self {
            result
        } else {
            panic!("not literal undefined")
        }
    }

    pub fn is_order_by(&self) -> bool {
        matches!(self, Ast::OrderBy(_))
    }
    pub fn as_order_by(&self) -> &AstOrderBy {
        if let Ast::OrderBy(result) = self { result } else { panic!("not order by") }
    }
    pub fn is_policy(&self) -> bool {
        matches!(self, Ast::Policy(_))
    }
    pub fn as_policy(&self) -> &AstPolicy {
        if let Ast::Policy(result) = self { result } else { panic!("not policy") }
    }

    pub fn is_policy_block(&self) -> bool {
        matches!(self, Ast::PolicyBlock(_))
    }
    pub fn as_policy_block(&self) -> &AstPolicyBlock {
        if let Ast::PolicyBlock(result) = self { result } else { panic!("not policy block") }
    }

    pub fn is_prefix(&self) -> bool {
        matches!(self, Ast::Prefix(_))
    }
    pub fn as_prefix(&self) -> &AstPrefix {
        if let Ast::Prefix(result) = self { result } else { panic!("not prefix") }
    }

    pub fn is_select(&self) -> bool {
        matches!(self, Ast::Select(_))
    }

    pub fn as_select(&self) -> &AstSelect {
        if let Ast::Select(result) = self { result } else { panic!("not select") }
    }

    pub fn is_tuple(&self) -> bool {
        matches!(self, Ast::Tuple(_))
    }

    pub fn as_tuple(&self) -> &AstTuple {
        if let Ast::Tuple(result) = self { result } else { panic!("not tuple") }
    }

    pub fn is_kind(&self) -> bool {
        matches!(self, Ast::Kind(_))
    }

    pub fn as_kind(&self) -> &AstKind {
        if let Ast::Kind(result) = self { result } else { panic!("not kind") }
    }
}

#[derive(Debug, PartialEq)]
pub struct AstCast {
    pub token: Token,
    pub tuple: AstTuple,
}

#[derive(Debug, PartialEq)]
pub struct AstBlock {
    pub token: Token,
    pub nodes: Vec<Ast>,
}

impl AstBlock {
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl Index<usize> for AstBlock {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        &self.nodes[index]
    }
}

#[derive(Debug, PartialEq)]
pub enum AstCreate {
    DeferredView {
        token: Token,
        schema: AstIdentifier,
        name: AstIdentifier,
        columns: Vec<AstColumnToCreate>,
        // FIXME query
    },
    Schema {
        token: Token,
        name: AstIdentifier,
    },
    Series {
        token: Token,
        schema: AstIdentifier,
        name: AstIdentifier,
        columns: Vec<AstColumnToCreate>,
    },
    Table {
        token: Token,
        schema: AstIdentifier,
        name: AstIdentifier,
        columns: Vec<AstColumnToCreate>,
    },
}

#[derive(Debug, PartialEq)]
pub enum AstDescribe {
    Query { token: Token, node: Box<Ast> },
}

#[derive(Debug, PartialEq)]
pub struct AstColumnToCreate {
    pub name: AstIdentifier,
    pub ty: AstKind,
    pub policies: Option<AstPolicyBlock>,
}

impl AstCreate {
    pub fn token(&self) -> &Token {
        match self {
            AstCreate::DeferredView { token, .. } => token,
            AstCreate::Schema { token, .. } => token,
            AstCreate::Series { token, .. } => token,
            AstCreate::Table { token, .. } => token,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AstFilter {
    pub token: Token,
    pub node: Box<Ast>,
}

#[derive(Debug, PartialEq)]
pub enum AstFrom {
    Store { token: Token, schema: AstIdentifier, store: AstIdentifier },
    Query { token: Token, query: AstBlock },
}

#[derive(Debug, PartialEq)]
pub struct AstAggregateBy {
    pub token: Token,
    pub by: Vec<Ast>,
    pub projections: Vec<Ast>,
}

impl AstFrom {
    pub fn token(&self) -> &Token {
        match self {
            AstFrom::Store { token, .. } => token,
            AstFrom::Query { token, .. } => token,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AstLimit {
    pub token: Token,
    pub limit: usize,
}

#[derive(Debug, PartialEq)]
pub enum AstLiteral {
    Boolean(AstLiteralBoolean),
    Number(AstLiteralNumber),
    Text(AstLiteralText),
    Undefined(AstLiteralUndefined),
}

#[derive(Debug, PartialEq)]
pub struct AstIdentifier(pub Token);

impl AstIdentifier {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }

    pub fn name(&self) -> String {
        self.value().to_string()
    }
}

#[derive(Debug, PartialEq)]
pub enum InfixOperator {
    Add(Token),
    As(Token),
    Arrow(Token),
    AccessPackage(Token),
    AccessProperty(Token),
    Assign(Token),
    Call(Token),
    Subtract(Token),
    Multiply(Token),
    Divide(Token),
    Modulo(Token),
    Equal(Token),
    NotEqual(Token),
    LessThan(Token),
    LessThanEqual(Token),
    GreaterThan(Token),
    GreaterThanEqual(Token),
    TypeAscription(Token),
}

#[derive(Debug, PartialEq)]
pub struct AstInfix {
    pub token: Token,
    pub left: Box<Ast>,
    pub operator: InfixOperator,
    pub right: Box<Ast>,
}

#[derive(Debug, PartialEq)]
pub struct AstInsert {
    pub token: Token,
    pub schema: AstIdentifier,
    pub store: AstIdentifier,
    pub columns: AstTuple,
    pub rows: Vec<AstTuple>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AstLiteralNumber(pub Token);

impl AstLiteralNumber {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub struct AstLiteralText(pub Token);

impl AstLiteralText {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub struct AstLiteralBoolean(pub Token);

impl<'a> AstLiteralBoolean {
    pub fn value(&self) -> bool {
        self.0.kind == TokenKind::Literal(Literal::True)
    }
}

#[derive(Debug, PartialEq)]
pub struct AstLiteralUndefined(pub Token);

impl AstLiteralUndefined {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub struct AstOrderBy {
    pub token: Token,
    pub columns: Vec<AstIdentifier>,
    pub directions: Vec<Option<AstIdentifier>>,
}

#[derive(Debug, PartialEq)]
pub enum AstPolicyKind {
    Saturation,
    Default,
    NotUndefined,
}

#[derive(Debug, PartialEq)]
pub struct AstPolicy {
    pub token: Token,
    pub policy: AstPolicyKind,
    pub value: Box<Ast>,
}

#[derive(Debug, PartialEq)]
pub struct AstPolicyBlock {
    pub token: Token,
    pub policies: Vec<AstPolicy>,
}

#[derive(Debug, PartialEq)]
pub struct AstPrefix {
    pub operator: AstPrefixOperator,
    pub node: Box<Ast>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AstPrefixOperator {
    Plus(Token),
    Negate(Token),
    Not(Token),
}

impl AstPrefixOperator {
    pub fn token(&self) -> &Token {
        match self {
            AstPrefixOperator::Plus(token) => token,
            AstPrefixOperator::Negate(token) => token,
            AstPrefixOperator::Not(token) => token,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AstSelect {
    pub token: Token,
    pub columns: Vec<Ast>,
}

impl Index<usize> for AstSelect {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        &self.columns[index]
    }
}

impl AstSelect {
    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Debug, PartialEq)]
pub struct AstTuple {
    pub token: Token,
    pub nodes: Vec<Ast>,
}

impl AstTuple {
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl Index<usize> for AstTuple {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        &self.nodes[index]
    }
}

#[derive(Debug, PartialEq)]
pub enum AstKind {
    Boolean(Token),

    Float4(Token),
    Float8(Token),

    Int1(Token),
    Int2(Token),
    Int4(Token),
    Int8(Token),
    Int16(Token),

    Number(Token),
    Text(Token),

    Uint1(Token),
    Uint2(Token),
    Uint4(Token),
    Uint8(Token),
    Uint16(Token),
}

impl AstKind {
    pub fn token(&self) -> &Token {
        match self {
            AstKind::Boolean(token) => token,
            AstKind::Float4(token) => token,
            AstKind::Float8(token) => token,
            AstKind::Int1(token) => token,
            AstKind::Int2(token) => token,
            AstKind::Int4(token) => token,
            AstKind::Int8(token) => token,
            AstKind::Int16(token) => token,
            AstKind::Number(token) => token,
            AstKind::Text(token) => token,
            AstKind::Uint1(token) => token,
            AstKind::Uint2(token) => token,
            AstKind::Uint4(token) => token,
            AstKind::Uint8(token) => token,
            AstKind::Uint16(token) => token,
        }
    }
}

impl AstKind {
    pub fn value(&self) -> &str {
        self.token().value()
    }

    pub fn kind(&self) -> Kind {
        match self {
            AstKind::Boolean(_) => Kind::Bool,
            AstKind::Float4(_) => Kind::Float4,
            AstKind::Float8(_) => Kind::Float8,
            AstKind::Int1(_) => Kind::Int1,
            AstKind::Int2(_) => Kind::Int2,
            AstKind::Int4(_) => Kind::Int4,
            AstKind::Int8(_) => Kind::Int8,
            AstKind::Int16(_) => Kind::Int16,
            AstKind::Number(_) => unimplemented!(),
            AstKind::Text(_) => Kind::Text,
            AstKind::Uint1(_) => Kind::Uint1,
            AstKind::Uint2(_) => Kind::Uint2,
            AstKind::Uint4(_) => Kind::Uint4,
            AstKind::Uint8(_) => Kind::Uint8,
            AstKind::Uint16(_) => Kind::Uint16,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AstWildcard(pub Token);
