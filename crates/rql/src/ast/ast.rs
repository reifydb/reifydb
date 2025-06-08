// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::{Literal, Token, TokenKind};
use reifydb_core::ValueKind;
use std::ops::Index;

#[derive(Debug)]
pub struct AstStatement(pub Vec<Ast>);

impl IntoIterator for AstStatement {
    type Item = Ast;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, PartialEq)]
pub enum Ast {
    Create(AstCreate),
    From(AstFrom),
    GroupBy(AstGroupBy),
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
    Type(AstType),
    Wildcard(AstWildcard),
}

impl Ast {
    pub fn token(&self) -> &Token {
        match self {
            Ast::Create(node) => &node.token(),
            Ast::From(node) => &node.token(),
            Ast::GroupBy(node) => &node.token,
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
            Ast::Prefix(node) => &node.node.token(),
            Ast::Select(node) => &node.token,
            Ast::Tuple(node) => &node.token,
            Ast::Type(node) => &node.token(),
            Ast::Wildcard(node) => &node.0,
        }
    }

    pub fn value(&self) -> &str {
        self.token().value()
    }
}

impl Ast {
    pub fn is_create(&self) -> bool {
        matches!(self, Ast::Create(_))
    }
    pub fn as_create(&self) -> &AstCreate {
        if let Ast::Create(result) = self { result } else { panic!("not create") }
    }

    pub fn is_from(&self) -> bool {
        matches!(self, Ast::From(_))
    }
    pub fn as_from(&self) -> &AstFrom {
        if let Ast::From(result) = self { result } else { panic!("not from") }
    }

    pub fn is_group_by(&self) -> bool {
        matches!(self, Ast::GroupBy(_))
    }
    pub fn as_group_by(&self) -> &AstGroupBy {
        if let Ast::GroupBy(result) = self { result } else { panic!("not group by") }
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

    pub fn is_type(&self) -> bool {
        matches!(self, Ast::Type(_))
    }

    pub fn as_type(&self) -> &AstType {
        if let Ast::Type(result) = self { result } else { panic!("not type") }
    }
}

#[derive(Debug, PartialEq)]
pub enum AstCreate {
    Schema { token: Token, name: AstIdentifier },
    Series { token: Token, schema: AstIdentifier, name: AstIdentifier, definitions: AstTuple },
    Table { token: Token, schema: AstIdentifier, name: AstIdentifier, definitions: AstTuple },
}

impl AstCreate {
    pub fn token(&self) -> &Token {
        match self {
            AstCreate::Schema { token, .. } => token,
            AstCreate::Series { token, .. } => token,
            AstCreate::Table { token, .. } => token,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum AstFrom {
    Store { token: Token, schema: AstIdentifier, store: AstIdentifier },
    Query { token: Token, query: AstTuple },
}

#[derive(Debug, PartialEq)]
pub struct AstGroupBy {
    pub token: Token,
    pub columns: Vec<Ast>,
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
    pub columns: Vec<Ast>,
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
    pub operator: PrefixOperator,
    pub node: Box<Ast>,
}

#[derive(Debug, PartialEq)]
pub enum PrefixOperator {
    Plus(Token),
    Negate(Token),
    Not(Token),
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
pub enum AstType {
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

impl AstType {
    pub fn token(&self) -> &Token {
        match self {
            AstType::Boolean(token) => token,
            AstType::Float4(token) => token,
            AstType::Float8(token) => token,
            AstType::Int1(token) => token,
            AstType::Int2(token) => token,
            AstType::Int4(token) => token,
            AstType::Int8(token) => token,
            AstType::Int16(token) => token,
            AstType::Number(token) => token,
            AstType::Text(token) => token,
            AstType::Uint1(token) => token,
            AstType::Uint2(token) => token,
            AstType::Uint4(token) => token,
            AstType::Uint8(token) => token,
            AstType::Uint16(token) => token,
        }
    }
}

impl AstType {
    pub fn value(&self) -> &str {
        self.token().value()
    }

    pub fn kind(&self) -> ValueKind {
        match self {
            AstType::Boolean(_) => ValueKind::Bool,
            AstType::Float4(_) => ValueKind::Float4,
            AstType::Float8(_) => ValueKind::Float8,
            AstType::Int1(_) => ValueKind::Int1,
            AstType::Int2(_) => ValueKind::Int2,
            AstType::Int4(_) => ValueKind::Int4,
            AstType::Int8(_) => ValueKind::Int8,
            AstType::Int16(_) => ValueKind::Int16,
            AstType::Number(_) => unimplemented!(),
            AstType::Text(_) => ValueKind::String,
            AstType::Uint1(_) => ValueKind::Uint1,
            AstType::Uint2(_) => ValueKind::Uint2,
            AstType::Uint4(_) => ValueKind::Uint4,
            AstType::Uint8(_) => ValueKind::Uint8,
            AstType::Uint16(_) => ValueKind::Uint16,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AstWildcard(pub Token);
