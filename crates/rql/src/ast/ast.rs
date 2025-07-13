// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::{Literal, Token, TokenKind};
use reifydb_core::{DataType, Span};
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

    pub fn len(&self) -> usize {
        self.0.len()
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

#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    Aggregate(AstAggregate),
    Block(AstBlock),
    Cast(AstCast),
    Create(AstCreate),
    Describe(AstDescribe),
    Filter(AstFilter),
    From(AstFrom),
    Identifier(AstIdentifier),
    Infix(AstInfix),
    InsertIntoTable(AstInsertIntoTable),
    Join(AstJoin),
    Take(AstTake),
    Literal(AstLiteral),
    Nop,
    Sort(AstSort),
    Policy(AstPolicy),
    PolicyBlock(AstPolicyBlock),
    Prefix(AstPrefix),
    Map(AstMap),
    Tuple(AstTuple),
    DataType(AstDataType),
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
            Ast::Aggregate(node) => &node.token,
            Ast::Identifier(node) => &node.0,
            Ast::Infix(node) => &node.token,
            Ast::InsertIntoTable(node) => &node.token,
            Ast::Take(node) => &node.token,
            Ast::Literal(node) => match node {
                AstLiteral::Boolean(node) => &node.0,
                AstLiteral::Number(node) => &node.0,
                AstLiteral::Text(node) => &node.0,
                AstLiteral::Undefined(node) => &node.0,
            },
            Ast::Join(node) => match node {
                AstJoin::LeftJoin { token, .. } => token,
            },
            Ast::Nop => unreachable!(),
            Ast::Sort(node) => &node.token,
            Ast::Policy(node) => &node.token,
            Ast::PolicyBlock(node) => &node.token,
            Ast::Prefix(node) => node.node.token(),
            Ast::Map(node) => &node.token,
            Ast::Tuple(node) => &node.token,
            Ast::DataType(node) => node.token(),
            Ast::Wildcard(node) => &node.0,
        }
    }

    pub fn value(&self) -> &str {
        self.token().value()
    }
}

impl Ast {
    pub fn is_aggregate_by(&self) -> bool {
        matches!(self, Ast::Aggregate(_))
    }
    pub fn as_aggregate_by(&self) -> &AstAggregate {
        if let Ast::Aggregate(result) = self { result } else { panic!("not aggregate by") }
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
        matches!(self, Ast::InsertIntoTable(_))
    }
    pub fn as_insert(&self) -> &AstInsertIntoTable {
        if let Ast::InsertIntoTable(result) = self { result } else { panic!("not insert") }
    }

    pub fn is_join(&self) -> bool {
        matches!(self, Ast::Join(_))
    }
    pub fn as_join(&self) -> &AstJoin {
        if let Ast::Join(result) = self { result } else { panic!("not join") }
    }

    pub fn is_take(&self) -> bool {
        matches!(self, Ast::Take(_))
    }
    pub fn as_take(&self) -> &AstTake {
        if let Ast::Take(result) = self { result } else { panic!("not take") }
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

    pub fn is_sort(&self) -> bool {
        matches!(self, Ast::Sort(_))
    }
    pub fn as_sort(&self) -> &AstSort {
        if let Ast::Sort(result) = self { result } else { panic!("not sort") }
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

    pub fn is_map(&self) -> bool {
        matches!(self, Ast::Map(_))
    }

    pub fn as_map(&self) -> &AstMap {
        if let Ast::Map(result) = self { result } else { panic!("not map") }
    }

    pub fn is_tuple(&self) -> bool {
        matches!(self, Ast::Tuple(_))
    }

    pub fn as_tuple(&self) -> &AstTuple {
        if let Ast::Tuple(result) = self { result } else { panic!("not tuple") }
    }

    pub fn is_kind(&self) -> bool {
        matches!(self, Ast::DataType(_))
    }

    pub fn as_kind(&self) -> &AstDataType {
        if let Ast::DataType(result) = self { result } else { panic!("not data_type") }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCast {
    pub token: Token,
    pub tuple: AstTuple,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub enum AstCreate {
    DeferredView(AstCreateDeferredView),
    Schema(AstCreateSchema),
    Series(AstCreateSeries),
    Table(AstCreateTable),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDeferredView {
    pub token: Token,
    pub schema: AstIdentifier,
    pub view: AstIdentifier,
    pub columns: Vec<AstColumnToCreate>,
    // FIXME query
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSchema {
    pub token: Token,
    pub name: AstIdentifier,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSeries {
    pub token: Token,
    pub schema: AstIdentifier,
    pub name: AstIdentifier,
    pub columns: Vec<AstColumnToCreate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTable {
    pub token: Token,
    pub schema: AstIdentifier,
    pub table: AstIdentifier,
    pub columns: Vec<AstColumnToCreate>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDescribe {
    Query { token: Token, node: Box<Ast> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstColumnToCreate {
    pub name: AstIdentifier,
    pub ty: AstDataType,
    pub policies: Option<AstPolicyBlock>,
}

impl AstCreate {
    pub fn token(&self) -> &Token {
        match self {
            AstCreate::DeferredView(AstCreateDeferredView { token, .. }) => token,
            AstCreate::Schema(AstCreateSchema { token, .. }) => token,
            AstCreate::Series(AstCreateSeries { token, .. }) => token,
            AstCreate::Table(AstCreateTable { token, .. }) => token,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstFilter {
    pub token: Token,
    pub node: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstFrom {
    Table { token: Token, schema: Option<AstIdentifier>, table: AstIdentifier },
    Query { token: Token, query: AstBlock },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAggregate {
    pub token: Token,
    pub by: Vec<Ast>,
    pub map: Vec<Ast>,
}

impl AstFrom {
    pub fn token(&self) -> &Token {
        match self {
            AstFrom::Table { token, .. } => token,
            AstFrom::Query { token, .. } => token,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstTake {
    pub token: Token,
    pub take: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstLiteral {
    Boolean(AstLiteralBoolean),
    Number(AstLiteralNumber),
    Text(AstLiteralText),
    Undefined(AstLiteralUndefined),
}

impl AstLiteral {
    pub fn span(self) -> Span {
        match self {
            AstLiteral::Boolean(literal) => literal.0.span,
            AstLiteral::Number(literal) => literal.0.span,
            AstLiteral::Text(literal) => literal.0.span,
            AstLiteral::Undefined(literal) => literal.0.span,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIdentifier(pub Token);

impl AstIdentifier {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }

    pub fn name(&self) -> String {
        self.value().to_string()
    }

    pub fn span(self) -> Span {
        self.0.span
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InfixOperator {
    Add(Token),
    As(Token),
    Arrow(Token),
    AccessExtension(Token),
    AccessTable(Token),
    Assign(Token),
    Call(Token),
    Subtract(Token),
    Multiply(Token),
    Divide(Token),
    Rem(Token),
    Equal(Token),
    NotEqual(Token),
    LessThan(Token),
    LessThanEqual(Token),
    GreaterThan(Token),
    GreaterThanEqual(Token),
    TypeAscription(Token),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInfix {
    pub token: Token,
    pub left: Box<Ast>,
    pub operator: InfixOperator,
    pub right: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInsertIntoTable {
    pub token: Token,
    pub schema: AstIdentifier,
    pub table: AstIdentifier,
    pub columns: AstTuple,
    pub rows: Vec<AstTuple>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstJoin {
    LeftJoin { token: Token, with: Box<Ast>, on: Vec<Ast> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNumber(pub Token);

impl AstLiteralNumber {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralText(pub Token);

impl AstLiteralText {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralBoolean(pub Token);

impl<'a> AstLiteralBoolean {
    pub fn value(&self) -> bool {
        self.0.kind == TokenKind::Literal(Literal::True)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUndefined(pub Token);

impl AstLiteralUndefined {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSort {
    pub token: Token,
    pub columns: Vec<AstIdentifier>,
    pub directions: Vec<Option<AstIdentifier>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstPolicyKind {
    Saturation,
    Default,
    NotUndefined,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPolicy {
    pub token: Token,
    pub policy: AstPolicyKind,
    pub value: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPolicyBlock {
    pub token: Token,
    pub policies: Vec<AstPolicy>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPrefix {
    pub operator: AstPrefixOperator,
    pub node: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct AstMap {
    pub token: Token,
    pub map: Vec<Ast>,
}

impl Index<usize> for AstMap {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        &self.map[index]
    }
}

impl AstMap {
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub enum AstDataType {
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

impl AstDataType {
    pub fn token(&self) -> &Token {
        match self {
            AstDataType::Boolean(token) => token,
            AstDataType::Float4(token) => token,
            AstDataType::Float8(token) => token,
            AstDataType::Int1(token) => token,
            AstDataType::Int2(token) => token,
            AstDataType::Int4(token) => token,
            AstDataType::Int8(token) => token,
            AstDataType::Int16(token) => token,
            AstDataType::Number(token) => token,
            AstDataType::Text(token) => token,
            AstDataType::Uint1(token) => token,
            AstDataType::Uint2(token) => token,
            AstDataType::Uint4(token) => token,
            AstDataType::Uint8(token) => token,
            AstDataType::Uint16(token) => token,
        }
    }
}

impl AstDataType {
    pub fn value(&self) -> &str {
        self.token().value()
    }

    pub fn data_type(&self) -> DataType {
        match self {
            AstDataType::Boolean(_) => DataType::Bool,
            AstDataType::Float4(_) => DataType::Float4,
            AstDataType::Float8(_) => DataType::Float8,
            AstDataType::Int1(_) => DataType::Int1,
            AstDataType::Int2(_) => DataType::Int2,
            AstDataType::Int4(_) => DataType::Int4,
            AstDataType::Int8(_) => DataType::Int8,
            AstDataType::Int16(_) => DataType::Int16,
            AstDataType::Number(_) => unimplemented!(),
            AstDataType::Text(_) => DataType::Utf8,
            AstDataType::Uint1(_) => DataType::Uint1,
            AstDataType::Uint2(_) => DataType::Uint2,
            AstDataType::Uint4(_) => DataType::Uint4,
            AstDataType::Uint8(_) => DataType::Uint8,
            AstDataType::Uint16(_) => DataType::Uint16,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWildcard(pub Token);
