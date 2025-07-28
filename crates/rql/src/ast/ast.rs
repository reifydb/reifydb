// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::{Literal, Token, TokenKind};
use reifydb_core::{JoinType, OwnedSpan};
use std::ops::{Deref, Index};

#[derive(Debug, Clone, PartialEq)]
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
    Between(AstBetween),
    Cast(AstCast),
    Create(AstCreate),
    Describe(AstDescribe),
    Filter(AstFilter),
    From(AstFrom),
    Identifier(AstIdentifier),
    Infix(AstInfix),
    Inline(AstInline),
    AstDelete(AstDelete),
    AstInsert(AstInsert),
    AstUpdate(AstUpdate),
    Join(AstJoin),
    Take(AstTake),
    List(AstList),
    Literal(AstLiteral),
    Nop,
    Sort(AstSort),
    Policy(AstPolicy),
    PolicyBlock(AstPolicyBlock),
    Prefix(AstPrefix),
    Map(AstMap),
    Tuple(AstTuple),
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
            Ast::Inline(node) => &node.token,
            Ast::Between(node) => &node.token,
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
            Ast::AstDelete(node) => &node.token,
            Ast::AstInsert(node) => &node.token,
            Ast::AstUpdate(node) => &node.token,
            Ast::Take(node) => &node.token,
            Ast::List(node) => &node.token,
            Ast::Literal(node) => match node {
                AstLiteral::Boolean(node) => &node.0,
                AstLiteral::Number(node) => &node.0,
                AstLiteral::Temporal(node) => &node.0,
                AstLiteral::Text(node) => &node.0,
                AstLiteral::Undefined(node) => &node.0,
            },
            Ast::Join(node) => match node {
                AstJoin::InnerJoin { token, .. } => token,
                AstJoin::LeftJoin { token, .. } => token,
                AstJoin::NaturalJoin { token, .. } => token,
            },
            Ast::Nop => unreachable!(),
            Ast::Sort(node) => &node.token,
            Ast::Policy(node) => &node.token,
            Ast::PolicyBlock(node) => &node.token,
            Ast::Prefix(node) => node.node.token(),
            Ast::Map(node) => &node.token,
            Ast::Tuple(node) => &node.token,
            Ast::Wildcard(node) => &node.0,
        }
    }

    pub fn value(&self) -> &str {
        self.token().value()
    }
}

impl Ast {
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Ast::Aggregate(_))
    }
    pub fn as_aggregate(&self) -> &AstAggregate {
        if let Ast::Aggregate(result) = self { result } else { panic!("not aggregate") }
    }

    pub fn is_between(&self) -> bool {
        matches!(self, Ast::Between(_))
    }
    pub fn as_between(&self) -> &AstBetween {
        if let Ast::Between(result) = self { result } else { panic!("not between") }
    }

    pub fn is_block(&self) -> bool {
        matches!(self, Ast::Inline(_))
    }
    pub fn as_block(&self) -> &AstInline {
        if let Ast::Inline(result) = self { result } else { panic!("not block") }
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

    pub fn is_delete(&self) -> bool {
        matches!(self, Ast::AstDelete(_))
    }
    pub fn as_delete(&self) -> &AstDelete {
        if let Ast::AstDelete(result) = self { result } else { panic!("not delete") }
    }

    pub fn is_insert(&self) -> bool {
        matches!(self, Ast::AstInsert(_))
    }
    pub fn as_insert(&self) -> &AstInsert {
        if let Ast::AstInsert(result) = self { result } else { panic!("not insert") }
    }

    pub fn is_update(&self) -> bool {
        matches!(self, Ast::AstUpdate(_))
    }
    pub fn as_update(&self) -> &AstUpdate {
        if let Ast::AstUpdate(result) = self { result } else { panic!("not update") }
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

    pub fn is_list(&self) -> bool {
        matches!(self, Ast::List(_))
    }
    pub fn as_list(&self) -> &AstList {
        if let Ast::List(result) = self { result } else { panic!("not list") }
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

    pub fn is_literal_temporal(&self) -> bool {
        matches!(self, Ast::Literal(AstLiteral::Temporal(_)))
    }

    pub fn as_literal_temporal(&self) -> &AstLiteralTemporal {
        if let Ast::Literal(AstLiteral::Temporal(result)) = self {
            result
        } else {
            panic!("not literal temporal")
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

    pub fn is_inline(&self) -> bool {
        matches!(self, Ast::Inline(_))
    }
    pub fn as_inline(&self) -> &AstInline {
        if let Ast::Inline(result) = self { result } else { panic!("not inline") }
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCast {
    pub token: Token,
    pub tuple: AstTuple,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInlineKeyedValue {
    pub key: AstIdentifier,
    pub value: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInline {
    pub token: Token,
    pub keyed_values: Vec<AstInlineKeyedValue>,
}

impl AstInline {
    pub fn len(&self) -> usize {
        self.keyed_values.len()
    }
}

impl Index<usize> for AstInline {
    type Output = AstInlineKeyedValue;

    fn index(&self, index: usize) -> &Self::Output {
        &self.keyed_values[index]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstCreate {
    ComputedView(AstCreateComputedView),
    Schema(AstCreateSchema),
    Series(AstCreateSeries),
    Table(AstCreateTable),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateComputedView {
    pub token: Token,
    pub schema: AstIdentifier,
    pub view: AstIdentifier,
    pub columns: Vec<AstColumnToCreate>,
    pub with: Option<AstStatement>,
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
    pub ty: AstIdentifier,
    pub policies: Option<AstPolicyBlock>,
}

impl AstCreate {
    pub fn token(&self) -> &Token {
        match self {
            AstCreate::ComputedView(AstCreateComputedView { token, .. }) => token,
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
    Static { token: Token, list: AstList },
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
            AstFrom::Static { token, .. } => token,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstTake {
    pub token: Token,
    pub take: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstList {
    pub token: Token,
    pub nodes: Vec<Ast>,
}

impl AstList {
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl Index<usize> for AstList {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        &self.nodes[index]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstLiteral {
    Boolean(AstLiteralBoolean),
    Number(AstLiteralNumber),
    Text(AstLiteralText),
    Temporal(AstLiteralTemporal),
    Undefined(AstLiteralUndefined),
}

impl AstLiteral {
    pub fn span(self) -> OwnedSpan {
        match self {
            AstLiteral::Boolean(literal) => literal.0.span,
            AstLiteral::Number(literal) => literal.0.span,
            AstLiteral::Text(literal) => literal.0.span,
            AstLiteral::Temporal(literal) => literal.0.span,
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

    pub fn span(self) -> OwnedSpan {
        self.0.span
    }
}

impl Deref for AstIdentifier {
    type Target = Token;

    fn deref(&self) -> &Self::Target {
        &self.0
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
    And(Token),
    Or(Token),
    Xor(Token),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInfix {
    pub token: Token,
    pub left: Box<Ast>,
    pub operator: InfixOperator,
    pub right: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDelete {
    pub token: Token,
    pub schema: Option<AstIdentifier>,
    pub table: AstIdentifier,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInsert {
    pub token: Token,
    pub schema: Option<AstIdentifier>,
    pub table: AstIdentifier,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstUpdate {
    pub token: Token,
    pub schema: Option<AstIdentifier>,
    pub table: AstIdentifier,
}


#[derive(Debug, Clone, PartialEq)]
pub enum AstJoin {
    InnerJoin { token: Token, with: Box<Ast>, on: Vec<Ast> },
    LeftJoin { token: Token, with: Box<Ast>, on: Vec<Ast> },
    NaturalJoin { token: Token, with: Box<Ast>, join_type: Option<JoinType> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNumber(pub Token);

impl AstLiteralNumber {
    pub fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralTemporal(pub Token);

impl AstLiteralTemporal {
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
    pub nodes: Vec<Ast>,
}

impl Index<usize> for AstMap {
    type Output = Ast;

    fn index(&self, index: usize) -> &Self::Output {
        &self.nodes[index]
    }
}

impl AstMap {
    pub fn len(&self) -> usize {
        self.nodes.len()
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
pub struct AstBetween {
    pub token: Token,
    pub value: Box<Ast>,
    pub lower: Box<Ast>,
    pub upper: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWildcard(pub Token);
