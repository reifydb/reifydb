// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::rql::lex::{Literal, Token, TokenKind};
use crate::rql::parse::infix::NodeInfix;
use crate::rql::parse::{Parser, Precedence};

impl Parser {
    pub(crate) fn parse_node(&mut self, precedence: Precedence) -> crate::rql::parse::Result<Node> {
        let mut left = self.parse_primary()?;

        while !self.is_eof() && precedence < self.current_precedence()? {
            left = Node::Infix(self.parse_infix(left)?);
        }
        Ok(left)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Node {
    Block(NodeBlock),
    From(NodeFrom),
    Identifier(NodeIdentifier),
    Infix(NodeInfix),
    Literal(NodeLiteral),
    Nop,
    Prefix(NodePrefix),
    Select(NodeSelect),
    Tuple(NodeTuple),
    Type(NodeType),
    Wildcard(NodeWildcard),
}

impl Node {
    pub(crate) fn token(&self) -> &Token {
        match self {
            Node::Block(node) => &node.token,
            Node::From(node) => &node.token,
            Node::Identifier(node) => &node.0,
            Node::Infix(node) => &node.token,
            Node::Literal(node) => match node {
                NodeLiteral::Boolean(node) => &node.0,
                NodeLiteral::Number(node) => &node.0,
                NodeLiteral::Text(node) => &node.0,
                NodeLiteral::Undefined(node) => &node.0,
            },
            Node::Nop => unreachable!(),
            Node::Prefix(node) => &node.node.token(),
            Node::Select(node) => &node.token,
            Node::Tuple(node) => &node.token,
            Node::Type(node) => &node.token(),
            Node::Wildcard(node) => &node.0,
        }
    }

    pub(crate) fn value(&self) -> &str {
        self.token().value()
    }
}

impl Node {
    pub(crate) fn is_block(&self) -> bool {
        matches!(self, Node::Block(_))
    }
    pub(crate) fn as_block(&self) -> &NodeBlock {
        if let Node::Block(result) = self {
            result
        } else {
            panic!("not block")
        }
    }

    pub(crate) fn is_from(&self) -> bool {
        matches!(self, Node::From(_))
    }
    pub(crate) fn as_from(&self) -> &NodeFrom {
        if let Node::From(result) = self {
            result
        } else {
            panic!("not from")
        }
    }

    pub(crate) fn is_identifier(&self) -> bool {
        matches!(self, Node::Identifier(_))
    }
    pub(crate) fn as_identifier(&self) -> &NodeIdentifier {
        if let Node::Identifier(result) = self {
            result
        } else {
            panic!("not identifier")
        }
    }

    pub(crate) fn is_infix(&self) -> bool {
        matches!(self, Node::Infix(_))
    }
    pub(crate) fn as_infix(&self) -> &NodeInfix {
        if let Node::Infix(result) = self {
            result
        } else {
            panic!("not infix")
        }
    }

    pub(crate) fn is_literal(&self) -> bool {
        matches!(self, Node::Literal(_))
    }
    pub(crate) fn as_literal(&self) -> &NodeLiteral {
        if let Node::Literal(result) = self {
            result
        } else {
            panic!("not literal")
        }
    }

    pub(crate) fn is_prefix(&self) -> bool {
        matches!(self, Node::Prefix(_))
    }
    pub(crate) fn as_prefix(&self) -> &NodePrefix {
        if let Node::Prefix(result) = self {
            result
        } else {
            panic!("not prefix")
        }
    }

    pub(crate) fn is_select(&self) -> bool {
        matches!(self, Node::Select(_))
    }

    pub(crate) fn as_select(&self) -> &NodeSelect {
        if let Node::Select(result) = self {
            result
        } else {
            panic!("not select")
        }
    }

    pub(crate) fn is_tuple(&self) -> bool {
        matches!(self, Node::Tuple(_))
    }

    pub(crate) fn as_tuple(&self) -> &NodeTuple {
        if let Node::Tuple(result) = self {
            result
        } else {
            panic!("not tuple")
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeBlock {
    pub token: Token,
    pub nodes: Vec<Node>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeFrom {
    pub token: Token,
    pub source: Box<Node>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum NodeLiteral {
    Boolean(NodeLiteralBoolean),
    Number(NodeLiteralNumber),
    Text(NodeLiteralText),
    Undefined(NodeLiteralUndefined),
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeIdentifier(pub(crate) Token);

impl NodeIdentifier {
    pub(crate) fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeLiteralNumber(pub(crate) Token);

impl NodeLiteralNumber {
    pub(crate) fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeLiteralText(pub(crate) Token);

impl NodeLiteralText {
    pub(crate) fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeLiteralBoolean(pub(crate) Token);

impl<'a> NodeLiteralBoolean {
    pub(crate) fn value(&self) -> bool {
        self.0.kind == TokenKind::Literal(Literal::True)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeLiteralUndefined(pub(crate) Token);

impl NodeLiteralUndefined {
    pub(crate) fn value(&self) -> &str {
        self.0.span.fragment.as_str()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodePrefix {
    pub(crate) operator: PrefixOperator,
    pub(crate) node: Box<Node>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PrefixOperator {
    Plus(Token),
    Negate(Token),
    Not(Token),
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeSelect {
    pub token: Token,
    pub columns: Vec<Node>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeTuple {
    pub(crate) token: Token,
    pub(crate) nodes: Vec<Node>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum NodeType {
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

impl NodeType {
    pub(crate) fn token(&self) -> &Token {
        match self {
            NodeType::Boolean(token) => token,
            NodeType::Float4(token) => token,
            NodeType::Float8(token) => token,
            NodeType::Int1(token) => token,
            NodeType::Int2(token) => token,
            NodeType::Int4(token) => token,
            NodeType::Int8(token) => token,
            NodeType::Int16(token) => token,
            NodeType::Number(token) => token,
            NodeType::Text(token) => token,
            NodeType::Uint1(token) => token,
            NodeType::Uint2(token) => token,
            NodeType::Uint4(token) => token,
            NodeType::Uint8(token) => token,
            NodeType::Uint16(token) => token,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct NodeWildcard(pub(crate) Token);
