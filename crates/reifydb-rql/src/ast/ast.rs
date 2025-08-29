// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::{Deref, Index};

use reifydb_core::{Fragment, IndexType, JoinType, SortDirection};

use crate::ast::tokenize::{Literal, ParameterKind, Token, TokenKind};

#[derive(Debug, Clone, PartialEq)]
pub struct AstStatement<'a>(pub Vec<Ast<'a>>);

impl<'a> AstStatement<'a> {
	pub fn first_unchecked(&self) -> &Ast<'a> {
		self.0.first().unwrap()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}
}

impl<'a> Index<usize> for AstStatement<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		self.0.index(index)
	}
}

impl<'a> IntoIterator for AstStatement<'a> {
	type Item = Ast<'a>;
	type IntoIter = std::vec::IntoIter<Self::Item>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum Ast<'a> {
	Aggregate(AstAggregate<'a>),
	Between(AstBetween<'a>),
	CallFunction(AstCallFunction<'a>),
	Cast(AstCast<'a>),
	Create(AstCreate<'a>),
	Alter(AstAlter<'a>),
	Describe(AstDescribe<'a>),
	Distinct(AstDistinct<'a>),
	Filter(AstFilter<'a>),
	From(AstFrom<'a>),
	Identifier(AstIdentifier<'a>),
	Infix(AstInfix<'a>),
	Inline(AstInline<'a>),
	AstDelete(AstDelete<'a>),
	AstInsert(AstInsert<'a>),
	AstUpdate(AstUpdate<'a>),
	Join(AstJoin<'a>),
	Take(AstTake<'a>),
	List(AstList<'a>),
	Literal(AstLiteral<'a>),
	Nop,
	ParameterRef(AstParameterRef<'a>),
	Sort(AstSort<'a>),
	Policy(AstPolicy<'a>),
	PolicyBlock(AstPolicyBlock<'a>),
	Prefix(AstPrefix<'a>),
	Map(AstMap<'a>),
	Extend(AstExtend<'a>),
	Tuple(AstTuple<'a>),
	Wildcard(AstWildcard<'a>),
}

impl<'a> Default for Ast<'a> {
	fn default() -> Self {
		Self::Nop
	}
}

impl<'a> Ast<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			Ast::Inline(node) => &node.token,
			Ast::Between(node) => &node.token,
			Ast::CallFunction(node) => &node.token,
			Ast::Cast(node) => &node.token,
			Ast::Create(node) => node.token(),
			Ast::Alter(node) => node.token(),
			Ast::Describe(node) => match node {
				AstDescribe::Query {
					token,
					..
				} => token,
			},
			Ast::Distinct(node) => &node.token,
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
				AstJoin::InnerJoin {
					token,
					..
				} => token,
				AstJoin::LeftJoin {
					token,
					..
				} => token,
				AstJoin::NaturalJoin {
					token,
					..
				} => token,
			},
			Ast::Nop => unreachable!(),
			Ast::ParameterRef(node) => &node.token,
			Ast::Sort(node) => &node.token,
			Ast::Policy(node) => &node.token,
			Ast::PolicyBlock(node) => &node.token,
			Ast::Prefix(node) => node.node.token(),
			Ast::Map(node) => &node.token,
			Ast::Extend(node) => &node.token,
			Ast::Tuple(node) => &node.token,
			Ast::Wildcard(node) => &node.0,
		}
	}

	pub fn value(&self) -> &str {
		self.token().value()
	}
}

impl<'a> Ast<'a> {
	pub fn is_aggregate(&self) -> bool {
		matches!(self, Ast::Aggregate(_))
	}
	pub fn as_aggregate(&self) -> &AstAggregate<'a> {
		if let Ast::Aggregate(result) = self {
			result
		} else {
			panic!("not aggregate")
		}
	}

	pub fn is_between(&self) -> bool {
		matches!(self, Ast::Between(_))
	}
	pub fn as_between(&self) -> &AstBetween<'a> {
		if let Ast::Between(result) = self {
			result
		} else {
			panic!("not between")
		}
	}

	pub fn is_call_function(&self) -> bool {
		matches!(self, Ast::CallFunction(_))
	}
	pub fn as_call_function(&self) -> &AstCallFunction<'a> {
		if let Ast::CallFunction(result) = self {
			result
		} else {
			panic!("not call function")
		}
	}

	pub fn is_block(&self) -> bool {
		matches!(self, Ast::Inline(_))
	}
	pub fn as_block(&self) -> &AstInline<'a> {
		if let Ast::Inline(result) = self {
			result
		} else {
			panic!("not block")
		}
	}

	pub fn is_cast(&self) -> bool {
		matches!(self, Ast::Cast(_))
	}
	pub fn as_cast(&self) -> &AstCast<'a> {
		if let Ast::Cast(result) = self {
			result
		} else {
			panic!("not cast")
		}
	}

	pub fn is_create(&self) -> bool {
		matches!(self, Ast::Create(_))
	}
	pub fn as_create(&self) -> &AstCreate<'a> {
		if let Ast::Create(result) = self {
			result
		} else {
			panic!("not create")
		}
	}

	pub fn is_alter(&self) -> bool {
		matches!(self, Ast::Alter(_))
	}
	pub fn as_alter(&self) -> &AstAlter<'a> {
		if let Ast::Alter(result) = self {
			result
		} else {
			panic!("not alter")
		}
	}

	pub fn is_describe(&self) -> bool {
		matches!(self, Ast::Describe(_))
	}
	pub fn as_describe(&self) -> &AstDescribe<'a> {
		if let Ast::Describe(result) = self {
			result
		} else {
			panic!("not describe")
		}
	}

	pub fn is_filter(&self) -> bool {
		matches!(self, Ast::Filter(_))
	}
	pub fn as_filter(&self) -> &AstFilter<'a> {
		if let Ast::Filter(result) = self {
			result
		} else {
			panic!("not filter")
		}
	}

	pub fn is_from(&self) -> bool {
		matches!(self, Ast::From(_))
	}
	pub fn as_from(&self) -> &AstFrom<'a> {
		if let Ast::From(result) = self {
			result
		} else {
			panic!("not from")
		}
	}

	pub fn is_identifier(&self) -> bool {
		matches!(self, Ast::Identifier(_))
	}
	pub fn as_identifier(&self) -> &AstIdentifier<'a> {
		if let Ast::Identifier(result) = self {
			result
		} else {
			panic!("not identifier")
		}
	}

	pub fn is_infix(&self) -> bool {
		matches!(self, Ast::Infix(_))
	}
	pub fn as_infix(&self) -> &AstInfix<'a> {
		if let Ast::Infix(result) = self {
			result
		} else {
			panic!("not infix")
		}
	}

	pub fn is_delete(&self) -> bool {
		matches!(self, Ast::AstDelete(_))
	}
	pub fn as_delete(&self) -> &AstDelete<'a> {
		if let Ast::AstDelete(result) = self {
			result
		} else {
			panic!("not delete")
		}
	}

	pub fn is_insert(&self) -> bool {
		matches!(self, Ast::AstInsert(_))
	}
	pub fn as_insert(&self) -> &AstInsert<'a> {
		if let Ast::AstInsert(result) = self {
			result
		} else {
			panic!("not insert")
		}
	}

	pub fn is_update(&self) -> bool {
		matches!(self, Ast::AstUpdate(_))
	}
	pub fn as_update(&self) -> &AstUpdate<'a> {
		if let Ast::AstUpdate(result) = self {
			result
		} else {
			panic!("not update")
		}
	}

	pub fn is_join(&self) -> bool {
		matches!(self, Ast::Join(_))
	}
	pub fn as_join(&self) -> &AstJoin<'a> {
		if let Ast::Join(result) = self {
			result
		} else {
			panic!("not join")
		}
	}

	pub fn is_take(&self) -> bool {
		matches!(self, Ast::Take(_))
	}
	pub fn as_take(&self) -> &AstTake<'a> {
		if let Ast::Take(result) = self {
			result
		} else {
			panic!("not take")
		}
	}

	pub fn is_list(&self) -> bool {
		matches!(self, Ast::List(_))
	}
	pub fn as_list(&self) -> &AstList<'a> {
		if let Ast::List(result) = self {
			result
		} else {
			panic!("not list")
		}
	}

	pub fn is_literal(&self) -> bool {
		matches!(self, Ast::Literal(_))
	}

	pub fn as_literal(&self) -> &AstLiteral<'a> {
		if let Ast::Literal(result) = self {
			result
		} else {
			panic!("not literal")
		}
	}

	pub fn is_literal_boolean(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Boolean(_)))
	}

	pub fn as_literal_boolean(&self) -> &AstLiteralBoolean<'a> {
		if let Ast::Literal(AstLiteral::Boolean(result)) = self {
			result
		} else {
			panic!("not literal boolean")
		}
	}

	pub fn is_literal_number(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Number(_)))
	}

	pub fn as_literal_number(&self) -> &AstLiteralNumber<'a> {
		if let Ast::Literal(AstLiteral::Number(result)) = self {
			result
		} else {
			panic!("not literal number")
		}
	}

	pub fn is_literal_temporal(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Temporal(_)))
	}

	pub fn as_literal_temporal(&self) -> &AstLiteralTemporal<'a> {
		if let Ast::Literal(AstLiteral::Temporal(result)) = self {
			result
		} else {
			panic!("not literal temporal")
		}
	}

	pub fn is_literal_text(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Text(_)))
	}

	pub fn as_literal_text(&self) -> &AstLiteralText<'a> {
		if let Ast::Literal(AstLiteral::Text(result)) = self {
			result
		} else {
			panic!("not literal text")
		}
	}

	pub fn is_literal_undefined(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Undefined(_)))
	}

	pub fn as_literal_undefined(&self) -> &AstLiteralUndefined<'a> {
		if let Ast::Literal(AstLiteral::Undefined(result)) = self {
			result
		} else {
			panic!("not literal undefined")
		}
	}

	pub fn is_sort(&self) -> bool {
		matches!(self, Ast::Sort(_))
	}
	pub fn as_sort(&self) -> &AstSort<'a> {
		if let Ast::Sort(result) = self {
			result
		} else {
			panic!("not sort")
		}
	}
	pub fn is_policy(&self) -> bool {
		matches!(self, Ast::Policy(_))
	}
	pub fn as_policy(&self) -> &AstPolicy<'a> {
		if let Ast::Policy(result) = self {
			result
		} else {
			panic!("not policy")
		}
	}

	pub fn is_policy_block(&self) -> bool {
		matches!(self, Ast::PolicyBlock(_))
	}
	pub fn as_policy_block(&self) -> &AstPolicyBlock<'a> {
		if let Ast::PolicyBlock(result) = self {
			result
		} else {
			panic!("not policy block")
		}
	}

	pub fn is_inline(&self) -> bool {
		matches!(self, Ast::Inline(_))
	}
	pub fn as_inline(&self) -> &AstInline<'a> {
		if let Ast::Inline(result) = self {
			result
		} else {
			panic!("not inline")
		}
	}

	pub fn is_prefix(&self) -> bool {
		matches!(self, Ast::Prefix(_))
	}
	pub fn as_prefix(&self) -> &AstPrefix<'a> {
		if let Ast::Prefix(result) = self {
			result
		} else {
			panic!("not prefix")
		}
	}

	pub fn is_map(&self) -> bool {
		matches!(self, Ast::Map(_))
	}

	pub fn as_map(&self) -> &AstMap<'a> {
		if let Ast::Map(result) = self {
			result
		} else {
			panic!("not map")
		}
	}

	pub fn as_extend(&self) -> &AstExtend<'a> {
		if let Ast::Extend(result) = self {
			result
		} else {
			panic!("not extend")
		}
	}

	pub fn is_tuple(&self) -> bool {
		matches!(self, Ast::Tuple(_))
	}

	pub fn as_tuple(&self) -> &AstTuple<'a> {
		if let Ast::Tuple(result) = self {
			result
		} else {
			panic!("not tuple")
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCast<'a> {
	pub token: Token<'a>,
	pub tuple: AstTuple<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCallFunction<'a> {
	pub token: Token<'a>,
	pub namespaces: Vec<AstIdentifier<'a>>,
	pub function: AstIdentifier<'a>,
	pub arguments: AstTuple<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInlineKeyedValue<'a> {
	pub key: AstIdentifier<'a>,
	pub value: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInline<'a> {
	pub token: Token<'a>,
	pub keyed_values: Vec<AstInlineKeyedValue<'a>>,
}

impl<'a> AstInline<'a> {
	pub fn len(&self) -> usize {
		self.keyed_values.len()
	}
}

impl<'a> Index<usize> for AstInline<'a> {
	type Output = AstInlineKeyedValue<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.keyed_values[index]
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstCreate<'a> {
	DeferredView(AstCreateDeferredView<'a>),
	TransactionalView(AstCreateTransactionalView<'a>),
	Schema(AstCreateSchema<'a>),
	Series(AstCreateSeries<'a>),
	Table(AstCreateTable<'a>),
	Index(AstCreateIndex<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlter<'a> {
	Sequence(AstAlterSequence<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterSequence<'a> {
	pub token: Token<'a>,
	pub schema: Option<AstIdentifier<'a>>,
	pub table: AstIdentifier<'a>,
	pub column: AstIdentifier<'a>,
	pub value: AstLiteral<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDeferredView<'a> {
	pub token: Token<'a>,
	pub schema: AstIdentifier<'a>,
	pub view: AstIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
	pub with: Option<AstStatement<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTransactionalView<'a> {
	pub token: Token<'a>,
	pub schema: AstIdentifier<'a>,
	pub view: AstIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
	pub with: Option<AstStatement<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSchema<'a> {
	pub token: Token<'a>,
	pub name: AstIdentifier<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSeries<'a> {
	pub token: Token<'a>,
	pub schema: AstIdentifier<'a>,
	pub name: AstIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTable<'a> {
	pub token: Token<'a>,
	pub schema: AstIdentifier<'a>,
	pub table: AstIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDescribe<'a> {
	Query {
		token: Token<'a>,
		node: Box<Ast<'a>>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstColumnToCreate<'a> {
	pub name: AstIdentifier<'a>,
	pub ty: AstIdentifier<'a>,
	pub policies: Option<AstPolicyBlock<'a>>,
	pub auto_increment: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateIndex<'a> {
	pub token: Token<'a>,
	pub index_type: IndexType,
	pub name: AstIdentifier<'a>,
	pub schema: AstIdentifier<'a>,
	pub table: AstIdentifier<'a>,
	pub columns: Vec<AstIndexColumn<'a>>,
	pub filters: Vec<Box<Ast<'a>>>,
	pub map: Option<Box<Ast<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIndexColumn<'a> {
	pub column: AstIdentifier<'a>,
	pub order: Option<SortDirection>,
}

impl<'a> AstCreate<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			AstCreate::DeferredView(AstCreateDeferredView {
				token,
				..
			}) => token,
			AstCreate::TransactionalView(
				AstCreateTransactionalView {
					token,
					..
				},
			) => token,
			AstCreate::Schema(AstCreateSchema {
				token,
				..
			}) => token,
			AstCreate::Series(AstCreateSeries {
				token,
				..
			}) => token,
			AstCreate::Table(AstCreateTable {
				token,
				..
			}) => token,
			AstCreate::Index(AstCreateIndex {
				token,
				..
			}) => token,
		}
	}
}

impl<'a> AstAlter<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			AstAlter::Sequence(AstAlterSequence {
				token,
				..
			}) => token,
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstFilter<'a> {
	pub token: Token<'a>,
	pub node: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstFrom<'a> {
	Source {
		token: Token<'a>,
		schema: Option<AstIdentifier<'a>>,
		source: AstIdentifier<'a>,
	},
	Inline {
		token: Token<'a>,
		list: AstList<'a>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAggregate<'a> {
	pub token: Token<'a>,
	pub by: Vec<Ast<'a>>,
	pub map: Vec<Ast<'a>>,
}

impl<'a> AstFrom<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			AstFrom::Source {
				token,
				..
			} => token,
			AstFrom::Inline {
				token,
				..
			} => token,
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstTake<'a> {
	pub token: Token<'a>,
	pub take: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstList<'a> {
	pub token: Token<'a>,
	pub nodes: Vec<Ast<'a>>,
}

impl<'a> AstList<'a> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

impl<'a> Index<usize> for AstList<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstLiteral<'a> {
	Boolean(AstLiteralBoolean<'a>),
	Number(AstLiteralNumber<'a>),
	Text(AstLiteralText<'a>),
	Temporal(AstLiteralTemporal<'a>),
	Undefined(AstLiteralUndefined<'a>),
}

impl<'a> AstLiteral<'a> {
	pub fn fragment(self) -> Fragment<'a> {
		match self {
			AstLiteral::Boolean(literal) => {
				literal.0.fragment.clone()
			}
			AstLiteral::Number(literal) => {
				literal.0.fragment.clone()
			}
			AstLiteral::Text(literal) => literal.0.fragment.clone(),
			AstLiteral::Temporal(literal) => {
				literal.0.fragment.clone()
			}
			AstLiteral::Undefined(literal) => {
				literal.0.fragment.clone()
			}
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIdentifier<'a>(pub Token<'a>);

impl<'a> AstIdentifier<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.fragment()
	}

	pub fn name(&self) -> String {
		self.value().to_string()
	}

	pub fn fragment(self) -> Fragment<'a> {
		self.0.fragment.clone()
	}
}

impl<'a> Deref for AstIdentifier<'a> {
	type Target = Token<'a>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum InfixOperator<'a> {
	Add(Token<'a>),
	As(Token<'a>),
	Arrow(Token<'a>),
	AccessNamespace(Token<'a>),
	AccessTable(Token<'a>),
	Assign(Token<'a>),
	Call(Token<'a>),
	Subtract(Token<'a>),
	Multiply(Token<'a>),
	Divide(Token<'a>),
	Rem(Token<'a>),
	Equal(Token<'a>),
	NotEqual(Token<'a>),
	LessThan(Token<'a>),
	LessThanEqual(Token<'a>),
	GreaterThan(Token<'a>),
	GreaterThanEqual(Token<'a>),
	TypeAscription(Token<'a>),
	And(Token<'a>),
	Or(Token<'a>),
	Xor(Token<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInfix<'a> {
	pub token: Token<'a>,
	pub left: Box<Ast<'a>>,
	pub operator: InfixOperator<'a>,
	pub right: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDelete<'a> {
	pub token: Token<'a>,
	pub schema: Option<AstIdentifier<'a>>,
	pub table: Option<AstIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInsert<'a> {
	pub token: Token<'a>,
	pub schema: Option<AstIdentifier<'a>>,
	pub table: AstIdentifier<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstUpdate<'a> {
	pub token: Token<'a>,
	pub schema: Option<AstIdentifier<'a>>,
	pub table: Option<AstIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstJoin<'a> {
	InnerJoin {
		token: Token<'a>,
		with: Box<Ast<'a>>,
		on: Vec<Ast<'a>>,
	},
	LeftJoin {
		token: Token<'a>,
		with: Box<Ast<'a>>,
		on: Vec<Ast<'a>>,
	},
	NaturalJoin {
		token: Token<'a>,
		with: Box<Ast<'a>>,
		join_type: Option<JoinType>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNumber<'a>(pub Token<'a>);

impl<'a> AstLiteralNumber<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.fragment()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralTemporal<'a>(pub Token<'a>);

impl<'a> AstLiteralTemporal<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.fragment()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralText<'a>(pub Token<'a>);

impl<'a> AstLiteralText<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.fragment()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralBoolean<'a>(pub Token<'a>);

impl<'a> AstLiteralBoolean<'a> {
	pub fn value(&self) -> bool {
		self.0.kind == TokenKind::Literal(Literal::True)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUndefined<'a>(pub Token<'a>);

impl<'a> AstLiteralUndefined<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.fragment()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDistinct<'a> {
	pub token: Token<'a>,
	pub columns: Vec<AstIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSort<'a> {
	pub token: Token<'a>,
	pub columns: Vec<AstIdentifier<'a>>,
	pub directions: Vec<Option<AstIdentifier<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstPolicyKind {
	Saturation,
	Default,
	NotUndefined,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPolicy<'a> {
	pub token: Token<'a>,
	pub policy: AstPolicyKind,
	pub value: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPolicyBlock<'a> {
	pub token: Token<'a>,
	pub policies: Vec<AstPolicy<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPrefix<'a> {
	pub operator: AstPrefixOperator<'a>,
	pub node: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstPrefixOperator<'a> {
	Plus(Token<'a>),
	Negate(Token<'a>),
	Not(Token<'a>),
}

impl<'a> AstPrefixOperator<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			AstPrefixOperator::Plus(token) => token,
			AstPrefixOperator::Negate(token) => token,
			AstPrefixOperator::Not(token) => token,
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstMap<'a> {
	pub token: Token<'a>,
	pub nodes: Vec<Ast<'a>>,
}

impl<'a> Index<usize> for AstMap<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl<'a> AstMap<'a> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstExtend<'a> {
	pub token: Token<'a>,
	pub nodes: Vec<Ast<'a>>,
}

impl<'a> Index<usize> for AstExtend<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl<'a> AstExtend<'a> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstTuple<'a> {
	pub token: Token<'a>,
	pub nodes: Vec<Ast<'a>>,
}

impl<'a> AstTuple<'a> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

impl<'a> Index<usize> for AstTuple<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstBetween<'a> {
	pub token: Token<'a>,
	pub value: Box<Ast<'a>>,
	pub lower: Box<Ast<'a>>,
	pub upper: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWildcard<'a>(pub Token<'a>);

#[derive(Debug, Clone, PartialEq)]
pub struct AstParameterRef<'a> {
	pub token: Token<'a>,
	pub kind: ParameterKind,
}

impl<'a> AstParameterRef<'a> {
	pub fn position(&self) -> Option<u32> {
		match self.kind {
			ParameterKind::Positional(n) => Some(n),
			ParameterKind::Named => None,
		}
	}

	pub fn name(&self) -> Option<&str> {
		match self.kind {
			ParameterKind::Named => {
				// Extract name from token value (skip the '$')
				Some(&self.token.value()[1..])
			}
			ParameterKind::Positional(_) => None,
		}
	}
}
