// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Index;

use reifydb_core::{IndexType, JoinType, SortDirection};
use reifydb_type::Fragment;

use crate::ast::{
	identifier::{
		MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
		MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier, MaybeQualifiedFunctionIdentifier,
		MaybeQualifiedIndexIdentifier, MaybeQualifiedNamespaceIdentifier, MaybeQualifiedSequenceIdentifier,
		MaybeQualifiedTableIdentifier, MaybeQualifiedTransactionalViewIdentifier, UnqualifiedIdentifier,
		UnresolvedSourceIdentifier,
	},
	tokenize::{Literal, Token, TokenKind},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AstStatement<'a> {
	pub nodes: Vec<Ast<'a>>,
	pub has_pipes: bool,
}

impl<'a> AstStatement<'a> {
	pub fn first_unchecked(&self) -> &Ast<'a> {
		self.nodes.first().unwrap()
	}

	pub fn is_empty(&self) -> bool {
		self.nodes.is_empty()
	}

	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

impl<'a> Index<usize> for AstStatement<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		self.nodes.index(index)
	}
}

impl<'a> IntoIterator for AstStatement<'a> {
	type Item = Ast<'a>;
	type IntoIter = std::vec::IntoIter<Self::Item>;

	fn into_iter(self) -> Self::IntoIter {
		self.nodes.into_iter()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum Ast<'a> {
	Aggregate(AstAggregate<'a>),
	Apply(AstApply<'a>),
	Between(AstBetween<'a>),
	Call(AstCall<'a>),
	CallFunction(AstCallFunction<'a>),
	Cast(AstCast<'a>),
	Create(AstCreate<'a>),
	Alter(AstAlter<'a>),
	Drop(AstDrop<'a>),
	Describe(AstDescribe<'a>),
	Distinct(AstDistinct<'a>),
	Filter(AstFilter<'a>),
	From(AstFrom<'a>),
	Identifier(UnqualifiedIdentifier<'a>),
	If(AstIf<'a>),
	Infix(AstInfix<'a>),
	Inline(AstInline<'a>),
	Let(AstLet<'a>),
	Delete(AstDelete<'a>),
	Insert(AstInsert<'a>),
	Update(AstUpdate<'a>),
	Join(AstJoin<'a>),
	Merge(AstMerge<'a>),
	Take(AstTake<'a>),
	List(AstList<'a>),
	Literal(AstLiteral<'a>),
	Nop,
	Variable(AstVariable<'a>),
	Environment(AstEnvironment<'a>),
	Sort(AstSort<'a>),
	SubQuery(AstSubQuery<'a>),
	Policy(AstPolicy<'a>),
	PolicyBlock(AstPolicyBlock<'a>),
	Prefix(AstPrefix<'a>),
	Map(AstMap<'a>),
	Generator(AstGenerator<'a>),
	Extend(AstExtend<'a>),
	Tuple(AstTuple<'a>),
	Wildcard(AstWildcard<'a>),
	Window(AstWindow<'a>),
	StatementExpression(AstStatementExpression<'a>),
	Rownum(AstRownum<'a>),
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
			Ast::Apply(node) => &node.token,
			Ast::Between(node) => &node.token,
			Ast::Call(node) => &node.token,
			Ast::CallFunction(node) => &node.token,
			Ast::Cast(node) => &node.token,
			Ast::Create(node) => node.token(),
			Ast::Alter(node) => node.token(),
			Ast::Drop(node) => node.token(),
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
			Ast::Identifier(identifier) => &identifier.token,
			Ast::If(node) => &node.token,
			Ast::Infix(node) => &node.token,
			Ast::Let(node) => &node.token,
			Ast::Delete(node) => &node.token,
			Ast::Insert(node) => &node.token,
			Ast::Update(node) => &node.token,
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
			Ast::Merge(node) => &node.token,
			Ast::Nop => unreachable!(),
			Ast::Variable(node) => &node.token,
			Ast::Sort(node) => &node.token,
			Ast::SubQuery(node) => &node.token,
			Ast::Policy(node) => &node.token,
			Ast::PolicyBlock(node) => &node.token,
			Ast::Prefix(node) => node.node.token(),
			Ast::Map(node) => &node.token,
			Ast::Generator(node) => &node.token,
			Ast::Extend(node) => &node.token,
			Ast::Tuple(node) => &node.token,
			Ast::Wildcard(node) => &node.0,
			Ast::Window(node) => &node.token,
			Ast::StatementExpression(node) => node.expression.token(),
			Ast::Environment(node) => &node.token,
			Ast::Rownum(node) => &node.token,
		}
	}

	pub fn value(&self) -> &str {
		match self {
			Ast::Identifier(ident) => ident.text(),
			_ => self.token().value(),
		}
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
	pub fn as_identifier(&self) -> &UnqualifiedIdentifier<'a> {
		if let Ast::Identifier(result) = self {
			result
		} else {
			panic!("not identifier")
		}
	}

	pub fn is_if(&self) -> bool {
		matches!(self, Ast::If(_))
	}
	pub fn as_if(&self) -> &AstIf<'a> {
		if let Ast::If(result) = self {
			result
		} else {
			panic!("not if")
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

	pub fn is_let(&self) -> bool {
		matches!(self, Ast::Let(_))
	}
	pub fn as_let(&self) -> &AstLet<'a> {
		if let Ast::Let(result) = self {
			result
		} else {
			panic!("not let")
		}
	}

	pub fn is_variable(&self) -> bool {
		matches!(self, Ast::Variable(_))
	}
	pub fn as_variable(&self) -> &AstVariable<'a> {
		if let Ast::Variable(result) = self {
			result
		} else {
			panic!("not variable")
		}
	}

	pub fn as_environment(&self) -> &AstEnvironment<'a> {
		if let Ast::Environment(result) = self {
			result
		} else {
			panic!("not environment")
		}
	}

	pub fn is_delete(&self) -> bool {
		matches!(self, Ast::Delete(_))
	}
	pub fn as_delete(&self) -> &AstDelete<'a> {
		if let Ast::Delete(result) = self {
			result
		} else {
			panic!("not delete")
		}
	}

	pub fn is_insert(&self) -> bool {
		matches!(self, Ast::Insert(_))
	}
	pub fn as_insert(&self) -> &AstInsert<'a> {
		if let Ast::Insert(result) = self {
			result
		} else {
			panic!("not insert")
		}
	}

	pub fn is_update(&self) -> bool {
		matches!(self, Ast::Update(_))
	}
	pub fn as_update(&self) -> &AstUpdate<'a> {
		if let Ast::Update(result) = self {
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

	pub fn is_merge(&self) -> bool {
		matches!(self, Ast::Merge(_))
	}
	pub fn as_merge(&self) -> &AstMerge<'a> {
		if let Ast::Merge(result) = self {
			result
		} else {
			panic!("not merge")
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

	pub fn is_generator(&self) -> bool {
		matches!(self, Ast::Generator(_))
	}

	pub fn as_generator(&self) -> &AstGenerator<'a> {
		if let Ast::Generator(result) = self {
			result
		} else {
			panic!("not generator")
		}
	}

	pub fn as_apply(&self) -> &AstApply<'a> {
		if let Ast::Apply(result) = self {
			result
		} else {
			panic!("not apply")
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

	pub fn is_window(&self) -> bool {
		matches!(self, Ast::Window(_))
	}

	pub fn as_window(&self) -> &AstWindow<'a> {
		if let Ast::Window(result) = self {
			result
		} else {
			panic!("not window")
		}
	}

	pub fn is_statement_expression(&self) -> bool {
		matches!(self, Ast::StatementExpression(_))
	}

	pub fn as_statement_expression(&self) -> &AstStatementExpression<'a> {
		if let Ast::StatementExpression(result) = self {
			result
		} else {
			panic!("not statement expression")
		}
	}

	pub fn is_rownum(&self) -> bool {
		matches!(self, Ast::Rownum(_))
	}

	pub fn as_rownum(&self) -> &AstRownum<'a> {
		if let Ast::Rownum(result) = self {
			result
		} else {
			panic!("not rownum")
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCast<'a> {
	pub token: Token<'a>,
	pub tuple: AstTuple<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstApply<'a> {
	pub token: Token<'a>,
	pub operator: UnqualifiedIdentifier<'a>,
	pub expressions: Vec<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCall<'a> {
	pub token: Token<'a>,
	pub operator: UnqualifiedIdentifier<'a>,
	pub arguments: AstTuple<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCallFunction<'a> {
	pub token: Token<'a>,
	pub function: MaybeQualifiedFunctionIdentifier<'a>,
	pub arguments: AstTuple<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInlineKeyedValue<'a> {
	pub key: UnqualifiedIdentifier<'a>,
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
	Flow(AstCreateFlow<'a>),
	Namespace(AstCreateNamespace<'a>),
	Series(AstCreateSeries<'a>),
	Table(AstCreateTable<'a>),
	RingBuffer(AstCreateRingBuffer<'a>),
	Dictionary(AstCreateDictionary<'a>),
	Index(AstCreateIndex<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlter<'a> {
	Sequence(AstAlterSequence<'a>),
	Table(AstAlterTable<'a>),
	View(AstAlterView<'a>),
	Flow(AstAlterFlow<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDrop<'a> {
	Flow(AstDropFlow<'a>),
	// Future: Table, View, Namespace, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropFlow<'a> {
	pub token: Token<'a>,
	pub if_exists: bool,
	pub flow: MaybeQualifiedFlowIdentifier<'a>,
	pub cascade: bool, // CASCADE or RESTRICT (false = RESTRICT)
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterSequence<'a> {
	pub token: Token<'a>,
	pub sequence: MaybeQualifiedSequenceIdentifier<'a>,
	pub column: Fragment<'a>,
	pub value: AstLiteral<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterTable<'a> {
	pub token: Token<'a>,
	pub table: MaybeQualifiedTableIdentifier<'a>,
	pub operations: Vec<AstAlterTableOperation<'a>>,
}

/// Represents a subquery - a complete query statement enclosed in braces
/// Used in contexts like joins, CTEs, and derived tables
#[derive(Debug, Clone, PartialEq)]
pub struct AstSubQuery<'a> {
	pub token: Token<'a>,
	pub statement: AstStatement<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlterTableOperation<'a> {
	CreatePrimaryKey {
		name: Option<Fragment<'a>>,
		columns: Vec<AstIndexColumn<'a>>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterView<'a> {
	pub token: Token<'a>,
	pub view: crate::ast::identifier::MaybeQualifiedViewIdentifier<'a>,
	pub operations: Vec<AstAlterViewOperation<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlterViewOperation<'a> {
	CreatePrimaryKey {
		name: Option<Fragment<'a>>,
		columns: Vec<AstIndexColumn<'a>>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterFlow<'a> {
	pub token: Token<'a>,
	pub flow: MaybeQualifiedFlowIdentifier<'a>,
	pub action: AstAlterFlowAction<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlterFlowAction<'a> {
	Rename {
		new_name: Fragment<'a>,
	},
	SetQuery {
		query: AstStatement<'a>,
	},
	Pause,
	Resume,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDeferredView<'a> {
	pub token: Token<'a>,
	pub view: MaybeQualifiedDeferredViewIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
	pub as_clause: Option<AstStatement<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTransactionalView<'a> {
	pub token: Token<'a>,
	pub view: MaybeQualifiedTransactionalViewIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
	pub as_clause: Option<AstStatement<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateFlow<'a> {
	pub token: Token<'a>,
	pub or_replace: bool,
	pub if_not_exists: bool,
	pub flow: MaybeQualifiedFlowIdentifier<'a>,
	pub as_clause: AstStatement<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateNamespace<'a> {
	pub token: Token<'a>,
	pub namespace: MaybeQualifiedNamespaceIdentifier<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSeries<'a> {
	pub token: Token<'a>,
	pub sequence: MaybeQualifiedSequenceIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTable<'a> {
	pub token: Token<'a>,
	pub table: MaybeQualifiedTableIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateRingBuffer<'a> {
	pub token: Token<'a>,
	pub ring_buffer: crate::ast::identifier::MaybeQualifiedRingBufferIdentifier<'a>,
	pub columns: Vec<AstColumnToCreate<'a>>,
	pub capacity: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDictionary<'a> {
	pub token: Token<'a>,
	pub if_not_exists: bool,
	pub dictionary: MaybeQualifiedDictionaryIdentifier<'a>,
	pub value_type: AstDataType<'a>,
	pub id_type: AstDataType<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDescribe<'a> {
	Query {
		token: Token<'a>,
		node: Box<Ast<'a>>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDataType<'a> {
	Unconstrained(Fragment<'a>), // UTF8, BLOB, etc.
	Constrained {
		name: Fragment<'a>,
		params: Vec<AstLiteral<'a>>,
	}, // UTF8(50), DECIMAL(10,2)
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstColumnToCreate<'a> {
	pub name: Fragment<'a>,
	pub ty: AstDataType<'a>,
	pub policies: Option<AstPolicyBlock<'a>>,
	pub auto_increment: bool,
	pub dictionary: Option<MaybeQualifiedDictionaryIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateIndex<'a> {
	pub token: Token<'a>,
	pub index_type: IndexType,
	pub index: MaybeQualifiedIndexIdentifier<'a>,
	pub columns: Vec<AstIndexColumn<'a>>,
	pub filters: Vec<Box<Ast<'a>>>,
	pub map: Option<Box<Ast<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIndexColumn<'a> {
	pub column: MaybeQualifiedColumnIdentifier<'a>,
	pub order: Option<SortDirection>,
}

impl<'a> AstCreate<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			AstCreate::DeferredView(AstCreateDeferredView {
				token,
				..
			}) => token,
			AstCreate::TransactionalView(AstCreateTransactionalView {
				token,
				..
			}) => token,
			AstCreate::Flow(AstCreateFlow {
				token,
				..
			}) => token,
			AstCreate::Namespace(AstCreateNamespace {
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
			AstCreate::RingBuffer(AstCreateRingBuffer {
				token,
				..
			}) => token,
			AstCreate::Dictionary(AstCreateDictionary {
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
			AstAlter::Table(AstAlterTable {
				token,
				..
			}) => token,
			AstAlter::View(AstAlterView {
				token,
				..
			}) => token,
			AstAlter::Flow(AstAlterFlow {
				token,
				..
			}) => token,
		}
	}
}

impl<'a> AstDrop<'a> {
	pub fn token(&self) -> &Token<'a> {
		match self {
			AstDrop::Flow(AstDropFlow {
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
		source: UnresolvedSourceIdentifier<'a>,
		index_name: Option<Fragment<'a>>,
	},
	Variable {
		token: Token<'a>,
		variable: AstVariable<'a>,
	},
	Environment {
		token: Token<'a>,
	},
	Inline {
		token: Token<'a>,
		list: AstList<'a>,
	},
	Generator(AstGenerator<'a>),
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
			AstFrom::Variable {
				token,
				..
			} => token,
			AstFrom::Inline {
				token,
				..
			} => token,
			AstFrom::Generator(generator) => &generator.token,
			AstFrom::Environment {
				token,
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
			AstLiteral::Boolean(literal) => literal.0.fragment.clone(),
			AstLiteral::Number(literal) => literal.0.fragment.clone(),
			AstLiteral::Text(literal) => literal.0.fragment.clone(),
			AstLiteral::Temporal(literal) => literal.0.fragment.clone(),
			AstLiteral::Undefined(literal) => literal.0.fragment.clone(),
		}
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
	In(Token<'a>),
	NotIn(Token<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInfix<'a> {
	pub token: Token<'a>,
	pub left: Box<Ast<'a>>,
	pub operator: InfixOperator<'a>,
	pub right: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LetValue<'a> {
	Expression(Box<Ast<'a>>),    // scalar/column expression
	Statement(AstStatement<'a>), // FROM … | …
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLet<'a> {
	pub token: Token<'a>,
	pub name: UnqualifiedIdentifier<'a>,
	pub value: LetValue<'a>,
	pub mutable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDelete<'a> {
	pub token: Token<'a>,
	pub target: Option<UnresolvedSourceIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInsert<'a> {
	pub token: Token<'a>,
	pub target: Option<UnresolvedSourceIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstUpdate<'a> {
	pub token: Token<'a>,
	pub target: Option<UnresolvedSourceIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstJoin<'a> {
	InnerJoin {
		token: Token<'a>,
		with: AstSubQuery<'a>,
		on: Vec<Ast<'a>>,
		alias: Option<Fragment<'a>>,
	},
	LeftJoin {
		token: Token<'a>,
		with: AstSubQuery<'a>,
		on: Vec<Ast<'a>>,
		alias: Option<Fragment<'a>>,
	},
	NaturalJoin {
		token: Token<'a>,
		with: AstSubQuery<'a>,
		join_type: Option<JoinType>,
		alias: Option<Fragment<'a>>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstMerge<'a> {
	pub token: Token<'a>,
	pub with: AstSubQuery<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNumber<'a>(pub Token<'a>);

impl<'a> AstLiteralNumber<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralTemporal<'a>(pub Token<'a>);

impl<'a> AstLiteralTemporal<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralText<'a>(pub Token<'a>);

impl<'a> AstLiteralText<'a> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
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
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDistinct<'a> {
	pub token: Token<'a>,
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSort<'a> {
	pub token: Token<'a>,
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'a>>,
	pub directions: Vec<Option<Fragment<'a>>>,
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
pub struct AstGenerator<'a> {
	pub token: Token<'a>,
	pub name: Fragment<'a>,
	pub nodes: Vec<Ast<'a>>,
}

impl<'a> Index<usize> for AstGenerator<'a> {
	type Output = Ast<'a>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl<'a> AstGenerator<'a> {
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
pub struct AstVariable<'a> {
	pub token: Token<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstRownum<'a> {
	pub token: Token<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstEnvironment<'a> {
	pub token: Token<'a>,
}

impl<'a> AstVariable<'a> {
	pub fn name(&self) -> &str {
		// Extract name from token value (skip the '$')
		let text = self.token.value();
		if text.starts_with('$') {
			&text[1..]
		} else {
			text
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIf<'a> {
	pub token: Token<'a>,
	pub condition: Box<Ast<'a>>,
	pub then_block: Box<Ast<'a>>,
	pub else_ifs: Vec<AstElseIf<'a>>,
	pub else_block: Option<Box<Ast<'a>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstElseIf<'a> {
	pub token: Token<'a>,
	pub condition: Box<Ast<'a>>,
	pub then_block: Box<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWindow<'a> {
	pub token: Token<'a>,
	pub config: Vec<AstWindowConfig<'a>>,
	pub aggregations: Vec<Ast<'a>>,
	pub group_by: Vec<Ast<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWindowConfig<'a> {
	pub key: UnqualifiedIdentifier<'a>,
	pub value: Ast<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstStatementExpression<'a> {
	pub expression: Box<Ast<'a>>,
}
