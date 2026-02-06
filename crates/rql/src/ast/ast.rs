// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Index;

use reifydb_core::{
	common::{IndexType, JoinType},
	sort::SortDirection,
};
use reifydb_type::fragment::Fragment;

use crate::{
	ast::identifier::{
		MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
		MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier, MaybeQualifiedFunctionIdentifier,
		MaybeQualifiedIndexIdentifier, MaybeQualifiedNamespaceIdentifier, MaybeQualifiedSequenceIdentifier,
		MaybeQualifiedTableIdentifier, MaybeQualifiedTransactionalViewIdentifier, UnqualifiedIdentifier,
		UnresolvedPrimitiveIdentifier,
	},
	token::token::{Literal, Token, TokenKind},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AstStatement {
	pub nodes: Vec<Ast>,
	pub has_pipes: bool,
	pub is_output: bool,
}

impl AstStatement {
	pub fn first_unchecked(&self) -> &Ast {
		self.nodes.first().unwrap()
	}

	pub fn is_empty(&self) -> bool {
		self.nodes.is_empty()
	}

	pub fn len(&self) -> usize {
		self.nodes.len()
	}

	/// Returns true if this statement contains any DDL nodes (CREATE, ALTER, DROP).
	pub fn contains_ddl(&self) -> bool {
		self.nodes.iter().any(|node| node.is_ddl())
	}
}

impl Index<usize> for AstStatement {
	type Output = Ast;

	fn index(&self, index: usize) -> &Self::Output {
		self.nodes.index(index)
	}
}

impl IntoIterator for AstStatement {
	type Item = Ast;
	type IntoIter = std::vec::IntoIter<Self::Item>;

	fn into_iter(self) -> Self::IntoIter {
		self.nodes.into_iter()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
	Aggregate(AstAggregate),
	Apply(AstApply),
	Between(AstBetween),
	Block(AstBlock),
	Break(AstBreak),
	Call(AstCall),
	CallFunction(AstCallFunction),
	Cast(AstCast),
	Continue(AstContinue),
	Create(AstCreate),
	Alter(AstAlter),
	Drop(AstDrop),
	Describe(AstDescribe),
	Distinct(AstDistinct),
	Filter(AstFilter),
	For(AstFor),
	From(AstFrom),
	Identifier(UnqualifiedIdentifier),
	If(AstIf),
	Infix(AstInfix),
	Inline(AstInline),
	Let(AstLet),
	Loop(AstLoop),
	Delete(AstDelete),
	Insert(AstInsert),
	Update(AstUpdate),
	Join(AstJoin),
	Merge(AstMerge),
	Take(AstTake),
	List(AstList),
	Literal(AstLiteral),
	Nop,
	Variable(AstVariable),
	Environment(AstEnvironment),
	Sort(AstSort),
	SubQuery(AstSubQuery),
	Policy(AstPolicy),
	PolicyBlock(AstPolicyBlock),
	Prefix(AstPrefix),
	Map(AstMap),
	Generator(AstGenerator),
	Extend(AstExtend),
	Patch(AstPatch),
	Tuple(AstTuple),
	While(AstWhile),
	Wildcard(AstWildcard),
	Window(AstWindow),
	StatementExpression(AstStatementExpression),
	Rownum(AstRownum),
	DefFunction(AstDefFunction),
	Return(AstReturn),
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
			Ast::Apply(node) => &node.token,
			Ast::Between(node) => &node.token,
			Ast::Block(node) => &node.token,
			Ast::Break(node) => &node.token,
			Ast::Call(node) => &node.token,
			Ast::CallFunction(node) => &node.token,
			Ast::Cast(node) => &node.token,
			Ast::Continue(node) => &node.token,
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
			Ast::For(node) => &node.token,
			Ast::From(node) => node.token(),
			Ast::Aggregate(node) => &node.token,
			Ast::Identifier(identifier) => &identifier.token,
			Ast::If(node) => &node.token,
			Ast::Infix(node) => &node.token,
			Ast::Let(node) => &node.token,
			Ast::Loop(node) => &node.token,
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
			Ast::Patch(node) => &node.token,
			Ast::Tuple(node) => &node.token,
			Ast::While(node) => &node.token,
			Ast::Wildcard(node) => &node.0,
			Ast::Window(node) => &node.token,
			Ast::StatementExpression(node) => node.expression.token(),
			Ast::Environment(node) => &node.token,
			Ast::Rownum(node) => &node.token,
			Ast::DefFunction(node) => &node.token,
			Ast::Return(node) => &node.token,
		}
	}

	pub fn value(&self) -> &str {
		match self {
			Ast::Identifier(ident) => ident.text(),
			_ => self.token().value(),
		}
	}
}

impl Ast {
	/// Returns true if this AST node is a DDL statement (CREATE, ALTER, DROP).
	pub fn is_ddl(&self) -> bool {
		matches!(self, Ast::Create(_) | Ast::Alter(_) | Ast::Drop(_))
	}

	pub fn is_aggregate(&self) -> bool {
		matches!(self, Ast::Aggregate(_))
	}
	pub fn as_aggregate(&self) -> &AstAggregate {
		if let Ast::Aggregate(result) = self {
			result
		} else {
			panic!("not aggregate")
		}
	}

	pub fn is_between(&self) -> bool {
		matches!(self, Ast::Between(_))
	}
	pub fn as_between(&self) -> &AstBetween {
		if let Ast::Between(result) = self {
			result
		} else {
			panic!("not between")
		}
	}

	pub fn is_call_function(&self) -> bool {
		matches!(self, Ast::CallFunction(_))
	}
	pub fn as_call_function(&self) -> &AstCallFunction {
		if let Ast::CallFunction(result) = self {
			result
		} else {
			panic!("not call function")
		}
	}

	pub fn is_block(&self) -> bool {
		matches!(self, Ast::Inline(_))
	}
	pub fn as_block(&self) -> &AstInline {
		if let Ast::Inline(result) = self {
			result
		} else {
			panic!("not block")
		}
	}

	pub fn is_cast(&self) -> bool {
		matches!(self, Ast::Cast(_))
	}
	pub fn as_cast(&self) -> &AstCast {
		if let Ast::Cast(result) = self {
			result
		} else {
			panic!("not cast")
		}
	}

	pub fn is_create(&self) -> bool {
		matches!(self, Ast::Create(_))
	}
	pub fn as_create(&self) -> &AstCreate {
		if let Ast::Create(result) = self {
			result
		} else {
			panic!("not create")
		}
	}

	pub fn is_alter(&self) -> bool {
		matches!(self, Ast::Alter(_))
	}
	pub fn as_alter(&self) -> &AstAlter {
		if let Ast::Alter(result) = self {
			result
		} else {
			panic!("not alter")
		}
	}

	pub fn is_describe(&self) -> bool {
		matches!(self, Ast::Describe(_))
	}
	pub fn as_describe(&self) -> &AstDescribe {
		if let Ast::Describe(result) = self {
			result
		} else {
			panic!("not describe")
		}
	}

	pub fn is_filter(&self) -> bool {
		matches!(self, Ast::Filter(_))
	}
	pub fn as_filter(&self) -> &AstFilter {
		if let Ast::Filter(result) = self {
			result
		} else {
			panic!("not filter")
		}
	}

	pub fn is_from(&self) -> bool {
		matches!(self, Ast::From(_))
	}
	pub fn as_from(&self) -> &AstFrom {
		if let Ast::From(result) = self {
			result
		} else {
			panic!("not from")
		}
	}

	pub fn is_identifier(&self) -> bool {
		matches!(self, Ast::Identifier(_))
	}
	pub fn as_identifier(&self) -> &UnqualifiedIdentifier {
		if let Ast::Identifier(result) = self {
			result
		} else {
			panic!("not identifier")
		}
	}

	pub fn is_if(&self) -> bool {
		matches!(self, Ast::If(_))
	}
	pub fn as_if(&self) -> &AstIf {
		if let Ast::If(result) = self {
			result
		} else {
			panic!("not if")
		}
	}

	pub fn is_infix(&self) -> bool {
		matches!(self, Ast::Infix(_))
	}
	pub fn as_infix(&self) -> &AstInfix {
		if let Ast::Infix(result) = self {
			result
		} else {
			panic!("not infix")
		}
	}

	pub fn is_let(&self) -> bool {
		matches!(self, Ast::Let(_))
	}
	pub fn as_let(&self) -> &AstLet {
		if let Ast::Let(result) = self {
			result
		} else {
			panic!("not let")
		}
	}

	pub fn is_variable(&self) -> bool {
		matches!(self, Ast::Variable(_))
	}
	pub fn as_variable(&self) -> &AstVariable {
		if let Ast::Variable(result) = self {
			result
		} else {
			panic!("not variable")
		}
	}

	pub fn as_environment(&self) -> &AstEnvironment {
		if let Ast::Environment(result) = self {
			result
		} else {
			panic!("not environment")
		}
	}

	pub fn is_delete(&self) -> bool {
		matches!(self, Ast::Delete(_))
	}
	pub fn as_delete(&self) -> &AstDelete {
		if let Ast::Delete(result) = self {
			result
		} else {
			panic!("not delete")
		}
	}

	pub fn is_insert(&self) -> bool {
		matches!(self, Ast::Insert(_))
	}
	pub fn as_insert(&self) -> &AstInsert {
		if let Ast::Insert(result) = self {
			result
		} else {
			panic!("not insert")
		}
	}

	pub fn is_update(&self) -> bool {
		matches!(self, Ast::Update(_))
	}
	pub fn as_update(&self) -> &AstUpdate {
		if let Ast::Update(result) = self {
			result
		} else {
			panic!("not update")
		}
	}

	pub fn is_join(&self) -> bool {
		matches!(self, Ast::Join(_))
	}
	pub fn as_join(&self) -> &AstJoin {
		if let Ast::Join(result) = self {
			result
		} else {
			panic!("not join")
		}
	}

	pub fn is_merge(&self) -> bool {
		matches!(self, Ast::Merge(_))
	}
	pub fn as_merge(&self) -> &AstMerge {
		if let Ast::Merge(result) = self {
			result
		} else {
			panic!("not merge")
		}
	}

	pub fn is_take(&self) -> bool {
		matches!(self, Ast::Take(_))
	}
	pub fn as_take(&self) -> &AstTake {
		if let Ast::Take(result) = self {
			result
		} else {
			panic!("not take")
		}
	}

	pub fn is_list(&self) -> bool {
		matches!(self, Ast::List(_))
	}
	pub fn as_list(&self) -> &AstList {
		if let Ast::List(result) = self {
			result
		} else {
			panic!("not list")
		}
	}

	pub fn is_literal(&self) -> bool {
		matches!(self, Ast::Literal(_))
	}

	pub fn as_literal(&self) -> &AstLiteral {
		if let Ast::Literal(result) = self {
			result
		} else {
			panic!("not literal")
		}
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
		if let Ast::Sort(result) = self {
			result
		} else {
			panic!("not sort")
		}
	}
	pub fn is_policy(&self) -> bool {
		matches!(self, Ast::Policy(_))
	}
	pub fn as_policy(&self) -> &AstPolicy {
		if let Ast::Policy(result) = self {
			result
		} else {
			panic!("not policy")
		}
	}

	pub fn is_policy_block(&self) -> bool {
		matches!(self, Ast::PolicyBlock(_))
	}
	pub fn as_policy_block(&self) -> &AstPolicyBlock {
		if let Ast::PolicyBlock(result) = self {
			result
		} else {
			panic!("not policy block")
		}
	}

	pub fn is_inline(&self) -> bool {
		matches!(self, Ast::Inline(_))
	}
	pub fn as_inline(&self) -> &AstInline {
		if let Ast::Inline(result) = self {
			result
		} else {
			panic!("not inline")
		}
	}

	pub fn is_prefix(&self) -> bool {
		matches!(self, Ast::Prefix(_))
	}
	pub fn as_prefix(&self) -> &AstPrefix {
		if let Ast::Prefix(result) = self {
			result
		} else {
			panic!("not prefix")
		}
	}

	pub fn is_map(&self) -> bool {
		matches!(self, Ast::Map(_))
	}

	pub fn as_map(&self) -> &AstMap {
		if let Ast::Map(result) = self {
			result
		} else {
			panic!("not map")
		}
	}

	pub fn is_generator(&self) -> bool {
		matches!(self, Ast::Generator(_))
	}

	pub fn as_generator(&self) -> &AstGenerator {
		if let Ast::Generator(result) = self {
			result
		} else {
			panic!("not generator")
		}
	}

	pub fn as_apply(&self) -> &AstApply {
		if let Ast::Apply(result) = self {
			result
		} else {
			panic!("not apply")
		}
	}

	pub fn as_extend(&self) -> &AstExtend {
		if let Ast::Extend(result) = self {
			result
		} else {
			panic!("not extend")
		}
	}

	pub fn is_patch(&self) -> bool {
		matches!(self, Ast::Patch(_))
	}

	pub fn as_patch(&self) -> &AstPatch {
		if let Ast::Patch(result) = self {
			result
		} else {
			panic!("not patch")
		}
	}

	pub fn is_tuple(&self) -> bool {
		matches!(self, Ast::Tuple(_))
	}

	pub fn as_tuple(&self) -> &AstTuple {
		if let Ast::Tuple(result) = self {
			result
		} else {
			panic!("not tuple")
		}
	}

	pub fn is_window(&self) -> bool {
		matches!(self, Ast::Window(_))
	}

	pub fn as_window(&self) -> &AstWindow {
		if let Ast::Window(result) = self {
			result
		} else {
			panic!("not window")
		}
	}

	pub fn is_statement_expression(&self) -> bool {
		matches!(self, Ast::StatementExpression(_))
	}

	pub fn as_statement_expression(&self) -> &AstStatementExpression {
		if let Ast::StatementExpression(result) = self {
			result
		} else {
			panic!("not statement expression")
		}
	}

	pub fn is_rownum(&self) -> bool {
		matches!(self, Ast::Rownum(_))
	}

	pub fn as_rownum(&self) -> &AstRownum {
		if let Ast::Rownum(result) = self {
			result
		} else {
			panic!("not rownum")
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCast {
	pub token: Token,
	pub tuple: AstTuple,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstApply {
	pub token: Token,
	pub operator: UnqualifiedIdentifier,
	pub expressions: Vec<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCall {
	pub token: Token,
	pub operator: UnqualifiedIdentifier,
	pub arguments: AstTuple,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCallFunction {
	pub token: Token,
	pub function: MaybeQualifiedFunctionIdentifier,
	pub arguments: AstTuple,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInlineKeyedValue {
	pub key: UnqualifiedIdentifier,
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
	DeferredView(AstCreateDeferredView),
	TransactionalView(AstCreateTransactionalView),
	Flow(AstCreateFlow),
	Namespace(AstCreateNamespace),
	Series(AstCreateSeries),
	Subscription(AstCreateSubscription),
	Table(AstCreateTable),
	RingBuffer(AstCreateRingBuffer),
	Dictionary(AstCreateDictionary),
	Index(AstCreateIndex),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlter {
	Sequence(AstAlterSequence),
	Table(AstAlterTable),
	View(AstAlterView),
	Flow(AstAlterFlow),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDrop {
	Flow(AstDropFlow),
	// Future: Table, View, Namespace, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropFlow {
	pub token: Token,
	pub if_exists: bool,
	pub flow: MaybeQualifiedFlowIdentifier,
	pub cascade: bool, // CASCADE or RESTRICT (false = RESTRICT)
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterSequence {
	pub token: Token,
	pub sequence: MaybeQualifiedSequenceIdentifier,
	pub column: Fragment,
	pub value: AstLiteral,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterTable {
	pub token: Token,
	pub table: MaybeQualifiedTableIdentifier,
	pub operations: Vec<AstAlterTableOperation>,
}

/// Represents a subquery - a complete query statement enclosed in braces
/// Used in contexts like joins, CTEs, and derived tables
#[derive(Debug, Clone, PartialEq)]
pub struct AstSubQuery {
	pub token: Token,
	pub statement: AstStatement,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlterTableOperation {
	CreatePrimaryKey {
		name: Option<Fragment>,
		columns: Vec<AstIndexColumn>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterView {
	pub token: Token,
	pub view: crate::ast::identifier::MaybeQualifiedViewIdentifier,
	pub operations: Vec<AstAlterViewOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlterViewOperation {
	CreatePrimaryKey {
		name: Option<Fragment>,
		columns: Vec<AstIndexColumn>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterFlow {
	pub token: Token,
	pub flow: MaybeQualifiedFlowIdentifier,
	pub action: AstAlterFlowAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstAlterFlowAction {
	Rename {
		new_name: Fragment,
	},
	SetQuery {
		query: AstStatement,
	},
	Pause,
	Resume,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDeferredView {
	pub token: Token,
	pub view: MaybeQualifiedDeferredViewIdentifier,
	pub columns: Vec<AstColumnToCreate>,
	pub as_clause: Option<AstStatement>,
	pub primary_key: Option<AstPrimaryKeyDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTransactionalView {
	pub token: Token,
	pub view: MaybeQualifiedTransactionalViewIdentifier,
	pub columns: Vec<AstColumnToCreate>,
	pub as_clause: Option<AstStatement>,
	pub primary_key: Option<AstPrimaryKeyDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateFlow {
	pub token: Token,
	pub or_replace: bool,
	pub if_not_exists: bool,
	pub flow: MaybeQualifiedFlowIdentifier,
	pub as_clause: AstStatement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateNamespace {
	pub token: Token,
	pub namespace: MaybeQualifiedNamespaceIdentifier,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSeries {
	pub token: Token,
	pub sequence: MaybeQualifiedSequenceIdentifier,
	pub columns: Vec<AstColumnToCreate>,
}

/// CREATE SUBSCRIPTION with columns.
/// Subscriptions are identified only by UUID v7, not by name.
#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateSubscription {
	pub token: Token,
	pub columns: Vec<AstColumnToCreate>,
	pub as_clause: Option<AstStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateTable {
	pub token: Token,
	pub table: MaybeQualifiedTableIdentifier,
	pub columns: Vec<AstColumnToCreate>,
	pub primary_key: Option<AstPrimaryKeyDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateRingBuffer {
	pub token: Token,
	pub ringbuffer: crate::ast::identifier::MaybeQualifiedRingBufferIdentifier,
	pub columns: Vec<AstColumnToCreate>,
	pub capacity: u64,
	pub primary_key: Option<AstPrimaryKeyDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDictionary {
	pub token: Token,
	pub if_not_exists: bool,
	pub dictionary: MaybeQualifiedDictionaryIdentifier,
	pub value_type: AstDataType,
	pub id_type: AstDataType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDescribe {
	Query {
		token: Token,
		node: Box<Ast>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDataType {
	Unconstrained(Fragment), // UTF8, BLOB, etc.
	Constrained {
		name: Fragment,
		params: Vec<AstLiteral>,
	}, // UTF8(50), DECIMAL(10,2)
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstColumnToCreate {
	pub name: Fragment,
	pub ty: AstDataType,
	pub policies: Option<AstPolicyBlock>,
	pub auto_increment: bool,
	pub dictionary: Option<MaybeQualifiedDictionaryIdentifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateIndex {
	pub token: Token,
	pub index_type: IndexType,
	pub index: MaybeQualifiedIndexIdentifier,
	pub columns: Vec<AstIndexColumn>,
	pub filters: Vec<Box<Ast>>,
	pub map: Option<Box<Ast>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIndexColumn {
	pub column: MaybeQualifiedColumnIdentifier,
	pub order: Option<SortDirection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPrimaryKeyDef {
	pub columns: Vec<AstIndexColumn>,
}

impl AstCreate {
	pub fn token(&self) -> &Token {
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
			AstCreate::Subscription(AstCreateSubscription {
				token,
				..
			}) => token,
		}
	}
}

impl AstAlter {
	pub fn token(&self) -> &Token {
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

impl AstDrop {
	pub fn token(&self) -> &Token {
		match self {
			AstDrop::Flow(AstDropFlow {
				token,
				..
			}) => token,
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
	Source {
		token: Token,
		source: UnresolvedPrimitiveIdentifier,
		index_name: Option<Fragment>,
	},
	Variable {
		token: Token,
		variable: AstVariable,
	},
	Environment {
		token: Token,
	},
	Inline {
		token: Token,
		list: AstList,
	},
	Generator(AstGenerator),
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
	pub fn fragment(self) -> Fragment {
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
pub enum InfixOperator {
	Add(Token),
	As(Token),
	Arrow(Token),
	AccessNamespace(Token),
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
	In(Token),
	NotIn(Token),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInfix {
	pub token: Token,
	pub left: Box<Ast>,
	pub operator: InfixOperator,
	pub right: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LetValue {
	Expression(Box<Ast>),    // scalar/column expression
	Statement(AstStatement), // FROM … | …
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLet {
	pub token: Token,
	pub name: UnqualifiedIdentifier,
	pub value: LetValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDelete {
	pub token: Token,
	pub target: UnresolvedPrimitiveIdentifier,
	pub filter: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstInsert {
	pub token: Token,
	pub target: UnresolvedPrimitiveIdentifier,
	pub source: Box<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstUpdate {
	pub token: Token,
	pub target: UnresolvedPrimitiveIdentifier,
	pub assignments: Vec<Ast>,
	pub filter: Box<Ast>,
}

/// Connector between join condition pairs
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum JoinConnector {
	#[default]
	And,
	Or,
}

/// A pair of expressions in a join using clause: (expr1, expr2)
#[derive(Debug, Clone, PartialEq)]
pub struct AstJoinExpressionPair {
	pub first: Box<Ast>,
	pub second: Box<Ast>,
	pub connector: Option<JoinConnector>, // None for last pair
}

/// The using clause: using (a, b) and|or (c, d)
#[derive(Debug, Clone, PartialEq)]
pub struct AstUsingClause {
	pub token: Token,
	pub pairs: Vec<AstJoinExpressionPair>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstJoin {
	InnerJoin {
		token: Token,
		with: AstSubQuery,
		using_clause: AstUsingClause,
		alias: Fragment,
	},
	LeftJoin {
		token: Token,
		with: AstSubQuery,
		using_clause: AstUsingClause,
		alias: Fragment,
	},
	NaturalJoin {
		token: Token,
		with: AstSubQuery,
		join_type: Option<JoinType>,
		alias: Fragment, // Required alias (no 'as' keyword)
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstMerge {
	pub token: Token,
	pub with: AstSubQuery,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNumber(pub Token);

impl AstLiteralNumber {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralTemporal(pub Token);

impl AstLiteralTemporal {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralText(pub Token);

impl AstLiteralText {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralBoolean(pub Token);

impl AstLiteralBoolean {
	pub fn value(&self) -> bool {
		self.0.kind == TokenKind::Literal(Literal::True)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralUndefined(pub Token);

impl AstLiteralUndefined {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDistinct {
	pub token: Token,
	pub columns: Vec<MaybeQualifiedColumnIdentifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSort {
	pub token: Token,
	pub columns: Vec<MaybeQualifiedColumnIdentifier>,
	pub directions: Vec<Option<Fragment>>,
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
pub struct AstGenerator {
	pub token: Token,
	pub name: Fragment,
	pub nodes: Vec<Ast>,
}

impl Index<usize> for AstGenerator {
	type Output = Ast;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl AstGenerator {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstExtend {
	pub token: Token,
	pub nodes: Vec<Ast>,
}

impl Index<usize> for AstExtend {
	type Output = Ast;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl AstExtend {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPatch {
	pub token: Token,
	pub assignments: Vec<Ast>,
}

impl AstPatch {
	pub fn len(&self) -> usize {
		self.assignments.len()
	}
}

impl Index<usize> for AstPatch {
	type Output = Ast;

	fn index(&self, index: usize) -> &Self::Output {
		&self.assignments[index]
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

#[derive(Debug, Clone, PartialEq)]
pub struct AstVariable {
	pub token: Token,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstRownum {
	pub token: Token,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstEnvironment {
	pub token: Token,
}

impl AstVariable {
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
pub struct AstBlock {
	pub token: Token,
	pub statements: Vec<AstStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLoop {
	pub token: Token,
	pub body: AstBlock,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWhile {
	pub token: Token,
	pub condition: Box<Ast>,
	pub body: AstBlock,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstFor {
	pub token: Token,
	pub variable: AstVariable,
	pub iterable: Box<Ast>,
	pub body: AstBlock,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstBreak {
	pub token: Token,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstContinue {
	pub token: Token,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIf {
	pub token: Token,
	pub condition: Box<Ast>,
	pub then_block: AstBlock,
	pub else_ifs: Vec<AstElseIf>,
	pub else_block: Option<AstBlock>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstElseIf {
	pub token: Token,
	pub condition: Box<Ast>,
	pub then_block: AstBlock,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWindow {
	pub token: Token,
	pub config: Vec<AstWindowConfig>,
	pub aggregations: Vec<Ast>,
	pub group_by: Vec<Ast>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWindowConfig {
	pub key: UnqualifiedIdentifier,
	pub value: Ast,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstStatementExpression {
	pub expression: Box<Ast>,
}

/// Function parameter (always has $ prefix)
#[derive(Debug, Clone, PartialEq)]
pub struct AstFunctionParameter {
	pub token: Token,
	pub variable: AstVariable,
	pub type_annotation: Option<AstDataType>,
}

/// Function definition
#[derive(Debug, Clone, PartialEq)]
pub struct AstDefFunction {
	pub token: Token,
	pub name: UnqualifiedIdentifier,
	pub parameters: Vec<AstFunctionParameter>,
	pub return_type: Option<AstDataType>,
	pub body: AstBlock,
}

/// Return statement
#[derive(Debug, Clone, PartialEq)]
pub struct AstReturn {
	pub token: Token,
	pub value: Option<Box<Ast>>,
}
