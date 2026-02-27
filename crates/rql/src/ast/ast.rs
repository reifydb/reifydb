// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Index;

use reifydb_core::{
	common::{IndexType, JoinType},
	sort::SortDirection,
};

use crate::{
	ast::identifier::{
		MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
		MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFlowIdentifier, MaybeQualifiedFunctionIdentifier,
		MaybeQualifiedIndexIdentifier, MaybeQualifiedNamespaceIdentifier, MaybeQualifiedProcedureIdentifier,
		MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSequenceIdentifier, MaybeQualifiedSeriesIdentifier,
		MaybeQualifiedSumTypeIdentifier, MaybeQualifiedTableIdentifier,
		MaybeQualifiedTransactionalViewIdentifier, MaybeQualifiedViewIdentifier, UnqualifiedIdentifier,
		UnresolvedPrimitiveIdentifier,
	},
	bump::{BumpBox, BumpFragment},
	token::token::{Literal, Token, TokenKind},
};

#[derive(Debug)]
pub struct AstStatement<'bump> {
	pub nodes: Vec<Ast<'bump>>,
	pub has_pipes: bool,
	pub is_output: bool,
}

impl<'bump> AstStatement<'bump> {
	pub fn first_unchecked(&self) -> &Ast<'bump> {
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

impl<'bump> Index<usize> for AstStatement<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		self.nodes.index(index)
	}
}

impl<'bump> IntoIterator for AstStatement<'bump> {
	type Item = Ast<'bump>;
	type IntoIter = std::vec::IntoIter<Self::Item>;

	fn into_iter(self) -> Self::IntoIter {
		self.nodes.into_iter()
	}
}

#[derive(Debug)]
pub enum Ast<'bump> {
	Aggregate(AstAggregate<'bump>),
	Append(AstAppend<'bump>),
	Assert(AstAssert<'bump>),
	Apply(AstApply<'bump>),
	Between(AstBetween<'bump>),
	Block(AstBlock<'bump>),
	Break(AstBreak<'bump>),
	Call(AstCall<'bump>),
	CallFunction(AstCallFunction<'bump>),
	Cast(AstCast<'bump>),
	Continue(AstContinue<'bump>),
	Create(AstCreate<'bump>),
	Alter(AstAlter<'bump>),
	Drop(AstDrop<'bump>),
	Describe(AstDescribe<'bump>),
	Distinct(AstDistinct<'bump>),
	Filter(AstFilter<'bump>),
	For(AstFor<'bump>),
	From(AstFrom<'bump>),
	Identifier(UnqualifiedIdentifier<'bump>),
	If(AstIf<'bump>),
	Infix(AstInfix<'bump>),
	Inline(AstInline<'bump>),
	Let(AstLet<'bump>),
	Loop(AstLoop<'bump>),
	Delete(AstDelete<'bump>),
	Insert(AstInsert<'bump>),
	Update(AstUpdate<'bump>),
	Join(AstJoin<'bump>),
	Take(AstTake<'bump>),
	List(AstList<'bump>),
	Literal(AstLiteral<'bump>),
	Nop,
	Variable(AstVariable<'bump>),
	Environment(AstEnvironment<'bump>),
	Sort(AstSort<'bump>),
	SubQuery(AstSubQuery<'bump>),
	Prefix(AstPrefix<'bump>),
	Map(AstMap<'bump>),
	Generator(AstGenerator<'bump>),
	Extend(AstExtend<'bump>),
	Patch(AstPatch<'bump>),
	Tuple(AstTuple<'bump>),
	While(AstWhile<'bump>),
	Wildcard(AstWildcard<'bump>),
	Window(AstWindow<'bump>),
	StatementExpression(AstStatementExpression<'bump>),
	Rownum(AstRownum<'bump>),
	DefFunction(AstDefFunction<'bump>),
	Return(AstReturn<'bump>),
	SumTypeConstructor(AstSumTypeConstructor<'bump>),
	IsVariant(AstIsVariant<'bump>),
	Match(AstMatch<'bump>),
	Closure(AstClosure<'bump>),
	Dispatch(AstDispatch<'bump>),
	Grant(AstGrant<'bump>),
	Revoke(AstRevoke<'bump>),
	Identity(AstIdentity<'bump>),
	Require(AstRequire<'bump>),
	Migrate(AstMigrate<'bump>),
	RollbackMigration(AstRollbackMigration<'bump>),
}

impl<'bump> Default for Ast<'bump> {
	fn default() -> Self {
		Self::Nop
	}
}

impl<'bump> Ast<'bump> {
	pub fn token(&self) -> &Token<'bump> {
		match self {
			Ast::Inline(node) => &node.token,
			Ast::Append(node) => node.token(),
			Ast::Assert(node) => &node.token,
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
				AstLiteral::None(node) => &node.0,
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
			Ast::Variable(node) => &node.token,
			Ast::Sort(node) => &node.token,
			Ast::SubQuery(node) => &node.token,
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
			Ast::SumTypeConstructor(node) => &node.token,
			Ast::IsVariant(node) => &node.token,
			Ast::Match(node) => &node.token,
			Ast::Closure(node) => &node.token,
			Ast::Dispatch(node) => &node.token,
			Ast::Grant(node) => &node.token,
			Ast::Revoke(node) => &node.token,
			Ast::Identity(node) => &node.token,
			Ast::Require(node) => &node.token,
			Ast::Migrate(node) => &node.token,
			Ast::RollbackMigration(node) => &node.token,
		}
	}

	pub fn value(&self) -> &str {
		match self {
			Ast::Identifier(ident) => ident.text(),
			_ => self.token().value(),
		}
	}
}

impl<'bump> Ast<'bump> {
	/// Returns true if this AST node is a DDL statement (CREATE, ALTER, DROP).
	pub fn is_ddl(&self) -> bool {
		matches!(
			self,
			Ast::Create(_)
				| Ast::Alter(_) | Ast::Drop(_)
				| Ast::Grant(_) | Ast::Revoke(_)
				| Ast::Migrate(_) | Ast::RollbackMigration(_)
		)
	}

	pub fn is_dispatch(&self) -> bool {
		matches!(self, Ast::Dispatch(_))
	}
	pub fn as_dispatch(&self) -> &AstDispatch<'bump> {
		if let Ast::Dispatch(result) = self {
			result
		} else {
			panic!("not dispatch")
		}
	}

	pub fn is_assert(&self) -> bool {
		matches!(self, Ast::Assert(_))
	}
	pub fn as_assert(&self) -> &AstAssert<'bump> {
		if let Ast::Assert(result) = self {
			result
		} else {
			panic!("not assert")
		}
	}

	pub fn is_aggregate(&self) -> bool {
		matches!(self, Ast::Aggregate(_))
	}
	pub fn as_aggregate(&self) -> &AstAggregate<'bump> {
		if let Ast::Aggregate(result) = self {
			result
		} else {
			panic!("not aggregate")
		}
	}

	pub fn is_between(&self) -> bool {
		matches!(self, Ast::Between(_))
	}
	pub fn as_between(&self) -> &AstBetween<'bump> {
		if let Ast::Between(result) = self {
			result
		} else {
			panic!("not between")
		}
	}

	pub fn is_call_function(&self) -> bool {
		matches!(self, Ast::CallFunction(_))
	}
	pub fn as_call_function(&self) -> &AstCallFunction<'bump> {
		if let Ast::CallFunction(result) = self {
			result
		} else {
			panic!("not call function")
		}
	}

	pub fn is_block(&self) -> bool {
		matches!(self, Ast::Inline(_))
	}
	pub fn as_block(&self) -> &AstInline<'bump> {
		if let Ast::Inline(result) = self {
			result
		} else {
			panic!("not block")
		}
	}

	pub fn is_cast(&self) -> bool {
		matches!(self, Ast::Cast(_))
	}
	pub fn as_cast(&self) -> &AstCast<'bump> {
		if let Ast::Cast(result) = self {
			result
		} else {
			panic!("not cast")
		}
	}

	pub fn is_create(&self) -> bool {
		matches!(self, Ast::Create(_))
	}
	pub fn as_create(&self) -> &AstCreate<'bump> {
		if let Ast::Create(result) = self {
			result
		} else {
			panic!("not create")
		}
	}

	pub fn is_alter(&self) -> bool {
		matches!(self, Ast::Alter(_))
	}
	pub fn as_alter(&self) -> &AstAlter<'bump> {
		if let Ast::Alter(result) = self {
			result
		} else {
			panic!("not alter")
		}
	}

	pub fn is_describe(&self) -> bool {
		matches!(self, Ast::Describe(_))
	}
	pub fn as_describe(&self) -> &AstDescribe<'bump> {
		if let Ast::Describe(result) = self {
			result
		} else {
			panic!("not describe")
		}
	}

	pub fn is_filter(&self) -> bool {
		matches!(self, Ast::Filter(_))
	}
	pub fn as_filter(&self) -> &AstFilter<'bump> {
		if let Ast::Filter(result) = self {
			result
		} else {
			panic!("not filter")
		}
	}

	pub fn is_from(&self) -> bool {
		matches!(self, Ast::From(_))
	}
	pub fn as_from(&self) -> &AstFrom<'bump> {
		if let Ast::From(result) = self {
			result
		} else {
			panic!("not from")
		}
	}

	pub fn is_identifier(&self) -> bool {
		matches!(self, Ast::Identifier(_))
	}
	pub fn as_identifier(&self) -> &UnqualifiedIdentifier<'bump> {
		if let Ast::Identifier(result) = self {
			result
		} else {
			panic!("not identifier")
		}
	}

	pub fn is_if(&self) -> bool {
		matches!(self, Ast::If(_))
	}
	pub fn as_if(&self) -> &AstIf<'bump> {
		if let Ast::If(result) = self {
			result
		} else {
			panic!("not if")
		}
	}

	pub fn is_infix(&self) -> bool {
		matches!(self, Ast::Infix(_))
	}
	pub fn as_infix(&self) -> &AstInfix<'bump> {
		if let Ast::Infix(result) = self {
			result
		} else {
			panic!("not infix")
		}
	}

	pub fn is_let(&self) -> bool {
		matches!(self, Ast::Let(_))
	}
	pub fn as_let(&self) -> &AstLet<'bump> {
		if let Ast::Let(result) = self {
			result
		} else {
			panic!("not let")
		}
	}

	pub fn is_variable(&self) -> bool {
		matches!(self, Ast::Variable(_))
	}
	pub fn as_variable(&self) -> &AstVariable<'bump> {
		if let Ast::Variable(result) = self {
			result
		} else {
			panic!("not variable")
		}
	}

	pub fn as_environment(&self) -> &AstEnvironment<'bump> {
		if let Ast::Environment(result) = self {
			result
		} else {
			panic!("not environment")
		}
	}

	pub fn is_delete(&self) -> bool {
		matches!(self, Ast::Delete(_))
	}
	pub fn as_delete(&self) -> &AstDelete<'bump> {
		if let Ast::Delete(result) = self {
			result
		} else {
			panic!("not delete")
		}
	}

	pub fn is_insert(&self) -> bool {
		matches!(self, Ast::Insert(_))
	}
	pub fn as_insert(&self) -> &AstInsert<'bump> {
		if let Ast::Insert(result) = self {
			result
		} else {
			panic!("not insert")
		}
	}

	pub fn is_update(&self) -> bool {
		matches!(self, Ast::Update(_))
	}
	pub fn as_update(&self) -> &AstUpdate<'bump> {
		if let Ast::Update(result) = self {
			result
		} else {
			panic!("not update")
		}
	}

	pub fn is_join(&self) -> bool {
		matches!(self, Ast::Join(_))
	}
	pub fn as_join(&self) -> &AstJoin<'bump> {
		if let Ast::Join(result) = self {
			result
		} else {
			panic!("not join")
		}
	}

	pub fn is_take(&self) -> bool {
		matches!(self, Ast::Take(_))
	}
	pub fn as_take(&self) -> &AstTake<'bump> {
		if let Ast::Take(result) = self {
			result
		} else {
			panic!("not take")
		}
	}

	pub fn is_list(&self) -> bool {
		matches!(self, Ast::List(_))
	}
	pub fn as_list(&self) -> &AstList<'bump> {
		if let Ast::List(result) = self {
			result
		} else {
			panic!("not list")
		}
	}

	pub fn is_literal(&self) -> bool {
		matches!(self, Ast::Literal(_))
	}

	pub fn as_literal(&self) -> &AstLiteral<'bump> {
		if let Ast::Literal(result) = self {
			result
		} else {
			panic!("not literal")
		}
	}

	pub fn is_literal_boolean(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Boolean(_)))
	}

	pub fn as_literal_boolean(&self) -> &AstLiteralBoolean<'bump> {
		if let Ast::Literal(AstLiteral::Boolean(result)) = self {
			result
		} else {
			panic!("not literal boolean")
		}
	}

	pub fn is_literal_number(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Number(_)))
	}

	pub fn as_literal_number(&self) -> &AstLiteralNumber<'bump> {
		if let Ast::Literal(AstLiteral::Number(result)) = self {
			result
		} else {
			panic!("not literal number")
		}
	}

	pub fn is_literal_temporal(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Temporal(_)))
	}

	pub fn as_literal_temporal(&self) -> &AstLiteralTemporal<'bump> {
		if let Ast::Literal(AstLiteral::Temporal(result)) = self {
			result
		} else {
			panic!("not literal temporal")
		}
	}

	pub fn is_literal_text(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::Text(_)))
	}

	pub fn as_literal_text(&self) -> &AstLiteralText<'bump> {
		if let Ast::Literal(AstLiteral::Text(result)) = self {
			result
		} else {
			panic!("not literal text")
		}
	}

	pub fn is_literal_none(&self) -> bool {
		matches!(self, Ast::Literal(AstLiteral::None(_)))
	}

	pub fn as_literal_none(&self) -> &AstLiteralNone<'bump> {
		if let Ast::Literal(AstLiteral::None(result)) = self {
			result
		} else {
			panic!("not literal none")
		}
	}

	pub fn is_sort(&self) -> bool {
		matches!(self, Ast::Sort(_))
	}
	pub fn as_sort(&self) -> &AstSort<'bump> {
		if let Ast::Sort(result) = self {
			result
		} else {
			panic!("not sort")
		}
	}
	pub fn is_inline(&self) -> bool {
		matches!(self, Ast::Inline(_))
	}
	pub fn as_inline(&self) -> &AstInline<'bump> {
		if let Ast::Inline(result) = self {
			result
		} else {
			panic!("not inline")
		}
	}

	pub fn is_prefix(&self) -> bool {
		matches!(self, Ast::Prefix(_))
	}
	pub fn as_prefix(&self) -> &AstPrefix<'bump> {
		if let Ast::Prefix(result) = self {
			result
		} else {
			panic!("not prefix")
		}
	}

	pub fn is_map(&self) -> bool {
		matches!(self, Ast::Map(_))
	}

	pub fn as_map(&self) -> &AstMap<'bump> {
		if let Ast::Map(result) = self {
			result
		} else {
			panic!("not map")
		}
	}

	pub fn is_generator(&self) -> bool {
		matches!(self, Ast::Generator(_))
	}

	pub fn as_generator(&self) -> &AstGenerator<'bump> {
		if let Ast::Generator(result) = self {
			result
		} else {
			panic!("not generator")
		}
	}

	pub fn as_apply(&self) -> &AstApply<'bump> {
		if let Ast::Apply(result) = self {
			result
		} else {
			panic!("not apply")
		}
	}

	pub fn as_extend(&self) -> &AstExtend<'bump> {
		if let Ast::Extend(result) = self {
			result
		} else {
			panic!("not extend")
		}
	}

	pub fn is_patch(&self) -> bool {
		matches!(self, Ast::Patch(_))
	}

	pub fn as_patch(&self) -> &AstPatch<'bump> {
		if let Ast::Patch(result) = self {
			result
		} else {
			panic!("not patch")
		}
	}

	pub fn is_tuple(&self) -> bool {
		matches!(self, Ast::Tuple(_))
	}

	pub fn as_tuple(&self) -> &AstTuple<'bump> {
		if let Ast::Tuple(result) = self {
			result
		} else {
			panic!("not tuple")
		}
	}

	pub fn is_window(&self) -> bool {
		matches!(self, Ast::Window(_))
	}

	pub fn as_window(&self) -> &AstWindow<'bump> {
		if let Ast::Window(result) = self {
			result
		} else {
			panic!("not window")
		}
	}

	pub fn is_statement_expression(&self) -> bool {
		matches!(self, Ast::StatementExpression(_))
	}

	pub fn as_statement_expression(&self) -> &AstStatementExpression<'bump> {
		if let Ast::StatementExpression(result) = self {
			result
		} else {
			panic!("not statement expression")
		}
	}

	pub fn is_rownum(&self) -> bool {
		matches!(self, Ast::Rownum(_))
	}

	pub fn as_rownum(&self) -> &AstRownum<'bump> {
		if let Ast::Rownum(result) = self {
			result
		} else {
			panic!("not rownum")
		}
	}

	pub fn is_match(&self) -> bool {
		matches!(self, Ast::Match(_))
	}

	pub fn as_match(&self) -> &AstMatch<'bump> {
		if let Ast::Match(result) = self {
			result
		} else {
			panic!("not match")
		}
	}
}

#[derive(Debug)]
pub struct AstCast<'bump> {
	pub token: Token<'bump>,
	pub tuple: AstTuple<'bump>,
}

#[derive(Debug)]
pub struct AstApply<'bump> {
	pub token: Token<'bump>,
	pub operator: UnqualifiedIdentifier<'bump>,
	pub expressions: Vec<Ast<'bump>>,
}

#[derive(Debug)]
pub struct AstCall<'bump> {
	pub token: Token<'bump>,
	pub function: MaybeQualifiedFunctionIdentifier<'bump>,
	pub arguments: AstTuple<'bump>,
}

#[derive(Debug)]
pub struct AstCallFunction<'bump> {
	pub token: Token<'bump>,
	pub function: MaybeQualifiedFunctionIdentifier<'bump>,
	pub arguments: AstTuple<'bump>,
}

#[derive(Debug)]
pub struct AstInlineKeyedValue<'bump> {
	pub key: UnqualifiedIdentifier<'bump>,
	pub value: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug)]
pub struct AstInline<'bump> {
	pub token: Token<'bump>,
	pub keyed_values: Vec<AstInlineKeyedValue<'bump>>,
}

impl<'bump> AstInline<'bump> {
	pub fn len(&self) -> usize {
		self.keyed_values.len()
	}
}

impl<'bump> Index<usize> for AstInline<'bump> {
	type Output = AstInlineKeyedValue<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.keyed_values[index]
	}
}

#[derive(Debug)]
pub struct AstSumTypeConstructor<'bump> {
	pub token: Token<'bump>,
	pub namespace: BumpFragment<'bump>,
	pub sumtype_name: BumpFragment<'bump>,
	pub variant_name: BumpFragment<'bump>,
	pub columns: AstInline<'bump>,
}

#[derive(Debug)]
pub struct AstIsVariant<'bump> {
	pub token: Token<'bump>,
	pub expression: BumpBox<'bump, Ast<'bump>>,
	pub namespace: Option<BumpFragment<'bump>>,
	pub sumtype_name: BumpFragment<'bump>,
	pub variant_name: BumpFragment<'bump>,
}

#[derive(Debug)]
pub enum AstCreate<'bump> {
	DeferredView(AstCreateDeferredView<'bump>),
	TransactionalView(AstCreateTransactionalView<'bump>),
	Flow(AstCreateFlow<'bump>),
	Namespace(AstCreateNamespace<'bump>),
	Series(AstCreateSeries<'bump>),
	Subscription(AstCreateSubscription<'bump>),
	Table(AstCreateTable<'bump>),
	RingBuffer(AstCreateRingBuffer<'bump>),
	Dictionary(AstCreateDictionary<'bump>),
	Enum(AstCreateSumType<'bump>),
	Index(AstCreateIndex<'bump>),
	PrimaryKey(AstCreatePrimaryKey<'bump>),
	ColumnProperty(AstCreateColumnProperty<'bump>),
	Procedure(AstCreateProcedure<'bump>),
	Event(AstCreateEvent<'bump>),
	Tag(AstCreateTag<'bump>),
	Handler(AstCreateHandler<'bump>),
	User(AstCreateUser<'bump>),
	Role(AstCreateRole<'bump>),
	Authentication(AstCreateAuthentication<'bump>),
	SecurityPolicy(AstCreateSecurityPolicy<'bump>),
	Migration(AstCreateMigration<'bump>),
}

#[derive(Debug)]
pub enum AstAlter<'bump> {
	Sequence(AstAlterSequence<'bump>),
	Flow(AstAlterFlow<'bump>),
	SecurityPolicy(AstAlterSecurityPolicy<'bump>),
	Table(AstAlterTable<'bump>),
}

#[derive(Debug)]
pub struct AstAlterTable<'bump> {
	pub token: Token<'bump>,
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub action: AstAlterTableAction<'bump>,
}

#[derive(Debug)]
pub enum AstAlterTableAction<'bump> {
	AddColumn {
		column: AstColumnToCreate<'bump>,
	},
	DropColumn {
		column: BumpFragment<'bump>,
	},
	RenameColumn {
		old_name: BumpFragment<'bump>,
		new_name: BumpFragment<'bump>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstDrop<'bump> {
	Flow(AstDropFlow<'bump>),
	Table(AstDropTable<'bump>),
	View(AstDropView<'bump>),
	RingBuffer(AstDropRingBuffer<'bump>),
	Namespace(AstDropNamespace<'bump>),
	Dictionary(AstDropDictionary<'bump>),
	Enum(AstDropSumType<'bump>),
	Subscription(AstDropSubscription<'bump>),
	Series(AstDropSeries<'bump>),
	User(AstDropUser<'bump>),
	Role(AstDropRole<'bump>),
	Authentication(AstDropAuthentication<'bump>),
	SecurityPolicy(AstDropSecurityPolicy<'bump>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropFlow<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub flow: MaybeQualifiedFlowIdentifier<'bump>,
	pub cascade: bool, // CASCADE or RESTRICT (false = RESTRICT)
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropTable<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropView<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub view: MaybeQualifiedViewIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropRingBuffer<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub ringbuffer: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropSeries<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub series: MaybeQualifiedSeriesIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropNamespace<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub namespace: MaybeQualifiedNamespaceIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropDictionary<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub dictionary: MaybeQualifiedDictionaryIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropSumType<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub sumtype: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropSubscription<'bump> {
	pub token: Token<'bump>,
	pub if_exists: bool,
	pub identifier: BumpFragment<'bump>,
	pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterSequence<'bump> {
	pub token: Token<'bump>,
	pub sequence: MaybeQualifiedSequenceIdentifier<'bump>,
	pub column: BumpFragment<'bump>,
	pub value: AstLiteral<'bump>,
}

#[derive(Debug)]
pub struct AstSubQuery<'bump> {
	pub token: Token<'bump>,
	pub statement: AstStatement<'bump>,
}

#[derive(Debug)]
pub struct AstAlterFlow<'bump> {
	pub token: Token<'bump>,
	pub flow: MaybeQualifiedFlowIdentifier<'bump>,
	pub action: AstAlterFlowAction<'bump>,
}

#[derive(Debug)]
pub enum AstAlterFlowAction<'bump> {
	Rename {
		new_name: BumpFragment<'bump>,
	},
	SetQuery {
		query: AstStatement<'bump>,
	},
	Pause,
	Resume,
}

#[derive(Debug)]
pub struct AstCreateDeferredView<'bump> {
	pub token: Token<'bump>,
	pub view: MaybeQualifiedDeferredViewIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub as_clause: Option<AstStatement<'bump>>,
}

#[derive(Debug)]
pub struct AstCreateTransactionalView<'bump> {
	pub token: Token<'bump>,
	pub view: MaybeQualifiedTransactionalViewIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub as_clause: Option<AstStatement<'bump>>,
}

#[derive(Debug)]
pub struct AstCreateFlow<'bump> {
	pub token: Token<'bump>,
	pub or_replace: bool,
	pub if_not_exists: bool,
	pub flow: MaybeQualifiedFlowIdentifier<'bump>,
	pub as_clause: AstStatement<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateNamespace<'bump> {
	pub token: Token<'bump>,
	pub namespace: MaybeQualifiedNamespaceIdentifier<'bump>,
	pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct AstCreateSeries<'bump> {
	pub token: Token<'bump>,
	pub series: MaybeQualifiedSeriesIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub tag: Option<MaybeQualifiedSumTypeIdentifier<'bump>>,
	pub precision: Option<AstTimestampPrecision>,
}

#[derive(Debug, Clone, Copy)]
pub enum AstTimestampPrecision {
	Millisecond,
	Microsecond,
	Nanosecond,
}

#[derive(Debug)]
pub struct AstCreateSubscription<'bump> {
	pub token: Token<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub as_clause: Option<AstStatement<'bump>>,
}

#[derive(Debug)]
pub struct AstCreateTable<'bump> {
	pub token: Token<'bump>,
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
}

#[derive(Debug)]
pub struct AstCreateProcedure<'bump> {
	pub token: Token<'bump>,
	pub name: MaybeQualifiedProcedureIdentifier<'bump>,
	pub params: Vec<AstProcedureParam<'bump>>,
	pub body: Vec<Ast<'bump>>,
	pub body_source: String,
}

#[derive(Debug)]
pub struct AstProcedureParam<'bump> {
	pub name: BumpFragment<'bump>,
	pub param_type: AstType<'bump>,
}

#[derive(Debug)]
pub struct AstCreateRingBuffer<'bump> {
	pub token: Token<'bump>,
	pub ringbuffer: crate::ast::identifier::MaybeQualifiedRingBufferIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub capacity: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateDictionary<'bump> {
	pub token: Token<'bump>,
	pub if_not_exists: bool,
	pub dictionary: MaybeQualifiedDictionaryIdentifier<'bump>,
	pub value_type: AstType<'bump>,
	pub id_type: AstType<'bump>,
}

#[derive(Debug)]
pub struct AstCreateSumType<'bump> {
	pub token: Token<'bump>,
	pub if_not_exists: bool,
	pub name: crate::ast::identifier::MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variants: Vec<AstVariantDef<'bump>>,
}

#[derive(Debug)]
pub struct AstVariantDef<'bump> {
	pub name: BumpFragment<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
}

#[derive(Debug)]
pub struct AstAssert<'bump> {
	pub token: Token<'bump>,
	pub node: BumpBox<'bump, Ast<'bump>>,
	pub message: Option<Token<'bump>>,
}

#[derive(Debug)]
pub enum AstDescribe<'bump> {
	Query {
		token: Token<'bump>,
		node: BumpBox<'bump, Ast<'bump>>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstType<'bump> {
	Unconstrained(BumpFragment<'bump>),
	Constrained {
		name: BumpFragment<'bump>,
		params: Vec<AstLiteral<'bump>>,
	},
	Optional(Box<AstType<'bump>>),
	Qualified {
		namespace: BumpFragment<'bump>,
		name: BumpFragment<'bump>,
	},
}

impl<'bump> AstType<'bump> {
	pub fn name_fragment(&self) -> &BumpFragment<'bump> {
		match self {
			AstType::Unconstrained(fragment) => fragment,
			AstType::Constrained {
				name,
				..
			} => name,
			AstType::Optional(inner) => inner.name_fragment(),
			AstType::Qualified {
				name,
				..
			} => name,
		}
	}

	pub fn namespace_fragment(&self) -> Option<&BumpFragment<'bump>> {
		match self {
			AstType::Qualified {
				namespace,
				..
			} => Some(namespace),
			_ => None,
		}
	}
}

#[derive(Debug)]
pub struct AstColumnToCreate<'bump> {
	pub name: BumpFragment<'bump>,
	pub ty: AstType<'bump>,
	pub properties: Vec<AstColumnProperty<'bump>>,
}

#[derive(Debug)]
pub enum AstColumnProperty<'bump> {
	AutoIncrement,
	Dictionary(MaybeQualifiedDictionaryIdentifier<'bump>),
	Saturation(BumpBox<'bump, Ast<'bump>>),
	Default(BumpBox<'bump, Ast<'bump>>),
}

#[derive(Debug)]
pub struct AstCreateIndex<'bump> {
	pub token: Token<'bump>,
	pub index_type: IndexType,
	pub index: MaybeQualifiedIndexIdentifier<'bump>,
	pub columns: Vec<AstIndexColumn<'bump>>,
	pub filters: Vec<BumpBox<'bump, Ast<'bump>>>,
	pub map: Option<BumpBox<'bump, Ast<'bump>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIndexColumn<'bump> {
	pub column: MaybeQualifiedColumnIdentifier<'bump>,
	pub order: Option<SortDirection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstPrimaryKeyDef<'bump> {
	pub columns: Vec<AstIndexColumn<'bump>>,
}

impl<'bump> AstCreate<'bump> {
	pub fn token(&self) -> &Token<'bump> {
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
			AstCreate::Enum(AstCreateSumType {
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
			AstCreate::PrimaryKey(AstCreatePrimaryKey {
				token,
				..
			}) => token,
			AstCreate::ColumnProperty(AstCreateColumnProperty {
				token,
				..
			}) => token,
			AstCreate::Procedure(AstCreateProcedure {
				token,
				..
			}) => token,
			AstCreate::Event(AstCreateEvent {
				token,
				..
			}) => token,
			AstCreate::Tag(AstCreateTag {
				token,
				..
			}) => token,
			AstCreate::Handler(AstCreateHandler {
				token,
				..
			}) => token,
			AstCreate::User(AstCreateUser {
				token,
				..
			}) => token,
			AstCreate::Role(AstCreateRole {
				token,
				..
			}) => token,
			AstCreate::Authentication(AstCreateAuthentication {
				token,
				..
			}) => token,
			AstCreate::SecurityPolicy(AstCreateSecurityPolicy {
				token,
				..
			}) => token,
			AstCreate::Migration(AstCreateMigration {
				token,
				..
			}) => token,
		}
	}
}

impl<'bump> AstAlter<'bump> {
	pub fn token(&self) -> &Token<'bump> {
		match self {
			AstAlter::Sequence(AstAlterSequence {
				token,
				..
			}) => token,
			AstAlter::Flow(AstAlterFlow {
				token,
				..
			}) => token,
			AstAlter::SecurityPolicy(AstAlterSecurityPolicy {
				token,
				..
			}) => token,
			AstAlter::Table(AstAlterTable {
				token,
				..
			}) => token,
		}
	}
}

impl<'bump> AstDrop<'bump> {
	pub fn token(&self) -> &Token<'bump> {
		match self {
			AstDrop::Flow(AstDropFlow {
				token,
				..
			}) => token,
			AstDrop::Table(AstDropTable {
				token,
				..
			}) => token,
			AstDrop::View(AstDropView {
				token,
				..
			}) => token,
			AstDrop::RingBuffer(AstDropRingBuffer {
				token,
				..
			}) => token,
			AstDrop::Namespace(AstDropNamespace {
				token,
				..
			}) => token,
			AstDrop::Dictionary(AstDropDictionary {
				token,
				..
			}) => token,
			AstDrop::Enum(AstDropSumType {
				token,
				..
			}) => token,
			AstDrop::Subscription(AstDropSubscription {
				token,
				..
			}) => token,
			AstDrop::Series(AstDropSeries {
				token,
				..
			}) => token,
			AstDrop::User(AstDropUser {
				token,
				..
			}) => token,
			AstDrop::Role(AstDropRole {
				token,
				..
			}) => token,
			AstDrop::Authentication(AstDropAuthentication {
				token,
				..
			}) => token,
			AstDrop::SecurityPolicy(AstDropSecurityPolicy {
				token,
				..
			}) => token,
		}
	}
}

#[derive(Debug)]
pub struct AstFilter<'bump> {
	pub token: Token<'bump>,
	pub node: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug)]
pub enum AstFrom<'bump> {
	Source {
		token: Token<'bump>,
		source: UnresolvedPrimitiveIdentifier<'bump>,
		index_name: Option<BumpFragment<'bump>>,
	},
	Variable {
		token: Token<'bump>,
		variable: AstVariable<'bump>,
	},
	Environment {
		token: Token<'bump>,
	},
	Inline {
		token: Token<'bump>,
		list: AstList<'bump>,
	},
	Generator(AstGenerator<'bump>),
}

#[derive(Debug)]
pub struct AstAggregate<'bump> {
	pub token: Token<'bump>,
	pub by: Vec<Ast<'bump>>,
	pub map: Vec<Ast<'bump>>,
}

impl<'bump> AstFrom<'bump> {
	pub fn token(&self) -> &Token<'bump> {
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
pub struct AstTake<'bump> {
	pub token: Token<'bump>,
	pub take: usize,
}

#[derive(Debug)]
pub struct AstList<'bump> {
	pub token: Token<'bump>,
	pub nodes: Vec<Ast<'bump>>,
}

impl<'bump> AstList<'bump> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

impl<'bump> Index<usize> for AstList<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstLiteral<'bump> {
	Boolean(AstLiteralBoolean<'bump>),
	Number(AstLiteralNumber<'bump>),
	Text(AstLiteralText<'bump>),
	Temporal(AstLiteralTemporal<'bump>),
	None(AstLiteralNone<'bump>),
}

impl<'bump> AstLiteral<'bump> {
	pub fn fragment(self) -> BumpFragment<'bump> {
		match self {
			AstLiteral::Boolean(literal) => literal.0.fragment,
			AstLiteral::Number(literal) => literal.0.fragment,
			AstLiteral::Text(literal) => literal.0.fragment,
			AstLiteral::Temporal(literal) => literal.0.fragment,
			AstLiteral::None(literal) => literal.0.fragment,
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum InfixOperator<'bump> {
	Add(Token<'bump>),
	As(Token<'bump>),
	AccessNamespace(Token<'bump>),
	AccessTable(Token<'bump>),
	Assign(Token<'bump>),
	Call(Token<'bump>),
	Subtract(Token<'bump>),
	Multiply(Token<'bump>),
	Divide(Token<'bump>),
	Rem(Token<'bump>),
	Equal(Token<'bump>),
	NotEqual(Token<'bump>),
	LessThan(Token<'bump>),
	LessThanEqual(Token<'bump>),
	GreaterThan(Token<'bump>),
	GreaterThanEqual(Token<'bump>),
	TypeAscription(Token<'bump>),
	And(Token<'bump>),
	Or(Token<'bump>),
	Xor(Token<'bump>),
	In(Token<'bump>),
	NotIn(Token<'bump>),
}

#[derive(Debug)]
pub struct AstInfix<'bump> {
	pub token: Token<'bump>,
	pub left: BumpBox<'bump, Ast<'bump>>,
	pub operator: InfixOperator<'bump>,
	pub right: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug)]
pub enum LetValue<'bump> {
	Expression(BumpBox<'bump, Ast<'bump>>), // scalar/column expression
	Statement(AstStatement<'bump>),         // FROM … | …
}

#[derive(Debug)]
pub struct AstLet<'bump> {
	pub token: Token<'bump>,
	pub name: UnqualifiedIdentifier<'bump>,
	pub value: LetValue<'bump>,
}

#[derive(Debug)]
pub struct AstDelete<'bump> {
	pub token: Token<'bump>,
	pub target: UnresolvedPrimitiveIdentifier<'bump>,
	pub filter: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug)]
pub struct AstInsert<'bump> {
	pub token: Token<'bump>,
	pub target: UnresolvedPrimitiveIdentifier<'bump>,
	pub source: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug)]
pub struct AstUpdate<'bump> {
	pub token: Token<'bump>,
	pub target: UnresolvedPrimitiveIdentifier<'bump>,
	pub assignments: Vec<Ast<'bump>>,
	pub filter: BumpBox<'bump, Ast<'bump>>,
}

/// Connector between join condition pairs
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum JoinConnector {
	#[default]
	And,
	Or,
}

/// A pair of expressions in a join using clause: (expr1, expr2)
#[derive(Debug)]
pub struct AstJoinExpressionPair<'bump> {
	pub first: BumpBox<'bump, Ast<'bump>>,
	pub second: BumpBox<'bump, Ast<'bump>>,
	pub connector: Option<JoinConnector>, // None for last pair
}

/// The using clause: using (a, b) and|or (c, d)
#[derive(Debug)]
pub struct AstUsingClause<'bump> {
	pub token: Token<'bump>,
	pub pairs: Vec<AstJoinExpressionPair<'bump>>,
}

#[derive(Debug)]
pub enum AstJoin<'bump> {
	InnerJoin {
		token: Token<'bump>,
		with: AstSubQuery<'bump>,
		using_clause: AstUsingClause<'bump>,
		alias: BumpFragment<'bump>,
	},
	LeftJoin {
		token: Token<'bump>,
		with: AstSubQuery<'bump>,
		using_clause: AstUsingClause<'bump>,
		alias: BumpFragment<'bump>,
	},
	NaturalJoin {
		token: Token<'bump>,
		with: AstSubQuery<'bump>,
		join_type: Option<JoinType>,
		alias: BumpFragment<'bump>, // Required alias (no 'as' keyword)
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNumber<'bump>(pub Token<'bump>);

impl<'bump> AstLiteralNumber<'bump> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralTemporal<'bump>(pub Token<'bump>);

impl<'bump> AstLiteralTemporal<'bump> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralText<'bump>(pub Token<'bump>);

impl<'bump> AstLiteralText<'bump> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralBoolean<'bump>(pub Token<'bump>);

impl<'bump> AstLiteralBoolean<'bump> {
	pub fn value(&self) -> bool {
		self.0.kind == TokenKind::Literal(Literal::True)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstLiteralNone<'bump>(pub Token<'bump>);

impl<'bump> AstLiteralNone<'bump> {
	pub fn value(&self) -> &str {
		self.0.fragment.text()
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDistinct<'bump> {
	pub token: Token<'bump>,
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'bump>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSort<'bump> {
	pub token: Token<'bump>,
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'bump>>,
	pub directions: Vec<Option<BumpFragment<'bump>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstColumnPropertyKind {
	Saturation,
	Default,
}

#[derive(Debug)]
pub struct AstColumnPropertyEntry<'bump> {
	pub kind: AstColumnPropertyKind,
	pub value: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug)]
pub struct AstCreatePrimaryKey<'bump> {
	pub token: Token<'bump>,
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub columns: Vec<AstIndexColumn<'bump>>,
}

#[derive(Debug)]
pub struct AstCreateColumnProperty<'bump> {
	pub token: Token<'bump>,
	pub column: MaybeQualifiedColumnIdentifier<'bump>,
	pub properties: Vec<AstColumnPropertyEntry<'bump>>,
}

#[derive(Debug)]
pub struct AstPrefix<'bump> {
	pub operator: AstPrefixOperator<'bump>,
	pub node: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AstPrefixOperator<'bump> {
	Plus(Token<'bump>),
	Negate(Token<'bump>),
	Not(Token<'bump>),
}

impl<'bump> AstPrefixOperator<'bump> {
	pub fn token(&self) -> &Token<'bump> {
		match self {
			AstPrefixOperator::Plus(token) => token,
			AstPrefixOperator::Negate(token) => token,
			AstPrefixOperator::Not(token) => token,
		}
	}
}

#[derive(Debug)]
pub struct AstMap<'bump> {
	pub token: Token<'bump>,
	pub nodes: Vec<Ast<'bump>>,
}

impl<'bump> Index<usize> for AstMap<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl<'bump> AstMap<'bump> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug)]
pub struct AstGenerator<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
	pub nodes: Vec<Ast<'bump>>,
}

impl<'bump> Index<usize> for AstGenerator<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl<'bump> AstGenerator<'bump> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug)]
pub struct AstExtend<'bump> {
	pub token: Token<'bump>,
	pub nodes: Vec<Ast<'bump>>,
}

impl<'bump> Index<usize> for AstExtend<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

impl<'bump> AstExtend<'bump> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

#[derive(Debug)]
pub struct AstPatch<'bump> {
	pub token: Token<'bump>,
	pub assignments: Vec<Ast<'bump>>,
}

impl<'bump> AstPatch<'bump> {
	pub fn len(&self) -> usize {
		self.assignments.len()
	}
}

impl<'bump> Index<usize> for AstPatch<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.assignments[index]
	}
}

#[derive(Debug)]
pub struct AstTuple<'bump> {
	pub token: Token<'bump>,
	pub nodes: Vec<Ast<'bump>>,
}

impl<'bump> AstTuple<'bump> {
	pub fn len(&self) -> usize {
		self.nodes.len()
	}
}

impl<'bump> Index<usize> for AstTuple<'bump> {
	type Output = Ast<'bump>;

	fn index(&self, index: usize) -> &Self::Output {
		&self.nodes[index]
	}
}

#[derive(Debug)]
pub struct AstBetween<'bump> {
	pub token: Token<'bump>,
	pub value: BumpBox<'bump, Ast<'bump>>,
	pub lower: BumpBox<'bump, Ast<'bump>>,
	pub upper: BumpBox<'bump, Ast<'bump>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstWildcard<'bump>(pub Token<'bump>);

#[derive(Debug, Clone, PartialEq)]
pub struct AstVariable<'bump> {
	pub token: Token<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstRownum<'bump> {
	pub token: Token<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstEnvironment<'bump> {
	pub token: Token<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstIdentity<'bump> {
	pub token: Token<'bump>,
}

#[derive(Debug)]
pub struct AstRequire<'bump> {
	pub token: Token<'bump>,
	pub body: BumpBox<'bump, Ast<'bump>>,
}

// === User/Role/Grant AST nodes ===

#[derive(Debug)]
pub struct AstCreateUser<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct AstCreateRole<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct AstGrant<'bump> {
	pub token: Token<'bump>,
	pub role: BumpFragment<'bump>,
	pub user: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct AstRevoke<'bump> {
	pub token: Token<'bump>,
	pub role: BumpFragment<'bump>,
	pub user: BumpFragment<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropUser<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
	pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropRole<'bump> {
	pub token: Token<'bump>,
	pub name: BumpFragment<'bump>,
	pub if_exists: bool,
}

// === Authentication AST nodes ===

#[derive(Debug)]
pub struct AstAuthenticationEntry<'bump> {
	pub key: BumpFragment<'bump>,
	pub value: Ast<'bump>,
}

#[derive(Debug)]
pub struct AstCreateAuthentication<'bump> {
	pub token: Token<'bump>,
	pub user: BumpFragment<'bump>,
	pub entries: Vec<AstAuthenticationEntry<'bump>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropAuthentication<'bump> {
	pub token: Token<'bump>,
	pub user: BumpFragment<'bump>,
	pub if_exists: bool,
	pub method: BumpFragment<'bump>,
}

// === Security Policy AST nodes ===

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AstPolicyTargetType {
	Table,
	Column,
	Namespace,
	Procedure,
	Function,
	Flow,
	Subscription,
	Series,
	Dictionary,
	Session,
	Feature,
}

#[derive(Debug)]
pub enum AstPolicyScope<'bump> {
	/// e.g. ON ns::object
	Specific(Vec<BumpFragment<'bump>>),
	/// e.g. ON ns (namespace-wide)
	NamespaceWide(BumpFragment<'bump>),
	/// SESSION POLICY (no ON clause)
	Global,
}

#[derive(Debug)]
pub struct AstPolicyOperationEntry<'bump> {
	pub operation: BumpFragment<'bump>,
	pub body: Vec<Ast<'bump>>,
	pub body_source: String,
}

#[derive(Debug)]
pub struct AstCreateSecurityPolicy<'bump> {
	pub token: Token<'bump>,
	pub name: Option<BumpFragment<'bump>>,
	pub target_type: AstPolicyTargetType,
	pub scope: AstPolicyScope<'bump>,
	pub operations: Vec<AstPolicyOperationEntry<'bump>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AstAlterPolicyAction {
	Enable,
	Disable,
}

#[derive(Debug)]
pub struct AstAlterSecurityPolicy<'bump> {
	pub token: Token<'bump>,
	pub target_type: AstPolicyTargetType,
	pub name: BumpFragment<'bump>,
	pub action: AstAlterPolicyAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropSecurityPolicy<'bump> {
	pub token: Token<'bump>,
	pub target_type: AstPolicyTargetType,
	pub name: BumpFragment<'bump>,
	pub if_exists: bool,
}

impl<'bump> AstVariable<'bump> {
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

#[derive(Debug)]
pub struct AstBlock<'bump> {
	pub token: Token<'bump>,
	pub statements: Vec<AstStatement<'bump>>,
}

#[derive(Debug)]
pub struct AstLoop<'bump> {
	pub token: Token<'bump>,
	pub body: AstBlock<'bump>,
}

#[derive(Debug)]
pub struct AstWhile<'bump> {
	pub token: Token<'bump>,
	pub condition: BumpBox<'bump, Ast<'bump>>,
	pub body: AstBlock<'bump>,
}

#[derive(Debug)]
pub struct AstFor<'bump> {
	pub token: Token<'bump>,
	pub variable: AstVariable<'bump>,
	pub iterable: BumpBox<'bump, Ast<'bump>>,
	pub body: AstBlock<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstBreak<'bump> {
	pub token: Token<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstContinue<'bump> {
	pub token: Token<'bump>,
}

#[derive(Debug)]
pub struct AstIf<'bump> {
	pub token: Token<'bump>,
	pub condition: BumpBox<'bump, Ast<'bump>>,
	pub then_block: AstBlock<'bump>,
	pub else_ifs: Vec<AstElseIf<'bump>>,
	pub else_block: Option<AstBlock<'bump>>,
}

#[derive(Debug)]
pub struct AstElseIf<'bump> {
	pub token: Token<'bump>,
	pub condition: BumpBox<'bump, Ast<'bump>>,
	pub then_block: AstBlock<'bump>,
}

#[derive(Debug)]
pub struct AstWindow<'bump> {
	pub token: Token<'bump>,
	pub config: Vec<AstWindowConfig<'bump>>,
	pub aggregations: Vec<Ast<'bump>>,
	pub group_by: Vec<Ast<'bump>>,
}

#[derive(Debug)]
pub struct AstWindowConfig<'bump> {
	pub key: UnqualifiedIdentifier<'bump>,
	pub value: Ast<'bump>,
}

#[derive(Debug)]
pub struct AstStatementExpression<'bump> {
	pub expression: BumpBox<'bump, Ast<'bump>>,
}

/// Function parameter (always has $ prefix)
#[derive(Debug, Clone, PartialEq)]
pub struct AstFunctionParameter<'bump> {
	pub token: Token<'bump>,
	pub variable: AstVariable<'bump>,
	pub type_annotation: Option<AstType<'bump>>,
}

/// Function definition
#[derive(Debug)]
pub struct AstDefFunction<'bump> {
	pub token: Token<'bump>,
	pub name: UnqualifiedIdentifier<'bump>,
	pub parameters: Vec<AstFunctionParameter<'bump>>,
	pub return_type: Option<AstType<'bump>>,
	pub body: AstBlock<'bump>,
}

/// Return statement
#[derive(Debug)]
pub struct AstReturn<'bump> {
	pub token: Token<'bump>,
	pub value: Option<BumpBox<'bump, Ast<'bump>>>,
}

/// APPEND statement
#[derive(Debug)]
pub enum AstAppend<'bump> {
	/// Imperative form: `APPEND $target FROM <source>`
	IntoVariable {
		token: Token<'bump>,
		target: AstVariable<'bump>,
		source: AstAppendSource<'bump>,
	},
	/// Query form: `APPEND { subquery }`
	Query {
		token: Token<'bump>,
		with: AstSubQuery<'bump>,
	},
}

impl<'bump> AstAppend<'bump> {
	pub fn token(&self) -> &Token<'bump> {
		match self {
			AstAppend::IntoVariable {
				token,
				..
			} => token,
			AstAppend::Query {
				token,
				..
			} => token,
		}
	}
}

/// Source for an APPEND statement
#[derive(Debug)]
pub enum AstAppendSource<'bump> {
	/// APPEND $x FROM table | FILTER ...
	Statement(AstStatement<'bump>),
	/// APPEND $x FROM [{...}]
	Inline(AstList<'bump>),
}

#[derive(Debug)]
pub struct AstMatchArmDestructure<'bump> {
	pub fields: Vec<BumpFragment<'bump>>,
}

#[derive(Debug)]
pub enum AstMatchArm<'bump> {
	/// Value arm: `value_expr [IF guard] => result_expr`
	Value {
		pattern: BumpBox<'bump, Ast<'bump>>,
		guard: Option<BumpBox<'bump, Ast<'bump>>>,
		result: BumpBox<'bump, Ast<'bump>>,
	},
	/// IS variant arm: `IS [ns.]Type::Variant [{ fields }] [IF guard] => result`
	IsVariant {
		namespace: Option<BumpFragment<'bump>>,
		sumtype_name: BumpFragment<'bump>,
		variant_name: BumpFragment<'bump>,
		destructure: Option<AstMatchArmDestructure<'bump>>,
		guard: Option<BumpBox<'bump, Ast<'bump>>>,
		result: BumpBox<'bump, Ast<'bump>>,
	},
	/// Simplified variant arm (no IS keyword, no type path):
	///   VariantName [{ field1, field2, ... }] [IF guard] => result
	Variant {
		variant_name: BumpFragment<'bump>,
		destructure: Option<AstMatchArmDestructure<'bump>>,
		guard: Option<BumpBox<'bump, Ast<'bump>>>,
		result: BumpBox<'bump, Ast<'bump>>,
	},
	/// Searched condition arm: `condition [IF guard] => result`
	Condition {
		condition: BumpBox<'bump, Ast<'bump>>,
		guard: Option<BumpBox<'bump, Ast<'bump>>>,
		result: BumpBox<'bump, Ast<'bump>>,
	},
	/// ELSE arm: `ELSE => result`
	Else {
		result: BumpBox<'bump, Ast<'bump>>,
	},
}

#[derive(Debug)]
pub struct AstMatch<'bump> {
	pub token: Token<'bump>,
	pub subject: Option<BumpBox<'bump, Ast<'bump>>>,
	pub arms: Vec<AstMatchArm<'bump>>,
}

/// Closure expression: `($params) { body }`
#[derive(Debug)]
pub struct AstClosure<'bump> {
	pub token: Token<'bump>,
	pub parameters: Vec<AstFunctionParameter<'bump>>,
	pub body: AstBlock<'bump>,
}

/// CREATE EVENT — declares a typed event type (a sum type with is_event: true)
#[derive(Debug)]
pub struct AstCreateEvent<'bump> {
	pub token: Token<'bump>,
	pub name: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variants: Vec<AstVariantDef<'bump>>,
}

/// CREATE TAG — declares a tag type (a sum type with SumTypeKind::Tag)
#[derive(Debug)]
pub struct AstCreateTag<'bump> {
	pub token: Token<'bump>,
	pub name: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variants: Vec<AstVariantDef<'bump>>,
}

/// CREATE HANDLER — registers a computation handler for a specific event variant
#[derive(Debug)]
pub struct AstCreateHandler<'bump> {
	pub token: Token<'bump>,
	pub name: MaybeQualifiedTableIdentifier<'bump>,
	pub on_event: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub on_variant: BumpFragment<'bump>,
	pub body: Vec<Ast<'bump>>,
	pub body_source: String,
}

/// CREATE MIGRATION — stores a named migration script in the database
#[derive(Debug)]
pub struct AstCreateMigration<'bump> {
	pub token: Token<'bump>,
	pub name: String,
	pub body_source: String,
	pub rollback_body_source: Option<String>,
}

/// MIGRATE — applies pending migrations
#[derive(Debug)]
pub struct AstMigrate<'bump> {
	pub token: Token<'bump>,
	pub target: Option<String>,
}

/// ROLLBACK MIGRATION — rolls back applied migrations
#[derive(Debug)]
pub struct AstRollbackMigration<'bump> {
	pub token: Token<'bump>,
	pub target: Option<String>,
}

/// DISPATCH — fires all handlers registered for the specified event variant
#[derive(Debug)]
pub struct AstDispatch<'bump> {
	pub token: Token<'bump>,
	pub on_event: MaybeQualifiedSumTypeIdentifier<'bump>,
	pub variant: BumpFragment<'bump>,
	pub fields: Vec<(BumpFragment<'bump>, BumpBox<'bump, Ast<'bump>>)>,
}
