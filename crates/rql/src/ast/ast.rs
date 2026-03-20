// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Index;

use reifydb_core::{
	common::{IndexType, JoinType},
	sort::SortDirection,
};

use crate::{
	ast::identifier::{
		MaybeQualifiedColumnIdentifier, MaybeQualifiedDeferredViewIdentifier,
		MaybeQualifiedDictionaryIdentifier, MaybeQualifiedFunctionIdentifier, MaybeQualifiedIndexIdentifier,
		MaybeQualifiedNamespaceIdentifier, MaybeQualifiedProcedureIdentifier,
		MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSequenceIdentifier, MaybeQualifiedSeriesIdentifier,
		MaybeQualifiedSumTypeIdentifier, MaybeQualifiedTableIdentifier, MaybeQualifiedTestIdentifier,
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
	Gate(AstGate<'bump>),
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
	RunTests(AstRunTests<'bump>),
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
			Ast::Gate(node) => &node.token,
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
			Ast::RunTests(node) => node.token(),
		}
	}

	pub fn value(&self) -> &str {
		match self {
			Ast::Identifier(ident) => ident.text(),
			_ => self.token().value(),
		}
	}
}

macro_rules! ast_accessor {
	($variant:ident, $type:ty, $fn_is:ident, $fn_as:ident, $label:expr) => {
		pub fn $fn_is(&self) -> bool {
			matches!(self, Ast::$variant(_))
		}
		pub fn $fn_as(&self) -> &$type {
			if let Ast::$variant(result) = self {
				result
			} else {
				panic!(concat!("not ", $label))
			}
		}
	};
}

macro_rules! ast_literal_accessor {
	($lit_variant:ident, $type:ty, $fn_is:ident, $fn_as:ident, $label:expr) => {
		pub fn $fn_is(&self) -> bool {
			matches!(self, Ast::Literal(AstLiteral::$lit_variant(_)))
		}
		pub fn $fn_as(&self) -> &$type {
			if let Ast::Literal(AstLiteral::$lit_variant(result)) = self {
				result
			} else {
				panic!(concat!("not literal ", $label))
			}
		}
	};
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
				| Ast::RunTests(_)
		)
	}

	/// Returns true if this AST node is a subscription DDL statement
	/// (CREATE SUBSCRIPTION or DROP SUBSCRIPTION).
	pub fn is_subscription_ddl(&self) -> bool {
		matches!(self, Ast::Create(AstCreate::Subscription(_)) | Ast::Drop(AstDrop::Subscription(_)))
	}

	ast_accessor!(Dispatch, AstDispatch<'bump>, is_dispatch, as_dispatch, "dispatch");
	ast_accessor!(Assert, AstAssert<'bump>, is_assert, as_assert, "assert");
	ast_accessor!(Aggregate, AstAggregate<'bump>, is_aggregate, as_aggregate, "aggregate");
	ast_accessor!(Between, AstBetween<'bump>, is_between, as_between, "between");
	ast_accessor!(CallFunction, AstCallFunction<'bump>, is_call_function, as_call_function, "call function");
	ast_accessor!(Cast, AstCast<'bump>, is_cast, as_cast, "cast");
	ast_accessor!(Create, AstCreate<'bump>, is_create, as_create, "create");
	ast_accessor!(Alter, AstAlter<'bump>, is_alter, as_alter, "alter");
	ast_accessor!(Describe, AstDescribe<'bump>, is_describe, as_describe, "describe");
	ast_accessor!(Filter, AstFilter<'bump>, is_filter, as_filter, "filter");
	ast_accessor!(From, AstFrom<'bump>, is_from, as_from, "from");
	ast_accessor!(Identifier, UnqualifiedIdentifier<'bump>, is_identifier, as_identifier, "identifier");
	ast_accessor!(If, AstIf<'bump>, is_if, as_if, "if");
	ast_accessor!(Infix, AstInfix<'bump>, is_infix, as_infix, "infix");
	ast_accessor!(Let, AstLet<'bump>, is_let, as_let, "let");
	ast_accessor!(Variable, AstVariable<'bump>, is_variable, as_variable, "variable");
	ast_accessor!(Delete, AstDelete<'bump>, is_delete, as_delete, "delete");
	ast_accessor!(Insert, AstInsert<'bump>, is_insert, as_insert, "insert");
	ast_accessor!(Update, AstUpdate<'bump>, is_update, as_update, "update");
	ast_accessor!(Join, AstJoin<'bump>, is_join, as_join, "join");
	ast_accessor!(Take, AstTake<'bump>, is_take, as_take, "take");
	ast_accessor!(List, AstList<'bump>, is_list, as_list, "list");
	ast_accessor!(Literal, AstLiteral<'bump>, is_literal, as_literal, "literal");
	ast_accessor!(Sort, AstSort<'bump>, is_sort, as_sort, "sort");
	ast_accessor!(Inline, AstInline<'bump>, is_inline, as_inline, "inline");
	ast_accessor!(Prefix, AstPrefix<'bump>, is_prefix, as_prefix, "prefix");
	ast_accessor!(Map, AstMap<'bump>, is_map, as_map, "map");
	ast_accessor!(Generator, AstGenerator<'bump>, is_generator, as_generator, "generator");
	ast_accessor!(Patch, AstPatch<'bump>, is_patch, as_patch, "patch");
	ast_accessor!(Tuple, AstTuple<'bump>, is_tuple, as_tuple, "tuple");
	ast_accessor!(Window, AstWindow<'bump>, is_window, as_window, "window");
	ast_accessor!(
		StatementExpression,
		AstStatementExpression<'bump>,
		is_statement_expression,
		as_statement_expression,
		"statement expression"
	);
	ast_accessor!(Rownum, AstRownum<'bump>, is_rownum, as_rownum, "rownum");
	ast_accessor!(Match, AstMatch<'bump>, is_match, as_match, "match");

	// Keep is_block/as_block as aliases for Inline (backwards compat semantics)
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

	pub fn as_environment(&self) -> &AstEnvironment<'bump> {
		if let Ast::Environment(result) = self {
			result
		} else {
			panic!("not environment")
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

	ast_literal_accessor!(Boolean, AstLiteralBoolean<'bump>, is_literal_boolean, as_literal_boolean, "boolean");
	ast_literal_accessor!(Number, AstLiteralNumber<'bump>, is_literal_number, as_literal_number, "number");
	ast_literal_accessor!(
		Temporal,
		AstLiteralTemporal<'bump>,
		is_literal_temporal,
		as_literal_temporal,
		"temporal"
	);
	ast_literal_accessor!(Text, AstLiteralText<'bump>, is_literal_text, as_literal_text, "text");
	ast_literal_accessor!(None, AstLiteralNone<'bump>, is_literal_none, as_literal_none, "none");
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
	pub rql: &'bump str,
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
	Namespace(AstCreateNamespace<'bump>),
	RemoteNamespace(AstCreateRemoteNamespace<'bump>),
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
	Policy(AstCreatePolicy<'bump>),
	Migration(AstCreateMigration<'bump>),
	Test(AstCreateTest<'bump>),
}

#[derive(Debug)]
pub enum AstAlter<'bump> {
	Sequence(AstAlterSequence<'bump>),
	Policy(AstAlterPolicy<'bump>),
	Table(AstAlterTable<'bump>),
	RemoteNamespace(AstAlterRemoteNamespace<'bump>),
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
	Policy(AstDropPolicy<'bump>),
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
pub enum AstViewStorageKind {
	Table,
	RingBuffer {
		capacity: u64,
		propagate_evictions: Option<bool>,
		partition_by: Vec<String>,
	},
	Series {
		timestamp_column: Option<String>,
		precision: Option<AstTimestampPrecision>,
	},
}

#[derive(Debug, Clone, Copy)]
pub enum AstTimestampPrecision {
	Millisecond,
	Microsecond,
	Nanosecond,
}

#[derive(Debug)]
pub struct AstCreateDeferredView<'bump> {
	pub token: Token<'bump>,
	pub view: MaybeQualifiedDeferredViewIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub as_clause: Option<AstStatement<'bump>>,
	pub storage_kind: AstViewStorageKind,
}

#[derive(Debug)]
pub struct AstCreateTransactionalView<'bump> {
	pub token: Token<'bump>,
	pub view: MaybeQualifiedTransactionalViewIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub as_clause: Option<AstStatement<'bump>>,
	pub storage_kind: AstViewStorageKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateNamespace<'bump> {
	pub token: Token<'bump>,
	pub namespace: MaybeQualifiedNamespaceIdentifier<'bump>,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstCreateRemoteNamespace<'bump> {
	pub token: Token<'bump>,
	pub namespace: MaybeQualifiedNamespaceIdentifier<'bump>,
	pub if_not_exists: bool,
	pub grpc: BumpFragment<'bump>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstAlterRemoteNamespace<'bump> {
	pub token: Token<'bump>,
	pub namespace: MaybeQualifiedNamespaceIdentifier<'bump>,
	pub grpc: BumpFragment<'bump>,
}

#[derive(Debug)]
pub struct AstCreateSeries<'bump> {
	pub token: Token<'bump>,
	pub series: MaybeQualifiedSeriesIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub tag: Option<MaybeQualifiedSumTypeIdentifier<'bump>>,
	pub precision: Option<AstTimestampPrecision>,
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
	pub if_not_exists: bool,
	pub columns: Vec<AstColumnToCreate<'bump>>,
}

#[derive(Debug)]
pub struct AstCreateProcedure<'bump> {
	pub token: Token<'bump>,
	pub name: MaybeQualifiedProcedureIdentifier<'bump>,
	pub params: Vec<AstProcedureParam<'bump>>,
	pub body: Vec<Ast<'bump>>,
	pub body_source: String,
	pub is_test: bool,
}

#[derive(Debug)]
pub struct AstCreateTest<'bump> {
	pub token: Token<'bump>,
	pub name: MaybeQualifiedTestIdentifier<'bump>,
	pub cases: Option<String>,
	pub body: Vec<Ast<'bump>>,
	pub body_source: String,
}

#[derive(Debug)]
pub enum AstRunTests<'bump> {
	All {
		token: Token<'bump>,
	},
	Namespace {
		token: Token<'bump>,
		namespace: MaybeQualifiedNamespaceIdentifier<'bump>,
	},
	Single {
		token: Token<'bump>,
		test: MaybeQualifiedTestIdentifier<'bump>,
	},
}

impl<'bump> AstRunTests<'bump> {
	pub fn token(&self) -> &Token<'bump> {
		match self {
			AstRunTests::All {
				token,
			} => token,
			AstRunTests::Namespace {
				token,
				..
			} => token,
			AstRunTests::Single {
				token,
				..
			} => token,
		}
	}
}

#[derive(Debug)]
pub struct AstProcedureParam<'bump> {
	pub name: BumpFragment<'bump>,
	pub param_type: AstType<'bump>,
}

#[derive(Debug)]
pub struct AstCreateRingBuffer<'bump> {
	pub token: Token<'bump>,
	pub ringbuffer: MaybeQualifiedRingBufferIdentifier<'bump>,
	pub columns: Vec<AstColumnToCreate<'bump>>,
	pub capacity: u64,
	pub partition_by: Vec<String>,
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
	pub name: MaybeQualifiedSumTypeIdentifier<'bump>,
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
	/// Single expression for pipeline-compatible ASSERT (e.g. `FROM x | ASSERT { cond }`)
	pub node: Option<BumpBox<'bump, Ast<'bump>>>,
	/// RQL source text of the body for multi-statement or ASSERT ERROR blocks
	pub body: Option<String>,
	/// True when `ASSERT ERROR { ... }` syntax is used
	pub expect_error: bool,
	pub message: Option<Token<'bump>>,
	pub rql: &'bump str,
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

/// Generates a `fn token()` method for an enum where every variant contains
/// a struct with a `token` field.
macro_rules! impl_token_for_enum {
	($enum_type:ident, $lt:lifetime, $( $variant:ident($inner:ty) ),+ $(,)?) => {
		impl<$lt> $enum_type<$lt> {
			pub fn token(&self) -> &Token<$lt> {
				match self {
					$( $enum_type::$variant(inner) => &inner.token, )+
				}
			}
		}
	};
}

impl_token_for_enum!(AstCreate, 'bump,
	DeferredView(AstCreateDeferredView<'bump>),
	TransactionalView(AstCreateTransactionalView<'bump>),
	Namespace(AstCreateNamespace<'bump>),
	RemoteNamespace(AstCreateRemoteNamespace<'bump>),
	Series(AstCreateSeries<'bump>),
	Table(AstCreateTable<'bump>),
	RingBuffer(AstCreateRingBuffer<'bump>),
	Dictionary(AstCreateDictionary<'bump>),
	Enum(AstCreateSumType<'bump>),
	Index(AstCreateIndex<'bump>),
	Subscription(AstCreateSubscription<'bump>),
	PrimaryKey(AstCreatePrimaryKey<'bump>),
	ColumnProperty(AstCreateColumnProperty<'bump>),
	Procedure(AstCreateProcedure<'bump>),
	Event(AstCreateEvent<'bump>),
	Tag(AstCreateTag<'bump>),
	Handler(AstCreateHandler<'bump>),
	User(AstCreateUser<'bump>),
	Role(AstCreateRole<'bump>),
	Authentication(AstCreateAuthentication<'bump>),
	Policy(AstCreatePolicy<'bump>),
	Migration(AstCreateMigration<'bump>),
	Test(AstCreateTest<'bump>),
);

impl_token_for_enum!(AstAlter, 'bump,
	Sequence(AstAlterSequence<'bump>),
	Policy(AstAlterPolicy<'bump>),
	Table(AstAlterTable<'bump>),
	RemoteNamespace(AstAlterRemoteNamespace<'bump>),
);

impl_token_for_enum!(AstDrop, 'bump,
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
	Policy(AstDropPolicy<'bump>),
);

#[derive(Debug)]
pub struct AstFilter<'bump> {
	pub token: Token<'bump>,
	pub node: BumpBox<'bump, Ast<'bump>>,
	pub rql: &'bump str,
}

#[derive(Debug)]
pub struct AstGate<'bump> {
	pub token: Token<'bump>,
	pub node: BumpBox<'bump, Ast<'bump>>,
	pub rql: &'bump str,
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
	pub rql: &'bump str,
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
pub enum AstTakeValue<'bump> {
	Literal(usize),
	Variable(Token<'bump>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstTake<'bump> {
	pub token: Token<'bump>,
	pub take: AstTakeValue<'bump>,
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
	Contains(Token<'bump>),
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
	pub returning: Option<Vec<Ast<'bump>>>,
}

#[derive(Debug)]
pub struct AstInsert<'bump> {
	pub token: Token<'bump>,
	pub target: UnresolvedPrimitiveIdentifier<'bump>,
	pub source: BumpBox<'bump, Ast<'bump>>,
	pub returning: Option<Vec<Ast<'bump>>>,
}

#[derive(Debug)]
pub struct AstUpdate<'bump> {
	pub token: Token<'bump>,
	pub target: UnresolvedPrimitiveIdentifier<'bump>,
	pub assignments: Vec<Ast<'bump>>,
	pub filter: BumpBox<'bump, Ast<'bump>>,
	pub returning: Option<Vec<Ast<'bump>>>,
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
		rql: &'bump str,
	},
	LeftJoin {
		token: Token<'bump>,
		with: AstSubQuery<'bump>,
		using_clause: AstUsingClause<'bump>,
		alias: BumpFragment<'bump>,
		rql: &'bump str,
	},
	NaturalJoin {
		token: Token<'bump>,
		with: AstSubQuery<'bump>,
		join_type: Option<JoinType>,
		alias: BumpFragment<'bump>, // Required alias (no 'as' keyword)
		rql: &'bump str,
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
	pub rql: &'bump str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstSort<'bump> {
	pub token: Token<'bump>,
	pub columns: Vec<MaybeQualifiedColumnIdentifier<'bump>>,
	pub directions: Vec<Option<BumpFragment<'bump>>>,
	pub rql: &'bump str,
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
	pub rql: &'bump str,
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
	pub rql: &'bump str,
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
	pub rql: &'bump str,
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
	Subscription,
	Series,
	Dictionary,
	Session,
	Feature,
	View,
	RingBuffer,
}

impl AstPolicyTargetType {
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Table => "table",
			Self::Column => "column",
			Self::Namespace => "namespace",
			Self::Procedure => "procedure",
			Self::Function => "function",
			Self::Subscription => "subscription",
			Self::Series => "series",
			Self::Dictionary => "dictionary",
			Self::Session => "session",
			Self::Feature => "feature",
			Self::View => "view",
			Self::RingBuffer => "ringbuffer",
		}
	}
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
pub struct AstCreatePolicy<'bump> {
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
pub struct AstAlterPolicy<'bump> {
	pub token: Token<'bump>,
	pub target_type: AstPolicyTargetType,
	pub name: BumpFragment<'bump>,
	pub action: AstAlterPolicyAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AstDropPolicy<'bump> {
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
	pub rql: &'bump str,
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
