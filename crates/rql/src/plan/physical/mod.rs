// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod alter;
pub mod create;
pub mod drop;
pub(crate) mod interner;
pub mod mutate;

use std::{collections, fmt, iter::once, marker};

use bumpalo::Bump;
use reifydb_catalog::catalog::{Catalog, table::TableColumnToCreate, view::ViewColumnToCreate};
use reifydb_core::{
	common::{JoinType, TimeSource},
	error::diagnostic::{
		catalog::{
			dictionary_not_found, namespace_not_found, queue_not_found, queue_reserved_column_collision,
			ringbuffer_not_found, series_not_found, table_not_found,
		},
		operation::lookup_right_unsupported,
	},
	expression::{
		ConstantExpression, Expression, Expression::Constant, VariableExpression, extract_variable_names,
	},
	interface::{
		catalog::{
			column::{Column, ColumnIndex},
			id::{ColumnId, NamespaceId, TableId},
			namespace::Namespace,
			ringbuffer::EvictionVisibility,
			table::Table,
			view::ViewStorageKind,
		},
		resolved::{
			ResolvedColumn, ResolvedDictionary, ResolvedNamespace, ResolvedObject, ResolvedQueue,
			ResolvedRingBuffer, ResolvedSeries, ResolvedTable, ResolvedView,
		},
	},
	operator_with::{AggregateWith, ApplyWith, ChangesWith, DistinctWith, JoinWith, LookupWith, WindowWith},
	row::Ttl,
	sort::SortKey,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql_frontend::{
	ast::{
		ast::{AstAlterPolicyAction, AstPolicyScope, AstViewStorageKind},
		identifier::{
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedTableIdentifier,
		},
	},
	bump::{BumpBox, BumpFragment, BumpVec},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	return_error,
	value::{constraint::TypeConstraint, duration::Duration, value_type::ValueType},
};
use tracing::instrument;

use crate::{
	Result, convert_data_type_with_constraints,
	error::RqlError,
	expression::ExpressionCompiler,
	nodes,
	nodes::{
		AlterSequenceNode, CreateDictionaryNode, CreateNamespaceNode, CreateQueueNode, CreateRingBufferNode,
		CreateSumTypeNode, CreateTableNode, DictionaryScanNode, EnvironmentNode, GeneratorNode, IndexScanNode,
		InlineDataNode, PartitionKey, QUEUE_DEDUPLICATION_KEY_FIELD, QUEUE_NOT_BEFORE_FIELD, QueueScanNode,
		RingBufferScanNode, RowKeyPredicate, RowListLookupNode, RowPointLookupNode, RowRangeScanNode,
		SeriesBound, SeriesScanNode, TableScanNode, TableVirtualScanNode, VariableNode, ViewScanNode,
	},
	optimize::rules::optimize_tree,
	plan::{
		logical,
		logical::{
			LogicalPlan,
			partition_predicate::{extract_partition, extract_partition_keys},
			row_predicate::{RowPredicate, extract_row_key_predicate, extract_row_predicate},
			series_predicate::{extract_series_bounds, extract_series_predicate},
			tree::{QueryTree, fold},
		},
		physical::interner::FragmentInterner,
	},
};

#[derive(Debug)]
pub enum PhysicalPlan<'bump> {
	CreateDeferredView(CreateDeferredViewNode<'bump>),
	CreateDeferredWindowedViewGroup(CreateDeferredWindowedViewGroupNode<'bump>),
	CreateTransactionalView(CreateTransactionalViewNode<'bump>),
	CreateNamespace(CreateNamespaceNode),
	CreateRemoteNamespace(nodes::CreateRemoteNamespaceNode),
	CreateTable(CreateTableNode),
	CreateQueue(CreateQueueNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSumType(CreateSumTypeNode),
	CreatePrimaryKey(nodes::CreatePrimaryKeyNode),
	CreateColumnProperty(nodes::CreateColumnPropertyNode),
	CreateProcedure(nodes::CreateProcedureNode),
	CreateEvent(nodes::CreateEventNode),

	CreateSeries(nodes::CreateSeriesNode),
	CreateSource(nodes::CreateSourceNode),
	CreateSink(nodes::CreateSinkNode),
	CreateBinding(nodes::CreateBindingNode),
	CreateRelationship(nodes::CreateRelationshipNode),
	CreateTest(nodes::CreateTestNode),
	RunTests(nodes::RunTestsNode),
	CreateMigration(nodes::CreateMigrationNode),
	Migrate(nodes::MigrateNode),
	RollbackMigration(nodes::RollbackMigrationNode),
	Dispatch(nodes::DispatchNode),

	DropNamespace(nodes::DropNamespaceNode),
	DropTable(nodes::DropTableNode),
	DropView(nodes::DropViewNode),
	DropQueue(nodes::DropQueueNode),
	DropRingBuffer(nodes::DropRingBufferNode),
	DropDictionary(nodes::DropDictionaryNode),
	DropSumType(nodes::DropSumTypeNode),
	DropSeries(nodes::DropSeriesNode),
	DropSource(nodes::DropSourceNode),
	DropSink(nodes::DropSinkNode),
	DropWindowedViewGroup(nodes::DropWindowedViewGroupNode),
	DropProcedure(nodes::DropProcedureNode),
	DropHandler(nodes::DropHandlerNode),
	DropTest(nodes::DropTestNode),
	DropBinding(nodes::DropBindingNode),
	DropRelationship(nodes::DropRelationshipNode),

	AlterSequence(AlterSequenceNode),
	AlterTable(AlterTableNode<'bump>),
	AlterRemoteNamespace(nodes::AlterRemoteNamespaceNode),
	AlterIdentity(nodes::AlterIdentityNode),

	Delete(DeleteTableNode<'bump>),
	DeleteRingBuffer(DeleteRingBufferNode<'bump>),
	DeleteSeries(DeleteSeriesNode<'bump>),
	InsertTable(InsertTableNode<'bump>),
	InsertRingBuffer(InsertRingBufferNode<'bump>),
	InsertQueue(InsertQueueNode<'bump>),
	InsertDictionary(InsertDictionaryNode<'bump>),
	InsertSeries(InsertSeriesNode<'bump>),
	Update(UpdateTableNode<'bump>),
	UpdateRingBuffer(UpdateRingBufferNode<'bump>),
	UpdateSeries(UpdateSeriesNode<'bump>),

	Declare(DeclareNode<'bump>),
	Assign(AssignNode<'bump>),
	Append(AppendPhysicalNode<'bump>),

	Variable(VariableNode),
	Environment(EnvironmentNode),

	Conditional(ConditionalNode<'bump>),
	Loop(LoopNode<'bump>),
	While(WhileNode<'bump>),
	For(ForNode<'bump>),
	Break,
	Continue,

	DefineFunction(DefineFunctionNode<'bump>),
	Return(ReturnNode<'bump>),
	CallFunction(CallFunctionNode),

	DefineClosure(DefineClosureNode<'bump>),

	Aggregate(AggregateNode<'bump>),
	Assert(AssertNode<'bump>),
	AssertBlock(AssertBlockNode),
	Distinct(DistinctNode<'bump>),
	Changes(ChangesNode<'bump>),
	Filter(FilterNode<'bump>),
	Gate(GateNode<'bump>),
	IndexScan(IndexScanNode),
	RowPointLookup(RowPointLookupNode),
	RowListLookup(RowListLookupNode),
	RowRangeScan(RowRangeScanNode),
	RowParameterLookup(RowParameterLookupNode<'bump>),
	PartitionParameterScan(PartitionParameterScanNode<'bump>),
	SeriesParameterScan(SeriesParameterScanNode<'bump>),
	JoinInner(JoinInnerNode<'bump>),
	JoinLeft(JoinLeftNode<'bump>),
	JoinNatural(JoinNaturalNode<'bump>),
	Lookup(LookupNode<'bump>),
	Take(TakeNode<'bump>),
	Sort(SortNode<'bump>),
	Map(MapNode<'bump>),
	Extend(ExtendNode<'bump>),
	Patch(PatchNode<'bump>),
	Apply(ApplyNode<'bump>),
	InlineData(InlineDataNode),
	RemoteScan(nodes::RemoteScanNode),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	DictionaryScan(DictionaryScanNode),
	SeriesScan(SeriesScanNode),
	QueueScan(QueueScanNode),
	Generator(GeneratorNode),
	Window(WindowNode<'bump>),

	CreateIdentity(nodes::CreateIdentityNode),
	CreateIdentityAttribute(nodes::CreateIdentityAttributeNode),
	CreateRole(nodes::CreateRoleNode),
	Grant(nodes::GrantNode),
	Revoke(nodes::RevokeNode),
	DropIdentity(nodes::DropIdentityNode),
	DropIdentityAttribute(nodes::DropIdentityAttributeNode),
	DropRole(nodes::DropRoleNode),
	CreateAuthentication(nodes::CreateAuthenticationNode),
	DropAuthentication(nodes::DropAuthenticationNode),
	CreatePolicy(nodes::CreatePolicyNode),
	AlterPolicy(nodes::AlterPolicyNode),
	DropPolicy(nodes::DropPolicyNode),
	CreateAuthenticationMethod(nodes::CreateAuthenticationMethodNode),
	AlterAuthenticationMethod(nodes::AlterAuthenticationMethodNode),
	DropAuthenticationMethod(nodes::DropAuthenticationMethodNode),
}

#[derive(Debug)]
pub struct CreateDeferredViewNode<'bump> {
	pub namespace: Namespace,
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub storage_kind: AstViewStorageKind,
	pub ttl: Option<Ttl>,
	pub persistent: bool,
	pub ttl_eviction: EvictionVisibility,
}

#[derive(Debug)]
pub struct CreateDeferredWindowedViewGroupNode<'bump> {
	pub namespace: Namespace,
	pub group: Fragment,
	pub columns: Vec<ViewColumnToCreate>,
	pub storage_kind: AstViewStorageKind,
	pub members: Vec<WindowedViewGroupMember<'bump>>,
}

#[derive(Debug)]
pub struct WindowedViewGroupMember<'bump> {
	pub namespace: Namespace,
	pub view: Fragment,
	pub duration: Duration,
	pub ttl: Option<Ttl>,
	pub persistent: bool,
	pub ttl_eviction: EvictionVisibility,
	pub as_clause: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'bump> {
	pub namespace: Namespace,
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub storage_kind: AstViewStorageKind,
	pub ttl: Option<Ttl>,
	pub persistent: bool,
}

#[derive(Debug)]
pub struct AlterTableNode<'bump> {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub action: AlterTableAction,
	pub _phantom: marker::PhantomData<&'bump ()>,
}

#[derive(Debug)]
pub enum AlterTableAction {
	AddColumn {
		column: TableColumnToCreate,
	},
	DropColumn {
		column: Fragment,
	},
	RenameColumn {
		old_name: Fragment,
		new_name: Fragment,
	},
	DropPartition {
		values: Vec<(String, String)>,
		remove_registry: bool,
	},
}

#[derive(Debug)]
pub struct DeleteTableNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub target: Option<ResolvedTable>,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct DeleteRingBufferNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub target: ResolvedRingBuffer,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct InsertTableNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedTable,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct InsertRingBufferNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedRingBuffer,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct InsertQueueNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedQueue,
	pub deduplication_key: Option<Expression>,
	pub not_before: Option<Expression>,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct InsertDictionaryNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedDictionary,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct InsertSeriesNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedSeries,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct DeleteSeriesNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub target: ResolvedSeries,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct UpdateTableNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: Option<ResolvedTable>,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct UpdateRingBufferNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedRingBuffer,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub struct UpdateSeriesNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedSeries,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug)]
pub enum LetValue<'bump> {
	Expression(Expression),
	Statement(BumpBox<'bump, PhysicalPlan<'bump>>),
	EmptyFrame,
}

impl fmt::Display for LetValue<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			LetValue::Expression(expr) => write!(f, "{}", expr),
			LetValue::Statement(plan) => write!(f, "Statement({:?})", plan),
			LetValue::EmptyFrame => write!(f, "EmptyFrame"),
		}
	}
}

#[derive(Debug)]
pub struct DeclareNode<'bump> {
	pub name: Fragment,
	pub value: LetValue<'bump>,
}

#[derive(Debug)]
pub enum AssignValue<'bump> {
	Expression(Expression),
	Statement(BumpBox<'bump, PhysicalPlan<'bump>>),
}

impl fmt::Display for AssignValue<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			AssignValue::Expression(expr) => write!(f, "{}", expr),
			AssignValue::Statement(plan) => write!(f, "Statement({:?})", plan),
		}
	}
}

#[derive(Debug)]
pub struct AssignNode<'bump> {
	pub name: Fragment,
	pub value: AssignValue<'bump>,
}

#[derive(Debug)]
pub enum AppendPhysicalNode<'bump> {
	IntoVariable {
		target: Fragment,
		source: AppendPhysicalSource<'bump>,
	},
	Query {
		fragment: Fragment,
		left: BumpBox<'bump, PhysicalPlan<'bump>>,
		right: BumpBox<'bump, PhysicalPlan<'bump>>,
	},
}

#[derive(Debug)]
pub enum AppendPhysicalSource<'bump> {
	Statement(Vec<PhysicalPlan<'bump>>),
	Inline(InlineDataNode),
}

#[derive(Debug)]
pub struct ConditionalNode<'bump> {
	pub condition: Expression,
	pub then_branch: Vec<PhysicalPlan<'bump>>,
	pub else_ifs: Vec<ElseIfBranch<'bump>>,
	pub else_branch: Option<Vec<PhysicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct ElseIfBranch<'bump> {
	pub condition: Expression,
	pub then_branch: Vec<PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct LoopNode<'bump> {
	pub body: Vec<PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct WhileNode<'bump> {
	pub condition: Expression,
	pub body: Vec<PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct ForNode<'bump> {
	pub variable_name: Fragment,
	pub iterable: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub body: Vec<PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct DefineFunctionNode<'bump> {
	pub name: Fragment,
	pub parameters: Vec<nodes::FunctionParameter>,
	pub return_type: Option<TypeConstraint>,
	pub body: Vec<PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub enum ReturnValue<'bump> {
	Expression(Expression),
	Statement(BumpBox<'bump, PhysicalPlan<'bump>>),
}

impl fmt::Display for ReturnValue<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ReturnValue::Expression(expr) => write!(f, "{}", expr),
			ReturnValue::Statement(plan) => write!(f, "Statement({:?})", plan),
		}
	}
}

#[derive(Debug)]
pub struct ReturnNode<'bump> {
	pub value: Option<ReturnValue<'bump>>,
}

#[derive(Debug)]
pub struct CallFunctionNode {
	pub name: Fragment,
	pub arguments: Vec<Expression>,
	pub is_procedure_call: bool,
}

#[derive(Debug)]
pub struct DefineClosureNode<'bump> {
	pub parameters: Vec<nodes::FunctionParameter>,
	pub body: Vec<PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct AggregateNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
	pub with: AggregateWith,
}

#[derive(Debug)]
pub struct ChangesNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub with: ChangesWith,
	pub fragment: Fragment,
}

#[derive(Debug)]
pub struct DistinctNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub columns: Vec<ResolvedColumn>,
	pub with: DistinctWith,
}

#[derive(Debug)]
pub struct AssertNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub conditions: Vec<Expression>,
	pub message: Option<String>,
}

#[derive(Debug)]
pub struct AssertBlockNode {
	pub rql: String,
	pub expect_error: bool,
	pub message: Option<String>,
}

#[derive(Debug)]
pub struct FilterNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug)]
pub struct SeriesParameterScanNode<'bump> {
	pub starts: Vec<SeriesBound>,
	pub ends: Vec<SeriesBound>,
	pub partition_keys: Option<Vec<PartitionKey>>,
	pub scan: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct PartitionParameterScanNode<'bump> {
	pub keys: Vec<PartitionKey>,
	pub scan: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct RowParameterLookupNode<'bump> {
	pub source: ResolvedObject,
	pub predicate: RowKeyPredicate,
	pub fallback: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct GateNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug)]
pub struct JoinInnerNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
	pub with: JoinWith,
}

#[derive(Debug)]
pub struct JoinLeftNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
	pub with: JoinWith,
}

#[derive(Debug)]
pub struct JoinNaturalNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub join_type: JoinType,
	pub fragment: Fragment,
	pub alias: Option<Fragment>,
	pub with: JoinWith,
}

#[derive(Debug)]
pub struct LookupNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Fragment,
	pub join_type: JoinType,
	pub with: LookupWith,
}

#[derive(Debug)]
pub struct TakeNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub take: nodes::TakeLimit,
}

#[derive(Debug)]
pub struct SortNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub by: Vec<SortKey>,
}

#[derive(Debug)]
pub struct MapNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub map: Vec<Expression>,
}

#[derive(Debug)]
pub struct ExtendNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub extend: Vec<Expression>,
}

#[derive(Debug)]
pub struct PatchNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub assignments: Vec<Expression>,
}

#[derive(Debug)]
pub struct ApplyNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub operator: Fragment,
	pub params: Vec<Expression>,
	pub with: ApplyWith,
}

#[derive(Debug)]
pub struct WindowNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub with: WindowWith,
	pub fragment: Fragment,
}

pub(crate) struct Compiler<'bump> {
	pub catalog: Catalog,
	pub routines: Routines,
	pub interner: FragmentInterner,
	pub bump: &'bump Bump,
}

#[instrument(name = "rql::compile::physical", level = "trace", skip(bump, catalog, routines, rx, logical))]
pub fn compile_physical<'b>(
	bump: &'b Bump,
	catalog: &Catalog,
	routines: &Routines,
	rx: &mut Transaction<'_>,
	logical: impl IntoIterator<Item = LogicalPlan<'b>>,
) -> Result<Option<PhysicalPlan<'b>>> {
	Compiler {
		catalog: catalog.clone(),
		routines: routines.clone(),
		interner: FragmentInterner::new(),
		bump,
	}
	.compile(rx, logical)
}

impl<'bump> Compiler<'bump> {
	fn bump_box(&self, plan: PhysicalPlan<'bump>) -> BumpBox<'bump, PhysicalPlan<'bump>> {
		BumpBox::new_in(plan, self.bump)
	}

	pub fn compile(
		&mut self,
		rx: &mut Transaction<'_>,
		logical: impl IntoIterator<Item = LogicalPlan<'bump>>,
	) -> Result<Option<PhysicalPlan<'bump>>> {
		match fold(self.bump, logical)? {
			Some(tree) => {
				let tree = optimize_tree(self.bump, &self.routines, tree);
				Ok(Some(self.compile_tree(rx, tree)?))
			}
			None => Ok(None),
		}
	}

	fn compile_branch(
		&mut self,
		rx: &mut Transaction<'_>,
		statements: Vec<BumpVec<'bump, LogicalPlan<'bump>>>,
	) -> Result<Vec<PhysicalPlan<'bump>>> {
		let mut branch = Vec::with_capacity(statements.len());
		for statement in statements {
			if let Some(plan) = self.compile(rx, statement)? {
				branch.push(plan);
			}
		}
		Ok(branch)
	}

	fn compile_tree(&mut self, rx: &mut Transaction<'_>, tree: QueryTree<'bump>) -> Result<PhysicalPlan<'bump>> {
		match tree {
			QueryTree::Aggregate {
				node: aggregate,
				input,
			} => self.compile_aggregate(rx, aggregate, input),

			QueryTree::Assert {
				node: assert_node,
				input,
			} => self.compile_assert(rx, assert_node, input),

			QueryTree::Filter {
				node: filter,
				input,
			} => self.compile_filter(rx, filter, input),

			QueryTree::Gate {
				node: gate,
				input,
			} => self.compile_gate(rx, gate, input),

			QueryTree::InlineData {
				node: inline,
			} => {
				return Ok(PhysicalPlan::InlineData(InlineDataNode {
					rows: inline.rows,
				}));
			}

			QueryTree::Generator {
				node: generator,
			} => self.compile_generator(generator),

			QueryTree::DeleteTable {
				target,
				returning,
				input,
			} => self.compile_delete_table(rx, target, returning, input),

			QueryTree::DeleteRingBuffer {
				target,
				returning,
				input,
			} => self.compile_delete_ring_buffer(rx, target, returning, input),

			QueryTree::DeleteSeries {
				target,
				returning,
				input,
			} => self.compile_delete_series(rx, target, returning, input),

			QueryTree::Update {
				target,
				returning,
				input,
			} => self.compile_update(rx, target, returning, input),

			QueryTree::UpdateRingBuffer {
				target,
				returning,
				input,
			} => self.compile_update_ring_buffer(rx, target, returning, input),

			QueryTree::UpdateSeries {
				target,
				returning,
				input,
			} => self.compile_update_series(rx, target, returning, input),

			QueryTree::JoinInner {
				on,
				alias,
				with,
				rql,
				input,
				right,
			} => self.compile_join_inner(rx, on, alias, with, rql, input, right),

			QueryTree::JoinLeft {
				on,
				alias,
				with,
				rql,
				input,
				right,
			} => self.compile_join_left(rx, on, alias, with, rql, input, right),

			QueryTree::JoinNatural {
				join_type,
				fragment,
				alias,
				with,
				rql,
				input,
				right,
			} => self.compile_join_natural(rx, join_type, fragment, alias, with, rql, input, right),

			QueryTree::Lookup {
				join_type,
				on,
				alias,
				with,
				input,
				right,
			} => self.compile_lookup(rx, join_type, on, alias, with, input, right),

			QueryTree::Order {
				node: order,
				input,
			} => self.compile_order(rx, order, input),

			QueryTree::Changes {
				node: changes,
				input,
			} => self.compile_changes(rx, changes, input),

			QueryTree::Distinct {
				node: distinct,
				input,
			} => self.compile_distinct(rx, distinct, input),

			QueryTree::Map {
				node: map,
				input,
			} => self.compile_map(rx, map, input),

			QueryTree::Extend {
				node: extend,
				input,
			} => self.compile_extend(rx, extend, input),

			QueryTree::Patch {
				node: patch,
				input,
			} => self.compile_patch(rx, patch, input),

			QueryTree::Apply {
				node: apply,
				input,
			} => self.compile_apply(rx, apply, input),

			QueryTree::SourceScan {
				node: scan,
			} => self.compile_source_scan(scan),

			QueryTree::RemoteScan {
				node: scan,
			} => self.compile_remote_scan(scan),

			QueryTree::Take {
				node: take,
				input,
			} => self.compile_take(rx, take, input),

			QueryTree::Window {
				node: window,
				input,
			} => self.compile_window(rx, window, input),

			QueryTree::VariableSource {
				node: source,
			} => self.compile_variable_source(source),

			QueryTree::Environment {
				..
			} => {
				return Ok(PhysicalPlan::Environment(EnvironmentNode {}));
			}

			QueryTree::AppendQuery {
				fragment,
				input,
				right,
			} => self.compile_append_query(rx, fragment, input, right),

			QueryTree::Statement(plan) => self.compile_statement(rx, plan),
		}
	}

	#[inline(never)]
	fn compile_aggregate(
		&mut self,
		rx: &mut Transaction<'_>,
		aggregate: logical::AggregateNode,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;

		let mut vars = Vec::new();
		for expr in aggregate.by.iter().chain(aggregate.map.iter()) {
			vars.extend(extract_variable_names(expr));
		}
		if let Some(pushed) = try_remote_push_down_with_vars(&input, || Some(aggregate.rql.clone()), vars) {
			return Ok(pushed);
		}

		return Ok(PhysicalPlan::Aggregate(AggregateNode {
			by: aggregate.by,
			map: aggregate.map,
			with: aggregate.with,
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_assert(
		&mut self,
		rx: &mut Transaction<'_>,
		assert_node: logical::AssertNode,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|p| self.bump_box(p));

		if let Some(ref boxed_input) = input {
			let vars = extract_variable_names(&assert_node.condition);
			if let Some(pushed) =
				try_remote_push_down_with_vars(boxed_input, || Some(assert_node.rql.clone()), vars)
			{
				return Ok(pushed);
			}
		}

		return Ok(PhysicalPlan::Assert(AssertNode {
			conditions: vec![assert_node.condition],
			message: assert_node.message,
			input,
		}));
	}

	#[inline(never)]
	fn compile_filter(
		&mut self,
		rx: &mut Transaction<'_>,
		filter: logical::FilterNode,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;

		let vars = extract_variable_names(&filter.condition);
		if let Some(pushed) = try_remote_push_down_with_vars(&input, || Some(filter.rql.clone()), vars) {
			return Ok(pushed);
		}

		let row_predicate = extract_row_predicate(&filter.condition);
		let row_key_predicate = match row_predicate {
			Some(_) => None,
			None => extract_row_key_predicate(&filter.condition),
		};
		if row_predicate.is_some() || row_key_predicate.is_some() {
			let source = match &input {
				PhysicalPlan::TableScan(scan) if scan.source.def().partition_by.is_empty() => {
					Some(ResolvedObject::Table(scan.source.clone()))
				}
				PhysicalPlan::ViewScan(scan)
					if scan.source.def().sort().is_empty()
						&& scan.source.def().partition_by().is_empty()
						&& scan.source.def().storage_kind() != ViewStorageKind::Series =>
				{
					Some(ResolvedObject::View(scan.source.clone()))
				}
				PhysicalPlan::RingBufferScan(scan) if scan.source.def().partition_by.is_empty() => {
					Some(ResolvedObject::RingBuffer(scan.source.clone()))
				}
				_ => None,
			};

			if let Some(source) = source {
				if let Some(predicate) = row_key_predicate {
					let fallback = PhysicalPlan::Filter(FilterNode {
						conditions: vec![filter.condition],
						input: self.bump_box(input),
					});
					return Ok(PhysicalPlan::RowParameterLookup(RowParameterLookupNode {
						source,
						predicate,
						fallback: self.bump_box(fallback),
					}));
				}

				if let Some(predicate) = row_predicate {
					match predicate {
						RowPredicate::Point(row_number) => {
							return Ok(PhysicalPlan::RowPointLookup(RowPointLookupNode {
								source,
								row_number,
							}));
						}
						RowPredicate::List(row_numbers) => {
							return Ok(PhysicalPlan::RowListLookup(RowListLookupNode {
								source,
								row_numbers,
							}));
						}
						RowPredicate::Range {
							start,
							end,
						} => {
							return Ok(PhysicalPlan::RowRangeScan(RowRangeScanNode {
								source,
								start,
								end,
							}));
						}
					}
				}
			}
		}

		if let PhysicalPlan::SeriesScan(ref scan) = input {
			let def = scan.source.def();
			let partitioned = !def.partition_by.is_empty();
			let partition = if partitioned {
				extract_partition(&filter.condition, &def.columns, &def.partition_by)
			} else {
				None
			};

			if partitioned
				&& partition.is_none() && let Some(partition_keys) =
				extract_partition_keys(&filter.condition, &def.columns, &def.partition_by)
			{
				let (starts, ends) = if def.tag.is_none() {
					extract_series_bounds(&filter.condition, def.key.column())
				} else {
					(Vec::new(), Vec::new())
				};
				let deferred = PhysicalPlan::SeriesParameterScan(SeriesParameterScanNode {
					starts,
					ends,
					partition_keys: Some(partition_keys),
					scan: self.bump_box(input),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(deferred),
				}));
			}

			let pushable = if partitioned && partition.is_none() {
				None
			} else {
				extract_series_predicate(&filter.condition, def.key.column()).filter(|sp| {
					def.tag.is_none()
						|| sp.variant_tag.is_some() || (sp.key_start.is_none() && sp.key_end.is_none())
				})
			};

			let ranged = !(partitioned && partition.is_none())
				&& (def.tag.is_none() || pushable.as_ref().is_some_and(|sp| sp.variant_tag.is_some()));
			let (starts, ends) = if ranged {
				let (starts, ends) = extract_series_bounds(&filter.condition, def.key.column());
				let variable = |bound: &SeriesBound| matches!(bound, SeriesBound::Variable(_));
				(
					starts.into_iter().filter(variable).collect::<Vec<_>>(),
					ends.into_iter().filter(variable).collect::<Vec<_>>(),
				)
			} else {
				(Vec::new(), Vec::new())
			};
			let deferred = !starts.is_empty() || !ends.is_empty();

			if pushable.is_some() || partition.is_some() || deferred {
				let rewritten = if pushable.is_some() || partition.is_some() {
					PhysicalPlan::SeriesScan(SeriesScanNode {
						source: scan.source.clone(),
						key_range_start: pushable
							.as_ref()
							.and_then(|sp| sp.key_start)
							.or(scan.key_range_start),
						key_range_end: pushable
							.as_ref()
							.and_then(|sp| sp.key_end)
							.or(scan.key_range_end),
						variant_tag: pushable
							.as_ref()
							.and_then(|sp| sp.variant_tag)
							.or(scan.variant_tag),
						partition: partition.or(scan.partition),
					})
				} else {
					input
				};
				let rewritten = if deferred {
					PhysicalPlan::SeriesParameterScan(SeriesParameterScanNode {
						starts,
						ends,
						partition_keys: None,
						scan: self.bump_box(rewritten),
					})
				} else {
					rewritten
				};
				let remaining = match pushable {
					Some(sp) => sp.remaining,
					None => vec![filter.condition],
				};
				if remaining.is_empty() {
					return Ok(rewritten);
				} else {
					return Ok(PhysicalPlan::Filter(FilterNode {
						conditions: remaining,
						input: self.bump_box(rewritten),
					}));
				}
			}
		}

		if let PhysicalPlan::TableScan(ref scan) = input {
			let def = scan.source.def();
			if !def.partition_by.is_empty()
				&& let Some(partition) =
					extract_partition(&filter.condition, &def.columns, &def.partition_by)
			{
				let pruned = PhysicalPlan::TableScan(TableScanNode {
					source: scan.source.clone(),
					partition: Some(partition),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(pruned),
				}));
			}
			if !def.partition_by.is_empty()
				&& let Some(keys) =
					extract_partition_keys(&filter.condition, &def.columns, &def.partition_by)
			{
				let pruned = PhysicalPlan::PartitionParameterScan(PartitionParameterScanNode {
					keys,
					scan: self.bump_box(input),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(pruned),
				}));
			}
		}

		if let PhysicalPlan::RingBufferScan(ref scan) = input {
			let def = scan.source.def();
			if !def.partition_by.is_empty()
				&& let Some(partition) =
					extract_partition(&filter.condition, &def.columns, &def.partition_by)
			{
				let pruned = PhysicalPlan::RingBufferScan(RingBufferScanNode {
					source: scan.source.clone(),
					partition: Some(partition),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(pruned),
				}));
			}
			if !def.partition_by.is_empty()
				&& let Some(keys) =
					extract_partition_keys(&filter.condition, &def.columns, &def.partition_by)
			{
				let pruned = PhysicalPlan::PartitionParameterScan(PartitionParameterScanNode {
					keys,
					scan: self.bump_box(input),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(pruned),
				}));
			}
		}

		if let PhysicalPlan::ViewScan(ref scan) = input {
			let def = scan.source.def();
			if !def.partition_by().is_empty()
				&& let Some(partition) =
					extract_partition(&filter.condition, def.columns(), def.partition_by())
			{
				let pruned = PhysicalPlan::ViewScan(ViewScanNode {
					source: scan.source.clone(),
					partition: Some(partition),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(pruned),
				}));
			}
			if !def.partition_by().is_empty()
				&& let Some(keys) =
					extract_partition_keys(&filter.condition, def.columns(), def.partition_by())
			{
				let pruned = PhysicalPlan::PartitionParameterScan(PartitionParameterScanNode {
					keys,
					scan: self.bump_box(input),
				});
				return Ok(PhysicalPlan::Filter(FilterNode {
					conditions: vec![filter.condition],
					input: self.bump_box(pruned),
				}));
			}
		}

		return Ok(PhysicalPlan::Filter(FilterNode {
			conditions: vec![filter.condition],
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_gate(
		&mut self,
		rx: &mut Transaction<'_>,
		gate: logical::GateNode,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;

		let vars = extract_variable_names(&gate.condition);
		if let Some(pushed) = try_remote_push_down_with_vars(&input, || Some(gate.rql.clone()), vars) {
			return Ok(pushed);
		}

		return Ok(PhysicalPlan::Gate(GateNode {
			conditions: vec![gate.condition],
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_generator(&mut self, generator: logical::GeneratorNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::Generator(GeneratorNode {
			name: self.interner.intern_fragment(&generator.name),
			expressions: generator.expressions,
		}));
	}

	#[inline(never)]
	fn compile_delete_table(
		&mut self,
		rx: &mut Transaction<'_>,
		target: Option<MaybeQualifiedTableIdentifier<'bump>>,
		returning: Option<Vec<Expression>>,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|i| self.bump_box(i));

		let target = if let Some(table_id) = target {
			let ns_segments: Vec<&str> = table_id.namespace.iter().map(|n| n.text()).collect();
			let namespace = self.catalog.find_namespace_by_segments(rx, &ns_segments)?.unwrap();
			let Some(table_def) =
				self.catalog.find_table_by_name(rx, namespace.id(), table_id.name.text())?
			else {
				return_error!(table_not_found(
					self.interner.intern_fragment(&table_id.name),
					namespace.name(),
					table_id.name.text()
				));
			};

			let namespace_id = if let Some(n) = table_id.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(namespace.name())
			} else {
				Fragment::internal(namespace.name())
			};
			let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
			Some(ResolvedTable::new(
				self.interner.intern_fragment(&table_id.name),
				resolved_namespace,
				table_def,
			))
		} else {
			None
		};

		return Ok(PhysicalPlan::Delete(DeleteTableNode {
			input,
			target,
			returning,
		}));
	}

	#[inline(never)]
	fn compile_delete_ring_buffer(
		&mut self,
		rx: &mut Transaction<'_>,
		target: MaybeQualifiedRingBufferIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|i| self.bump_box(i));

		let ringbuffer_id = target;
		let ns_segments: Vec<&str> = ringbuffer_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = ringbuffer_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(ringbuffer_def) =
			self.catalog.find_ringbuffer_by_name(rx, namespace.id(), ringbuffer_id.name.text())?
		else {
			return_error!(ringbuffer_not_found(
				self.interner.intern_fragment(&ringbuffer_id.name),
				namespace.name(),
				ringbuffer_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = ringbuffer_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedRingBuffer::new(
			self.interner.intern_fragment(&ringbuffer_id.name),
			resolved_namespace,
			ringbuffer_def,
		);

		return Ok(PhysicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
			input,
			target,
			returning,
		}));
	}

	#[inline(never)]
	fn compile_delete_series(
		&mut self,
		rx: &mut Transaction<'_>,
		target: MaybeQualifiedSeriesIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|i| self.bump_box(i));

		let series_id = target;
		let ns_segments: Vec<&str> = series_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = series_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(series_def) = self.catalog.find_series_by_name(rx, namespace.id(), series_id.name.text())?
		else {
			return_error!(series_not_found(
				self.interner.intern_fragment(&series_id.name),
				namespace.name(),
				series_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = series_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedSeries::new(
			self.interner.intern_fragment(&series_id.name),
			resolved_namespace,
			series_def,
		);

		return Ok(PhysicalPlan::DeleteSeries(DeleteSeriesNode {
			input,
			target,
			returning,
		}));
	}

	#[inline(never)]
	fn compile_update(
		&mut self,
		rx: &mut Transaction<'_>,
		target: Option<MaybeQualifiedTableIdentifier<'bump>>,
		returning: Option<Vec<Expression>>,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let input = self.bump_box(input);

		let target = if let Some(table_id) = target {
			let ns_segments: Vec<&str> = table_id.namespace.iter().map(|n| n.text()).collect();
			let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
				let ns_name = ns_segments.join("::");
				let fragment = table_id
					.namespace
					.first()
					.map(|n| self.interner.intern_fragment(n))
					.unwrap_or_else(|| Fragment::internal(&ns_name));
				return_error!(namespace_not_found(fragment, &ns_name));
			};
			let Some(table_def) =
				self.catalog.find_table_by_name(rx, namespace.id(), table_id.name.text())?
			else {
				return_error!(table_not_found(
					self.interner.intern_fragment(&table_id.name),
					namespace.name(),
					table_id.name.text()
				));
			};

			let namespace_id = if let Some(n) = table_id.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(namespace.name())
			} else {
				Fragment::internal(namespace.name())
			};
			let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
			Some(ResolvedTable::new(
				self.interner.intern_fragment(&table_id.name),
				resolved_namespace,
				table_def,
			))
		} else {
			None
		};

		return Ok(PhysicalPlan::Update(UpdateTableNode {
			input,
			target,
			returning,
		}));
	}

	#[inline(never)]
	fn compile_update_ring_buffer(
		&mut self,
		rx: &mut Transaction<'_>,
		target: MaybeQualifiedRingBufferIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let input = self.bump_box(input);

		let ringbuffer_id = target;
		let ns_segments: Vec<&str> = ringbuffer_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = ringbuffer_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(ringbuffer_def) =
			self.catalog.find_ringbuffer_by_name(rx, namespace.id(), ringbuffer_id.name.text())?
		else {
			return_error!(ringbuffer_not_found(
				self.interner.intern_fragment(&ringbuffer_id.name),
				namespace.name(),
				ringbuffer_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = ringbuffer_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedRingBuffer::new(
			self.interner.intern_fragment(&ringbuffer_id.name),
			resolved_namespace,
			ringbuffer_def,
		);

		return Ok(PhysicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
			input,
			target,
			returning,
		}));
	}

	#[inline(never)]
	fn compile_update_series(
		&mut self,
		rx: &mut Transaction<'_>,
		target: MaybeQualifiedSeriesIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let input = self.bump_box(input);

		let series_id = target;
		let ns_segments: Vec<&str> = series_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = series_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(series_def) = self.catalog.find_series_by_name(rx, namespace.id(), series_id.name.text())?
		else {
			return_error!(series_not_found(
				self.interner.intern_fragment(&series_id.name),
				namespace.name(),
				series_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = series_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedSeries::new(
			self.interner.intern_fragment(&series_id.name),
			resolved_namespace,
			series_def,
		);

		return Ok(PhysicalPlan::UpdateSeries(UpdateSeriesNode {
			input,
			target,
			returning,
		}));
	}

	#[inline(never)]
	fn compile_join_inner(
		&mut self,
		rx: &mut Transaction<'_>,
		on: Vec<Expression>,
		alias: Option<BumpFragment<'bump>>,
		with: JoinWith,
		rql: String,
		input: BumpBox<'bump, QueryTree<'bump>>,
		right: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let left = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let right = self.compile_tree(rx, BumpBox::into_inner(right))?;

		if let (PhysicalPlan::RemoteScan(l), PhysicalPlan::RemoteScan(r)) = (&left, &right)
			&& l.address == r.address
		{
			let mut pushed = l.clone();
			pushed.remote_rql = format!("{} | {}", pushed.remote_rql, rql);
			pushed.variables.extend(r.variables.iter().cloned());
			return Ok(PhysicalPlan::RemoteScan(pushed));
		}

		let alias = alias.map(|a| self.interner.intern_fragment(&a));
		return Ok(PhysicalPlan::JoinInner(JoinInnerNode {
			left: self.bump_box(left),
			right: self.bump_box(right),
			on,
			alias,
			with,
		}));
	}

	#[inline(never)]
	fn compile_join_left(
		&mut self,
		rx: &mut Transaction<'_>,
		on: Vec<Expression>,
		alias: Option<BumpFragment<'bump>>,
		with: JoinWith,
		rql: String,
		input: BumpBox<'bump, QueryTree<'bump>>,
		right: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let left = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let right = self.compile_tree(rx, BumpBox::into_inner(right))?;

		if let (PhysicalPlan::RemoteScan(l), PhysicalPlan::RemoteScan(r)) = (&left, &right)
			&& l.address == r.address
		{
			let mut pushed = l.clone();
			pushed.remote_rql = format!("{} | {}", pushed.remote_rql, rql);
			pushed.variables.extend(r.variables.iter().cloned());
			return Ok(PhysicalPlan::RemoteScan(pushed));
		}

		let alias = alias.map(|a| self.interner.intern_fragment(&a));
		return Ok(PhysicalPlan::JoinLeft(JoinLeftNode {
			left: self.bump_box(left),
			right: self.bump_box(right),
			on,
			alias,
			with,
		}));
	}

	#[inline(never)]
	fn compile_join_natural(
		&mut self,
		rx: &mut Transaction<'_>,
		join_type: JoinType,
		fragment: BumpFragment<'bump>,
		alias: Option<BumpFragment<'bump>>,
		with: JoinWith,
		rql: String,
		input: BumpBox<'bump, QueryTree<'bump>>,
		right: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let left = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let right = self.compile_tree(rx, BumpBox::into_inner(right))?;

		if let (PhysicalPlan::RemoteScan(l), PhysicalPlan::RemoteScan(r)) = (&left, &right)
			&& l.address == r.address
		{
			let mut pushed = l.clone();
			pushed.remote_rql = format!("{} | {}", pushed.remote_rql, rql);
			pushed.variables.extend(r.variables.iter().cloned());
			return Ok(PhysicalPlan::RemoteScan(pushed));
		}

		let alias = alias.map(|a| self.interner.intern_fragment(&a));
		return Ok(PhysicalPlan::JoinNatural(JoinNaturalNode {
			left: self.bump_box(left),
			right: self.bump_box(right),
			join_type,
			fragment: self.interner.intern_fragment(&fragment),
			alias,
			with,
		}));
	}

	#[inline(never)]
	fn compile_lookup(
		&mut self,
		rx: &mut Transaction<'_>,
		join_type: JoinType,
		on: Vec<Expression>,
		alias: BumpFragment<'bump>,
		with: LookupWith,
		input: BumpBox<'bump, QueryTree<'bump>>,
		right: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let alias = self.interner.intern_fragment(&alias);
		let left = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let right = self.compile_tree(rx, BumpBox::into_inner(right))?;
		if let PhysicalPlan::RemoteScan(remote) = &right {
			return_error!(lookup_right_unsupported(alias, &remote.remote_name));
		}

		return Ok(PhysicalPlan::Lookup(LookupNode {
			left: self.bump_box(left),
			right: self.bump_box(right),
			on,
			alias,
			join_type,
			with,
		}));
	}

	#[inline(never)]
	fn compile_order(
		&mut self,
		rx: &mut Transaction<'_>,
		order: logical::OrderNode,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;

		if let Some(pushed) = try_remote_push_down(&input, || Some(order.rql.clone())) {
			return Ok(pushed);
		}

		return Ok(PhysicalPlan::Sort(SortNode {
			by: order.by,
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_changes(
		&mut self,
		rx: &mut Transaction<'_>,
		changes: logical::ChangesNode,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;
		return Ok(PhysicalPlan::Changes(ChangesNode {
			with: changes.with,
			fragment: changes.fragment,
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_distinct(
		&mut self,
		rx: &mut Transaction<'_>,
		distinct: logical::DistinctNode<'bump>,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;

		if let Some(pushed) = try_remote_push_down(&input, || Some(distinct.rql.clone())) {
			return Ok(pushed);
		}

		let mut resolved_columns = Vec::with_capacity(distinct.columns.len());
		for col in distinct.columns {
			let namespace = ResolvedNamespace::new(
				Fragment::internal("_context"),
				Namespace::Local {
					id: NamespaceId::SYSTEM,
					name: "_context".to_string(),
					local_name: "_context".to_string(),
					parent_id: NamespaceId::ROOT,
				},
			);

			let table_def = Table {
				id: TableId(1),
				namespace: NamespaceId::SYSTEM,
				name: "_context".to_string(),
				columns: vec![],
				primary_key: None,
				partition_by: vec![],
				time: TimeSource::Processing,
			};

			let resolved_table = ResolvedTable::new(Fragment::internal("_context"), namespace, table_def);

			let resolved_source = ResolvedObject::Table(resolved_table);

			let column_def = Column {
				id: ColumnId(1),
				name: col.name.text().to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Utf8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			};

			resolved_columns.push(ResolvedColumn::new(
				self.interner.intern_fragment(&col.name),
				resolved_source,
				column_def,
			));
		}

		return Ok(PhysicalPlan::Distinct(DistinctNode {
			columns: resolved_columns,
			with: distinct.with,
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_map(
		&mut self,
		rx: &mut Transaction<'_>,
		map: logical::MapNode,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|p| self.bump_box(p));

		if let Some(ref boxed_input) = input {
			let mut vars = Vec::new();
			for expr in &map.map {
				vars.extend(extract_variable_names(expr));
			}
			if let Some(pushed) =
				try_remote_push_down_with_vars(boxed_input, || Some(map.rql.clone()), vars)
			{
				return Ok(pushed);
			}
		}

		return Ok(PhysicalPlan::Map(MapNode {
			map: map.map,
			input,
		}));
	}

	#[inline(never)]
	fn compile_extend(
		&mut self,
		rx: &mut Transaction<'_>,
		extend: logical::ExtendNode,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|p| self.bump_box(p));

		if let Some(ref boxed_input) = input {
			let mut vars = Vec::new();
			for expr in &extend.extend {
				vars.extend(extract_variable_names(expr));
			}
			if let Some(pushed) =
				try_remote_push_down_with_vars(boxed_input, || Some(extend.rql.clone()), vars)
			{
				return Ok(pushed);
			}
		}

		return Ok(PhysicalPlan::Extend(ExtendNode {
			extend: extend.extend,
			input,
		}));
	}

	#[inline(never)]
	fn compile_patch(
		&mut self,
		rx: &mut Transaction<'_>,
		patch: logical::PatchNode,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|p| self.bump_box(p));

		if let Some(ref boxed_input) = input {
			let mut vars = Vec::new();
			for expr in &patch.assignments {
				vars.extend(extract_variable_names(expr));
			}
			if let Some(pushed) =
				try_remote_push_down_with_vars(boxed_input, || Some(patch.rql.clone()), vars)
			{
				return Ok(pushed);
			}
		}

		return Ok(PhysicalPlan::Patch(PatchNode {
			assignments: patch.assignments,
			input,
		}));
	}

	#[inline(never)]
	fn compile_apply(
		&mut self,
		rx: &mut Transaction<'_>,
		apply: logical::ApplyNode<'bump>,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|p| self.bump_box(p));

		if let Some(ref boxed_input) = input {
			let mut vars = Vec::new();
			for expr in &apply.params {
				vars.extend(extract_variable_names(expr));
			}
			if let Some(pushed) =
				try_remote_push_down_with_vars(boxed_input, || Some(apply.rql.clone()), vars)
			{
				return Ok(pushed);
			}
		}

		return Ok(PhysicalPlan::Apply(ApplyNode {
			operator: self.interner.intern_fragment(&apply.operator),
			params: apply.params,
			with: apply.with,
			input,
		}));
	}

	#[inline(never)]
	fn compile_source_scan(&mut self, scan: logical::ObjectScanNode) -> Result<PhysicalPlan<'bump>> {
		match &scan.source {
			ResolvedObject::Table(resolved_table) => {
				if let Some(index) = &scan.index {
					return Ok(PhysicalPlan::IndexScan(IndexScanNode {
						source: resolved_table.clone(),
						index_name: index.identifier().text().to_string(),
					}));
				} else {
					return Ok(PhysicalPlan::TableScan(TableScanNode {
						source: resolved_table.clone(),
						partition: None,
					}));
				}
			}
			ResolvedObject::View(resolved_view) => {
				if scan.index.is_some() {
					unimplemented!("views do not support indexes yet");
				}
				return Ok(PhysicalPlan::ViewScan(ViewScanNode {
					source: resolved_view.clone(),
					partition: None,
				}));
			}
			ResolvedObject::DeferredView(resolved_view) => {
				if scan.index.is_some() {
					unimplemented!("views do not support indexes yet");
				}
				let view = ResolvedView::new(
					resolved_view.identifier().clone(),
					resolved_view.namespace().clone(),
					resolved_view.def().clone(),
				);
				return Ok(PhysicalPlan::ViewScan(ViewScanNode {
					source: view,
					partition: None,
				}));
			}
			ResolvedObject::TransactionalView(resolved_view) => {
				if scan.index.is_some() {
					unimplemented!("views do not support indexes yet");
				}
				let view = ResolvedView::new(
					resolved_view.identifier().clone(),
					resolved_view.namespace().clone(),
					resolved_view.def().clone(),
				);
				return Ok(PhysicalPlan::ViewScan(ViewScanNode {
					source: view,
					partition: None,
				}));
			}

			ResolvedObject::TableVirtual(resolved_virtual) => {
				if scan.index.is_some() {
					unimplemented!("virtual tables do not support indexes yet");
				}
				return Ok(PhysicalPlan::TableVirtualScan(TableVirtualScanNode {
					source: resolved_virtual.clone(),
					pushdown_context: None,
				}));
			}
			ResolvedObject::RingBuffer(resolved_ringbuffer) => {
				if scan.index.is_some() {
					unimplemented!("ring buffers do not support indexes yet");
				}
				return Ok(PhysicalPlan::RingBufferScan(RingBufferScanNode {
					source: resolved_ringbuffer.clone(),
					partition: None,
				}));
			}

			ResolvedObject::Dictionary(resolved_dictionary) => {
				if scan.index.is_some() {
					unimplemented!("dictionaries do not support indexes");
				}
				return Ok(PhysicalPlan::DictionaryScan(DictionaryScanNode {
					source: resolved_dictionary.clone(),
				}));
			}
			ResolvedObject::Series(resolved_series) => {
				if scan.index.is_some() {
					unimplemented!("series do not support indexes");
				}
				return Ok(PhysicalPlan::SeriesScan(SeriesScanNode {
					source: resolved_series.clone(),
					key_range_start: None,
					key_range_end: None,
					variant_tag: None,
					partition: None,
				}));
			}
			ResolvedObject::Queue(resolved_queue) => {
				if scan.index.is_some() {
					unimplemented!("queues do not support indexes");
				}
				return Ok(PhysicalPlan::QueueScan(QueueScanNode {
					source: resolved_queue.clone(),
				}));
			}
		}
	}

	#[inline(never)]
	fn compile_remote_scan(&mut self, scan: logical::RemoteScanNode) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::RemoteScan(nodes::RemoteScanNode {
			address: scan.address,
			token: scan.token,
			remote_rql: format!("FROM {}::{}", scan.local_namespace, scan.remote_name),
			local_namespace: scan.local_namespace,
			remote_name: scan.remote_name,
			variables: Vec::new(),
		}));
	}

	#[inline(never)]
	fn compile_take(
		&mut self,
		rx: &mut Transaction<'_>,
		take: logical::TakeNode,
		input: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self.compile_tree(rx, BumpBox::into_inner(input))?;

		if let Some(pushed) = try_remote_push_down(&input, || Some(format!("TAKE {}", take.take))) {
			return Ok(pushed);
		}

		return Ok(PhysicalPlan::Take(TakeNode {
			take: take.take,
			input: self.bump_box(input),
		}));
	}

	#[inline(never)]
	fn compile_window(
		&mut self,
		rx: &mut Transaction<'_>,
		window: logical::query::window::WindowNode,
		input: Option<BumpBox<'bump, QueryTree<'bump>>>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = input.map(|i| self.compile_tree(rx, BumpBox::into_inner(i))).transpose()?;
		let input = input.map(|p| self.bump_box(p));
		return Ok(PhysicalPlan::Window(WindowNode {
			group_by: window.group_by,
			aggregations: window.aggregations,
			with: window.with,
			fragment: window.fragment,
			input,
		}));
	}

	#[inline(never)]
	fn compile_variable_source(
		&mut self,
		source: logical::VariableSourceNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let variable_expr = VariableExpression {
			fragment: self.interner.intern_fragment(&source.name),
		};

		return Ok(PhysicalPlan::Variable(VariableNode {
			variable_expr,
		}));
	}

	#[inline(never)]
	fn compile_append_query(
		&mut self,
		rx: &mut Transaction<'_>,
		fragment: BumpFragment<'bump>,
		input: BumpBox<'bump, QueryTree<'bump>>,
		right: BumpBox<'bump, QueryTree<'bump>>,
	) -> Result<PhysicalPlan<'bump>> {
		let left = self.compile_tree(rx, BumpBox::into_inner(input))?;
		let right = self.compile_tree(rx, BumpBox::into_inner(right))?;
		return Ok(PhysicalPlan::Append(AppendPhysicalNode::Query {
			fragment: self.interner.intern_fragment(&fragment),
			left: self.bump_box(left),
			right: self.bump_box(right),
		}));
	}

	fn compile_statement(
		&mut self,
		rx: &mut Transaction<'_>,
		plan: LogicalPlan<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		match plan {
			LogicalPlan::CreateNamespace(create) => self.compile_create_namespace(rx, create),

			LogicalPlan::CreateRemoteNamespace(create) => self.compile_create_remote_namespace(rx, create),

			LogicalPlan::CreateTable(create) => self.compile_create_table(rx, create),

			LogicalPlan::CreateRingBuffer(create) => self.compile_create_ringbuffer(rx, create),

			LogicalPlan::CreateQueue(create) => self.compile_create_queue(rx, create),

			LogicalPlan::CreateDeferredView(create) => self.compile_create_deferred(rx, create),

			LogicalPlan::CreateDeferredWindowedViewGroup(create) => {
				self.compile_create_deferred_windowed_view_group(rx, create)
			}

			LogicalPlan::CreateTransactionalView(create) => self.compile_create_transactional(rx, create),

			LogicalPlan::CreateDictionary(create) => self.compile_create_dictionary(rx, create),

			LogicalPlan::CreateSumType(create) => self.compile_create_sumtype(rx, create),

			LogicalPlan::AlterSequence(alter) => self.compile_alter_sequence(rx, alter),

			LogicalPlan::CreatePrimaryKey(create) => self.compile_create_primary_key(rx, create),

			LogicalPlan::CreateColumnProperty(create) => self.compile_create_column_property(rx, create),

			LogicalPlan::CreateProcedure(create) => self.compile_create_procedure(rx, create),

			LogicalPlan::CreateTest(create) => self.compile_create_test(rx, create),

			LogicalPlan::RunTests(scope) => self.compile_run_tests(rx, scope),

			LogicalPlan::CreateSeries(create) => self.compile_create_series(rx, create),

			LogicalPlan::CreateEvent(create) => self.compile_create_event(rx, create),

			LogicalPlan::CreateSource(create) => self.compile_create_source(rx, create),

			LogicalPlan::CreateSink(create) => self.compile_create_sink(rx, create),

			LogicalPlan::CreateBinding(create) => self.compile_create_binding(rx, create),

			LogicalPlan::CreateRelationship(create) => {
				self.compile_create_relationship(rx, BumpBox::into_inner(create))
			}

			LogicalPlan::CreateMigration(create) => self.compile_create_migration(create),

			LogicalPlan::Migrate(node) => {
				return Ok(PhysicalPlan::Migrate(nodes::MigrateNode {
					target: node.target,
				}));
			}

			LogicalPlan::RollbackMigration(node) => {
				return Ok(PhysicalPlan::RollbackMigration(nodes::RollbackMigrationNode {
					target: node.target,
				}));
			}

			LogicalPlan::Dispatch(dispatch) => self.compile_dispatch(rx, dispatch),

			LogicalPlan::AlterTable(alter) => self.compile_alter_table(rx, alter),

			LogicalPlan::AlterRemoteNamespace(alter) => self.compile_alter_remote_namespace(alter),

			LogicalPlan::DropNamespace(drop) => self.compile_drop_namespace(rx, drop),
			LogicalPlan::DropTable(drop) => self.compile_drop_table(rx, drop),
			LogicalPlan::DropView(drop) => self.compile_drop_view(rx, drop),
			LogicalPlan::DropRingBuffer(drop) => self.compile_drop_ringbuffer(rx, drop),
			LogicalPlan::DropQueue(drop) => self.compile_drop_queue(rx, drop),
			LogicalPlan::DropDictionary(drop) => self.compile_drop_dictionary(rx, drop),
			LogicalPlan::DropSumType(drop) => self.compile_drop_sumtype(rx, drop),
			LogicalPlan::DropSeries(drop) => self.compile_drop_series(rx, drop),
			LogicalPlan::DropSource(drop) => self.compile_drop_source(rx, drop),
			LogicalPlan::DropSink(drop) => self.compile_drop_sink(rx, drop),
			LogicalPlan::DropWindowedViewGroup(drop) => self.compile_drop_windowed_view_group(rx, drop),
			LogicalPlan::DropProcedure(drop) => self.compile_drop_procedure(rx, drop),
			LogicalPlan::DropHandler(drop) => self.compile_drop_handler(rx, drop),
			LogicalPlan::DropTest(drop) => self.compile_drop_test(rx, drop),
			LogicalPlan::DropBinding(drop) => self.compile_drop_binding(rx, drop),
			LogicalPlan::DropRelationship(drop) => self.compile_drop_relationship(rx, drop),

			LogicalPlan::CreateIdentity(node) => self.compile_create_identity(node),
			LogicalPlan::AlterIdentity(node) => self.compile_alter_identity(node),
			LogicalPlan::CreateIdentityAttribute(node) => self.compile_create_identity_attribute(node),
			LogicalPlan::CreateRole(node) => {
				return Ok(PhysicalPlan::CreateRole(nodes::CreateRoleNode {
					name: self.interner.intern_fragment(&node.name),
				}));
			}
			LogicalPlan::Grant(node) => self.compile_grant(node),
			LogicalPlan::Revoke(node) => self.compile_revoke(node),
			LogicalPlan::DropIdentity(node) => self.compile_drop_identity(node),
			LogicalPlan::DropIdentityAttribute(node) => self.compile_drop_identity_attribute(node),
			LogicalPlan::DropRole(node) => self.compile_drop_role(node),
			LogicalPlan::CreateAuthentication(node) => self.compile_create_authentication(node),
			LogicalPlan::DropAuthentication(node) => self.compile_drop_authentication(node),
			LogicalPlan::CreatePolicy(node) => self.compile_create_policy(rx, node),
			LogicalPlan::AlterPolicy(node) => self.compile_alter_policy(node),
			LogicalPlan::DropPolicy(node) => self.compile_drop_policy(node),
			LogicalPlan::CreateAuthenticationMethod(node) => {
				self.compile_create_authentication_method(node)
			}
			LogicalPlan::AlterAuthenticationMethod(node) => self.compile_alter_authentication_method(node),
			LogicalPlan::DropAuthenticationMethod(node) => self.compile_drop_authentication_method(node),

			LogicalPlan::AssertBlock(node) => self.compile_assert_block(node),

			LogicalPlan::InsertTable(insert) => self.compile_insert_table(rx, insert),

			LogicalPlan::InsertRingBuffer(insert_rb) => self.compile_insert_ring_buffer(rx, insert_rb),

			LogicalPlan::InsertQueue(insert_queue) => self.compile_insert_queue(rx, insert_queue),

			LogicalPlan::InsertDictionary(insert_dict) => self.compile_insert_dictionary(rx, insert_dict),

			LogicalPlan::InsertSeries(insert_series) => self.compile_insert_series(rx, insert_series),

			LogicalPlan::Declare(declare_node) => self.compile_declare(rx, declare_node),

			LogicalPlan::Assign(assign_node) => self.compile_assign(rx, assign_node),

			LogicalPlan::Append(logical::AppendNode::IntoVariable {
				target,
				source,
			}) => self.compile_append(rx, target, source),

			LogicalPlan::Conditional(conditional_node) => self.compile_conditional(rx, conditional_node),

			LogicalPlan::Loop(loop_node) => self.compile_loop(rx, loop_node),

			LogicalPlan::While(while_node) => self.compile_while(rx, while_node),

			LogicalPlan::For(for_node) => self.compile_for(rx, for_node),

			LogicalPlan::Break => {
				return Ok(PhysicalPlan::Break);
			}

			LogicalPlan::Continue => {
				return Ok(PhysicalPlan::Continue);
			}

			LogicalPlan::DefineFunction(def_node) => self.compile_define_function(rx, def_node),

			LogicalPlan::Return(ret_node) => self.compile_return(rx, ret_node),

			LogicalPlan::CallFunction(call_node) => self.compile_call_function(call_node),

			LogicalPlan::DefineClosure(closure_node) => self.compile_define_closure(rx, closure_node),

			_ => unimplemented!(),
		}
	}

	#[inline(never)]
	fn compile_run_tests(
		&mut self,
		rx: &mut Transaction<'_>,
		scope: logical::RunTestsNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		match scope {
			logical::RunTestsNode::All => {
				return Ok(PhysicalPlan::RunTests(nodes::RunTestsNode {
					scope: nodes::RunTestsScope::All,
				}));
			}
			logical::RunTestsNode::Namespace(ns) => {
				let ns_segments: Vec<&str> = ns.segments.iter().map(|n| n.text()).collect();
				let namespace_name = ns_segments.join("::");
				let Some(namespace_def) = self.catalog.find_namespace_by_segments(rx, &ns_segments)?
				else {
					let ns_fragment = if let Some(n) = ns.segments.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_name)
					} else {
						Fragment::internal("default")
					};
					return_error!(namespace_not_found(ns_fragment, &namespace_name));
				};
				let namespace_id = if let Some(n) = ns.segments.first() {
					let interned = self.interner.intern_fragment(n);
					interned.with_text(&namespace_name)
				} else {
					Fragment::internal(namespace_name)
				};
				let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
				return Ok(PhysicalPlan::RunTests(nodes::RunTestsNode {
					scope: nodes::RunTestsScope::Namespace(resolved_namespace),
				}));
			}
			logical::RunTestsNode::Single(test) => {
				let ns_segments: Vec<&str> = test.namespace.iter().map(|n| n.text()).collect();
				let namespace_name = if ns_segments.is_empty() {
					"default".to_string()
				} else {
					ns_segments.join("::")
				};
				let Some(namespace_def) = self.catalog.find_namespace_by_segments(rx, &ns_segments)?
				else {
					let ns_fragment = if let Some(n) = test.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_name)
					} else {
						Fragment::internal("default")
					};
					return_error!(namespace_not_found(ns_fragment, &namespace_name));
				};
				let namespace_id = if let Some(n) = test.namespace.first() {
					let interned = self.interner.intern_fragment(n);
					interned.with_text(&namespace_name)
				} else {
					Fragment::internal(namespace_name)
				};
				let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
				return Ok(PhysicalPlan::RunTests(nodes::RunTestsNode {
					scope: nodes::RunTestsScope::Single(
						resolved_namespace,
						self.interner.intern_fragment(&test.name).text().to_string(),
					),
				}));
			}
		}
	}

	#[inline(never)]
	fn compile_create_migration(&mut self, create: logical::CreateMigrationNode) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::CreateMigration(nodes::CreateMigrationNode {
			name: create.name,
			body_source: create.body_source,
			rollback_body_source: create.rollback_body_source,
		}));
	}

	#[inline(never)]
	fn compile_alter_remote_namespace(
		&mut self,
		alter: logical::AlterRemoteNamespaceNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_name: String = alter.namespace.iter().map(|s| s.text()).collect::<Vec<_>>().join("::");
		let namespace = Fragment::internal(ns_name);
		return Ok(PhysicalPlan::AlterRemoteNamespace(nodes::AlterRemoteNamespaceNode {
			namespace,
			grpc: self.interner.intern_fragment(&alter.grpc),
		}));
	}

	#[inline(never)]
	fn compile_create_identity(&mut self, node: logical::CreateIdentityNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		let mut attributes = Vec::with_capacity(node.entries.len());
		for entry in node.entries {
			attributes.push(nodes::IdentityAttributeAssignment {
				name: self.interner.intern_fragment(&entry.key),
				value: ExpressionCompiler::compile(entry.value)?,
			});
		}
		return Ok(PhysicalPlan::CreateIdentity(nodes::CreateIdentityNode {
			name: self.interner.intern_fragment(&node.name),
			kind: node.kind,
			attributes,
		}));
	}

	#[inline(never)]
	fn compile_alter_identity(&mut self, node: logical::AlterIdentityNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		let mut attributes = Vec::with_capacity(node.entries.len());
		for entry in node.entries {
			attributes.push(nodes::IdentityAttributeAssignment {
				name: self.interner.intern_fragment(&entry.key),
				value: ExpressionCompiler::compile(entry.value)?,
			});
		}
		return Ok(PhysicalPlan::AlterIdentity(nodes::AlterIdentityNode {
			name: self.interner.intern_fragment(&node.name),
			attributes,
		}));
	}

	#[inline(never)]
	fn compile_create_identity_attribute(
		&mut self,
		node: logical::CreateIdentityAttributeNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let value_type = convert_data_type_with_constraints(&node.value_type)?.get_type();
		return Ok(PhysicalPlan::CreateIdentityAttribute(nodes::CreateIdentityAttributeNode {
			name: self.interner.intern_fragment(&node.name),
			value_type,
		}));
	}

	#[inline(never)]
	fn compile_grant(&mut self, node: logical::GrantNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::Grant(nodes::GrantNode {
			role: self.interner.intern_fragment(&node.role),
			user: self.interner.intern_fragment(&node.user),
		}));
	}

	#[inline(never)]
	fn compile_revoke(&mut self, node: logical::RevokeNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::Revoke(nodes::RevokeNode {
			role: self.interner.intern_fragment(&node.role),
			user: self.interner.intern_fragment(&node.user),
		}));
	}

	#[inline(never)]
	fn compile_drop_identity(&mut self, node: logical::DropIdentityNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::DropIdentity(nodes::DropIdentityNode {
			name: self.interner.intern_fragment(&node.name),
			kind: node.kind,
			if_exists: node.if_exists,
		}));
	}

	#[inline(never)]
	fn compile_drop_identity_attribute(
		&mut self,
		node: logical::DropIdentityAttributeNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::DropIdentityAttribute(nodes::DropIdentityAttributeNode {
			name: self.interner.intern_fragment(&node.name),
			if_exists: node.if_exists,
		}));
	}

	#[inline(never)]
	fn compile_drop_role(&mut self, node: logical::DropRoleNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::DropRole(nodes::DropRoleNode {
			name: self.interner.intern_fragment(&node.name),
			if_exists: node.if_exists,
		}));
	}

	#[inline(never)]
	fn compile_create_authentication(
		&mut self,
		node: logical::CreateAuthenticationNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let mut method_str = String::new();
		let mut config = collections::HashMap::new();
		for entry in &node.entries {
			let key = entry.key.text().to_string();
			let value = entry.value.value().to_string();
			if key == "method" {
				method_str = value;
			} else {
				config.insert(key, value);
			}
		}
		return Ok(PhysicalPlan::CreateAuthentication(nodes::CreateAuthenticationNode {
			user: self.interner.intern_fragment(&node.user),
			method: Fragment::internal(&method_str),
			config,
		}));
	}

	#[inline(never)]
	fn compile_drop_authentication(
		&mut self,
		node: logical::DropAuthenticationNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::DropAuthentication(nodes::DropAuthenticationNode {
			user: self.interner.intern_fragment(&node.user),
			method: self.interner.intern_fragment(&node.method),
			if_exists: node.if_exists,
		}));
	}

	#[inline(never)]
	fn compile_create_policy(
		&mut self,
		rx: &mut Transaction<'_>,
		node: logical::CreatePolicyNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let name = node.name.map(|n| self.interner.intern_fragment(&n));
		let target_type = node.target_type.as_str().to_string();
		let (scope_namespace, scope_object) = match &node.scope {
			AstPolicyScope::Specific(segments) => {
				if segments.len() >= 2 {
					let seg_strs: Vec<&str> = segments.iter().map(|s| s.text()).collect();
					let full_path = seg_strs.join("::");
					let parent_segs = &seg_strs[..seg_strs.len() - 1];
					let parent_path = parent_segs.join("::");
					let ns_fragment = self.interner.intern_fragment(&segments[0]);
					if self.catalog.find_namespace_by_segments(rx, &seg_strs)?.is_some() {
						(Some(ns_fragment.with_text(&full_path)), None)
					} else if self.catalog.find_namespace_by_segments(rx, parent_segs)?.is_some() {
						(
							Some(ns_fragment.with_text(&parent_path)),
							Some(self
								.interner
								.intern_fragment(&segments[segments.len() - 1])),
						)
					} else {
						return_error!(namespace_not_found(
							ns_fragment.with_text(&full_path),
							&full_path,
						));
					}
				} else if segments.len() == 1 {
					(Some(self.interner.intern_fragment(&segments[0])), None)
				} else {
					(None, None)
				}
			}
			AstPolicyScope::NamespaceWide(ns) => (Some(self.interner.intern_fragment(ns)), None),
			AstPolicyScope::Global => (None, None),
		};
		let operations = node
			.operations
			.iter()
			.map(|op| nodes::PolicyOperationNode {
				operation: op.operation.text().to_string(),
				body_source: op.body_source.clone(),
			})
			.collect();
		return Ok(PhysicalPlan::CreatePolicy(nodes::CreatePolicyNode {
			name,
			target_type,
			scope_namespace,
			scope_object,
			operations,
		}));
	}

	#[inline(never)]
	fn compile_alter_policy(&mut self, node: logical::AlterPolicyNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		let enable = node.action == AstAlterPolicyAction::Enable;
		return Ok(PhysicalPlan::AlterPolicy(nodes::AlterPolicyNode {
			target_type: node.target_type.as_str().to_string(),
			name: self.interner.intern_fragment(&node.name),
			enable,
		}));
	}

	#[inline(never)]
	fn compile_drop_policy(&mut self, node: logical::DropPolicyNode<'bump>) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::DropPolicy(nodes::DropPolicyNode {
			target_type: node.target_type.as_str().to_string(),
			name: self.interner.intern_fragment(&node.name),
			if_exists: node.if_exists,
		}));
	}

	#[inline(never)]
	fn compile_create_authentication_method(
		&mut self,
		node: logical::CreateAuthenticationMethodNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::CreateAuthenticationMethod(nodes::CreateAuthenticationMethodNode {
			name: self.interner.intern_fragment(&node.name),
			ttl: node.ttl,
			promote_guest: node.promote_guest,
			properties: node.properties,
		}));
	}

	#[inline(never)]
	fn compile_alter_authentication_method(
		&mut self,
		node: logical::AlterAuthenticationMethodNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::AlterAuthenticationMethod(nodes::AlterAuthenticationMethodNode {
			name: self.interner.intern_fragment(&node.name),
			ttl: node.ttl,
			promote_guest: node.promote_guest,
			properties: node.properties,
		}));
	}

	#[inline(never)]
	fn compile_drop_authentication_method(
		&mut self,
		node: logical::DropAuthenticationMethodNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::DropAuthenticationMethod(nodes::DropAuthenticationMethodNode {
			name: self.interner.intern_fragment(&node.name),
			if_exists: node.if_exists,
		}));
	}

	#[inline(never)]
	fn compile_assert_block(&mut self, node: logical::AssertBlockNode) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::AssertBlock(AssertBlockNode {
			rql: node.rql,
			expect_error: node.expect_error,
			message: node.message,
		}));
	}

	#[inline(never)]
	fn compile_insert_table(
		&mut self,
		rx: &mut Transaction<'_>,
		insert: logical::InsertTableNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self
			.compile(rx, once(BumpBox::into_inner(insert.source)))?
			.expect("Insert source must produce a plan");

		let table = insert.target;
		let ns_segments: Vec<&str> = table.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = table
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(table_def) = self.catalog.find_table_by_name(rx, namespace.id(), table.name.text())? else {
			return_error!(table_not_found(
				self.interner.intern_fragment(&table.name),
				namespace.name(),
				table.name.text()
			));
		};

		let namespace_id = if let Some(n) = table.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target =
			ResolvedTable::new(self.interner.intern_fragment(&table.name), resolved_namespace, table_def);

		return Ok(PhysicalPlan::InsertTable(InsertTableNode {
			input: self.bump_box(input),
			target,
			returning: insert.returning,
		}));
	}

	#[inline(never)]
	fn compile_insert_ring_buffer(
		&mut self,
		rx: &mut Transaction<'_>,
		insert_rb: logical::InsertRingBufferNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self
			.compile(rx, once(BumpBox::into_inner(insert_rb.source)))?
			.expect("Insert source must produce a plan");

		let ringbuffer_id = insert_rb.target;
		let ns_segments: Vec<&str> = ringbuffer_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = ringbuffer_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(ringbuffer_def) =
			self.catalog.find_ringbuffer_by_name(rx, namespace.id(), ringbuffer_id.name.text())?
		else {
			return_error!(ringbuffer_not_found(
				self.interner.intern_fragment(&ringbuffer_id.name),
				namespace.name(),
				ringbuffer_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = ringbuffer_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedRingBuffer::new(
			self.interner.intern_fragment(&ringbuffer_id.name),
			resolved_namespace,
			ringbuffer_def,
		);

		return Ok(PhysicalPlan::InsertRingBuffer(InsertRingBufferNode {
			input: self.bump_box(input),
			target,
			returning: insert_rb.returning,
		}));
	}

	#[inline(never)]
	fn compile_insert_queue(
		&mut self,
		rx: &mut Transaction<'_>,
		insert_queue: logical::InsertQueueNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self
			.compile(rx, once(BumpBox::into_inner(insert_queue.source)))?
			.expect("Insert source must produce a plan");

		let queue_id = insert_queue.target;
		let ns_segments: Vec<&str> = queue_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = queue_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(queue_def) = self.catalog.find_queue_by_name(rx, namespace.id(), queue_id.name.text())? else {
			return_error!(queue_not_found(
				self.interner.intern_fragment(&queue_id.name),
				namespace.name(),
				queue_id.name.text()
			));
		};

		if let Some(key) = &insert_queue.deduplication_key
			&& let Some(deduplicate) = &queue_def.deduplicate
		{
			return Err(RqlError::InsertWithKeyOnDeduplicatingQueue {
				fragment: key.full_fragment_owned(),
				queue: format!("{}::{}", namespace.name(), queue_id.name.text()),
				by: deduplicate.by.join(", "),
			}
			.into());
		}

		let namespace_id = if let Some(n) = queue_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedQueue::new(
			self.interner.intern_fragment(&queue_id.name),
			resolved_namespace,
			queue_def,
		);

		if insert_queue.deduplication_key.is_some() || insert_queue.not_before.is_some() {
			for reserved in [QUEUE_DEDUPLICATION_KEY_FIELD, QUEUE_NOT_BEFORE_FIELD] {
				if target.find_column(reserved).is_some() {
					return_error!(queue_reserved_column_collision(
						self.interner.intern_fragment(&queue_id.name),
						target.name(),
						reserved
					));
				}
			}
		}

		return Ok(PhysicalPlan::InsertQueue(InsertQueueNode {
			input: self.bump_box(input),
			target,
			deduplication_key: insert_queue.deduplication_key,
			not_before: insert_queue.not_before,
			returning: insert_queue.returning,
		}));
	}

	#[inline(never)]
	fn compile_insert_dictionary(
		&mut self,
		rx: &mut Transaction<'_>,
		insert_dict: logical::InsertDictionaryNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self
			.compile(rx, once(BumpBox::into_inner(insert_dict.source)))?
			.expect("Insert source must produce a plan");

		let dictionary_id = insert_dict.target;
		let ns_segments: Vec<&str> = dictionary_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = dictionary_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(dictionary_def) =
			self.catalog.find_dictionary_by_name(rx, namespace.id(), dictionary_id.name.text())?
		else {
			return_error!(dictionary_not_found(
				self.interner.intern_fragment(&dictionary_id.name),
				namespace.name(),
				dictionary_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = dictionary_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedDictionary::new(
			self.interner.intern_fragment(&dictionary_id.name),
			resolved_namespace,
			dictionary_def,
		);

		return Ok(PhysicalPlan::InsertDictionary(InsertDictionaryNode {
			input: self.bump_box(input),
			target,
			returning: insert_dict.returning,
		}));
	}

	#[inline(never)]
	fn compile_insert_series(
		&mut self,
		rx: &mut Transaction<'_>,
		insert_series: logical::InsertSeriesNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let input = self
			.compile(rx, once(BumpBox::into_inner(insert_series.source)))?
			.expect("Insert source must produce a plan");

		let series_id = insert_series.target;
		let ns_segments: Vec<&str> = series_id.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_name = ns_segments.join("::");
			let fragment = series_id
				.namespace
				.first()
				.map(|n| self.interner.intern_fragment(n))
				.unwrap_or_else(|| Fragment::internal(&ns_name));
			return_error!(namespace_not_found(fragment, &ns_name));
		};
		let Some(series_def) = self.catalog.find_series_by_name(rx, namespace.id(), series_id.name.text())?
		else {
			return_error!(series_not_found(
				self.interner.intern_fragment(&series_id.name),
				namespace.name(),
				series_id.name.text()
			));
		};

		let namespace_id = if let Some(n) = series_id.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(namespace.name())
		} else {
			Fragment::internal(namespace.name())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace);
		let target = ResolvedSeries::new(
			self.interner.intern_fragment(&series_id.name),
			resolved_namespace,
			series_def,
		);

		return Ok(PhysicalPlan::InsertSeries(InsertSeriesNode {
			input: self.bump_box(input),
			target,
			returning: insert_series.returning,
		}));
	}

	#[inline(never)]
	fn compile_declare(
		&mut self,
		rx: &mut Transaction<'_>,
		declare_node: logical::DeclareNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let value = match declare_node.value {
			logical::LetValue::Expression(expr) => LetValue::Expression(expr),
			logical::LetValue::Statement(logical_plans) => match self.compile(rx, logical_plans)? {
				Some(plan) => LetValue::Statement(self.bump_box(plan)),
				None => LetValue::Expression(Constant(ConstantExpression::None {
					fragment: Fragment::internal("none"),
				})),
			},
			logical::LetValue::EmptyFrame => LetValue::EmptyFrame,
		};

		return Ok(PhysicalPlan::Declare(DeclareNode {
			name: self.interner.intern_fragment(&declare_node.name),
			value,
		}));
	}

	#[inline(never)]
	fn compile_assign(
		&mut self,
		rx: &mut Transaction<'_>,
		assign_node: logical::AssignNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let value = match assign_node.value {
			logical::AssignValue::Expression(expr) => AssignValue::Expression(expr),
			logical::AssignValue::Statement(logical_plans) => match self.compile(rx, logical_plans)? {
				Some(plan) => AssignValue::Statement(self.bump_box(plan)),
				None => AssignValue::Expression(Constant(ConstantExpression::None {
					fragment: Fragment::internal("none"),
				})),
			},
		};

		return Ok(PhysicalPlan::Assign(AssignNode {
			name: self.interner.intern_fragment(&assign_node.name),
			value,
		}));
	}

	#[inline(never)]
	fn compile_append(
		&mut self,
		rx: &mut Transaction<'_>,
		target: BumpFragment<'bump>,
		source: logical::AppendSourcePlan<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let source = match source {
			logical::AppendSourcePlan::Statement(logical_plans) => {
				let mut physical_plans = Vec::new();
				if let Some(physical_plan) = self.compile(rx, logical_plans)? {
					physical_plans.push(physical_plan);
				}
				AppendPhysicalSource::Statement(physical_plans)
			}
			logical::AppendSourcePlan::Inline(inline) => AppendPhysicalSource::Inline(InlineDataNode {
				rows: inline.rows,
			}),
		};
		return Ok(PhysicalPlan::Append(AppendPhysicalNode::IntoVariable {
			target: self.interner.intern_fragment(&target),
			source,
		}));
	}

	#[inline(never)]
	fn compile_conditional(
		&mut self,
		rx: &mut Transaction<'_>,
		conditional_node: logical::ConditionalNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let then_branch = self.compile_branch(rx, conditional_node.then_branch)?;

		let mut else_ifs = Vec::new();
		for else_if in conditional_node.else_ifs {
			else_ifs.push(ElseIfBranch {
				condition: else_if.condition,
				then_branch: self.compile_branch(rx, else_if.then_branch)?,
			});
		}

		let else_branch = match conditional_node.else_branch {
			Some(statements) => Some(self.compile_branch(rx, statements)?),
			None => None,
		};

		return Ok(PhysicalPlan::Conditional(ConditionalNode {
			condition: conditional_node.condition,
			then_branch,
			else_ifs,
			else_branch,
		}));
	}

	#[inline(never)]
	fn compile_loop(
		&mut self,
		rx: &mut Transaction<'_>,
		loop_node: logical::LoopNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let mut body = Vec::new();
		for statement_plans in loop_node.body {
			if let Some(physical_plan) = self.compile(rx, statement_plans)? {
				body.push(physical_plan);
			}
		}
		return Ok(PhysicalPlan::Loop(LoopNode {
			body,
		}));
	}

	#[inline(never)]
	fn compile_while(
		&mut self,
		rx: &mut Transaction<'_>,
		while_node: logical::WhileNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let mut body = Vec::new();
		for statement_plans in while_node.body {
			if let Some(physical_plan) = self.compile(rx, statement_plans)? {
				body.push(physical_plan);
			}
		}
		return Ok(PhysicalPlan::While(WhileNode {
			condition: while_node.condition,
			body,
		}));
	}

	#[inline(never)]
	fn compile_for(
		&mut self,
		rx: &mut Transaction<'_>,
		for_node: logical::ForNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let iterable = self.compile(rx, for_node.iterable)?.expect("For iterable must produce a plan");
		let mut body = Vec::new();
		for statement_plans in for_node.body {
			if let Some(physical_plan) = self.compile(rx, statement_plans)? {
				body.push(physical_plan);
			}
		}
		return Ok(PhysicalPlan::For(ForNode {
			variable_name: self.interner.intern_fragment(&for_node.variable_name),
			iterable: self.bump_box(iterable),
			body,
		}));
	}

	#[inline(never)]
	fn compile_define_function(
		&mut self,
		rx: &mut Transaction<'_>,
		def_node: logical::function::DefineFunctionNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let mut parameters = Vec::with_capacity(def_node.parameters.len());
		for p in def_node.parameters {
			parameters.push(nodes::FunctionParameter {
				name: self.interner.intern_fragment(&p.name),
				type_constraint: p.type_constraint,
			});
		}

		let mut body = Vec::new();
		for statement_plans in def_node.body {
			if let Some(physical_plan) = self.compile(rx, statement_plans)? {
				body.push(physical_plan);
			}
		}

		return Ok(PhysicalPlan::DefineFunction(DefineFunctionNode {
			name: self.interner.intern_fragment(&def_node.name),
			parameters,
			return_type: def_node.return_type,
			body,
		}));
	}

	#[inline(never)]
	fn compile_return(
		&mut self,
		rx: &mut Transaction<'_>,
		ret_node: logical::function::ReturnNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let value = match ret_node.value {
			Some(logical::function::ReturnValue::Expression(expr)) => Some(ReturnValue::Expression(expr)),
			Some(logical::function::ReturnValue::Statement(logical_plans)) => {
				match self.compile(rx, logical_plans)? {
					Some(plan) => Some(ReturnValue::Statement(self.bump_box(plan))),
					None => Some(ReturnValue::Expression(Constant(ConstantExpression::None {
						fragment: Fragment::internal("none"),
					}))),
				}
			}
			None => None,
		};

		return Ok(PhysicalPlan::Return(ReturnNode {
			value,
		}));
	}

	#[inline(never)]
	fn compile_call_function(
		&mut self,
		call_node: logical::function::CallFunctionNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		return Ok(PhysicalPlan::CallFunction(CallFunctionNode {
			name: self.interner.intern_fragment(&call_node.name),
			arguments: call_node.arguments,
			is_procedure_call: call_node.is_procedure_call,
		}));
	}

	#[inline(never)]
	fn compile_define_closure(
		&mut self,
		rx: &mut Transaction<'_>,
		closure_node: logical::DefineClosureNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let mut parameters = Vec::with_capacity(closure_node.parameters.len());
		for p in closure_node.parameters {
			parameters.push(nodes::FunctionParameter {
				name: self.interner.intern_fragment(&p.name),
				type_constraint: p.type_constraint,
			});
		}

		let mut body = Vec::new();
		for statement_plans in closure_node.body {
			if let Some(physical_plan) = self.compile(rx, statement_plans)? {
				body.push(physical_plan);
			}
		}

		return Ok(PhysicalPlan::DefineClosure(DefineClosureNode {
			parameters,
			body,
		}));
	}
}

fn try_remote_push_down<'a>(
	input: &PhysicalPlan<'a>,
	rql_suffix: impl FnOnce() -> Option<String>,
) -> Option<PhysicalPlan<'a>> {
	try_remote_push_down_with_vars(input, rql_suffix, Vec::new())
}

fn try_remote_push_down_with_vars<'a>(
	input: &PhysicalPlan<'a>,
	rql_suffix: impl FnOnce() -> Option<String>,
	variables: Vec<String>,
) -> Option<PhysicalPlan<'a>> {
	if let PhysicalPlan::RemoteScan(remote) = input
		&& let Some(suffix) = rql_suffix()
	{
		let mut pushed = remote.clone();
		pushed.remote_rql = format!("{} | {}", pushed.remote_rql, suffix);
		for var in variables {
			if !pushed.variables.contains(&var) {
				pushed.variables.push(var);
			}
		}
		return Some(PhysicalPlan::RemoteScan(pushed));
	}
	None
}
