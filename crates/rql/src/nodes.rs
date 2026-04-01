// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections, fmt};

use reifydb_catalog::catalog::{
	ringbuffer::RingBufferColumnToCreate, series::SeriesColumnToCreate, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	common::{JoinType, WindowKind},
	interface::{
		catalog::{
			id::{NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
			namespace::Namespace,
			procedure::{ProcedureParam, ProcedureTrigger},
			property::ColumnPropertyKind,
			series::SeriesKey,
		},
		resolved::{
			ResolvedColumn, ResolvedDictionary, ResolvedNamespace, ResolvedRingBuffer, ResolvedSequence,
			ResolvedSeries, ResolvedShape, ResolvedTable, ResolvedTableVirtual, ResolvedView,
		},
	},
	sort::{SortDirection, SortKey},
};
use reifydb_type::{
	fragment::Fragment,
	value::{
		constraint::TypeConstraint, dictionary::DictionaryId, duration::Duration, sumtype::SumTypeId,
		r#type::Type,
	},
};

use crate::{
	expression::{AliasExpression, Expression, VariableExpression},
	query::QueryPlan,
};

/// Owned primary key definition for physical plan nodes (materialized from bump-allocated logical plan)
#[derive(Debug, Clone)]
pub struct PrimaryKey {
	pub columns: Vec<PrimaryKeyColumn>,
}

/// Owned primary key column for physical plan nodes
#[derive(Debug, Clone)]
pub struct PrimaryKeyColumn {
	pub column: Fragment,
	pub order: Option<SortDirection>,
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateNamespace(CreateNamespaceNode),
	CreateRemoteNamespace(CreateRemoteNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSumType(CreateSumTypeNode),
	CreateSubscription(CreateSubscriptionNode),
	CreatePrimaryKey(CreatePrimaryKeyNode),
	CreateColumnProperty(CreateColumnPropertyNode),
	CreateProcedure(CreateProcedureNode),
	CreateSeries(CreateSeriesNode),
	CreateEvent(CreateEventNode),
	CreateTag(CreateTagNode),
	CreateTest(CreateTestNode),
	RunTests(RunTestsNode),

	CreateMigration(CreateMigrationNode),
	Migrate(MigrateNode),
	RollbackMigration(RollbackMigrationNode),
	Dispatch(DispatchNode),
	// Alter
	AlterSequence(AlterSequenceNode),
	AlterTable(AlterTableNode),
	AlterRemoteNamespace(AlterRemoteNamespaceNode),
	// Mutate
	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),
	UpdateSeries(UpdateSeriesNode),
	// Variable assignment
	Declare(DeclareNode),
	Assign(AssignNode),
	Append(AppendPhysicalNode),
	// Variable resolution
	Variable(VariableNode),
	Environment(EnvironmentNode),
	// Control flow
	Conditional(ConditionalNode),
	Loop(LoopPhysicalNode),
	While(WhilePhysicalNode),
	For(ForPhysicalNode),
	Break,
	Continue,
	// User-defined functions
	DefineFunction(DefineFunctionNode),
	Return(ReturnNode),
	CallFunction(CallFunctionNode),

	// Query
	Aggregate(AggregateNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	IndexScan(IndexScanNode),
	// Row-number optimized access
	RowPointLookup(RowPointLookupNode),
	RowListLookup(RowListLookupNode),
	RowRangeScan(RowRangeScanNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Take(TakeNode),
	Sort(SortNode),
	Map(MapNode),
	Extend(ExtendNode),
	Patch(PatchNode),
	Apply(ApplyNode),
	InlineData(InlineDataNode),
	RemoteScan(RemoteScanNode),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	DictionaryScan(DictionaryScanNode),
	SeriesScan(SeriesScanNode),
	// Series DML
	InsertSeries(InsertSeriesNode),
	DeleteSeries(DeleteSeriesNode),
	Generator(GeneratorNode),
	Window(WindowNode),
	// Auto-scalarization for 1x1 frames
	Scalarize(ScalarizeNode),
	// Auth/Permissions
	CreateIdentity(CreateIdentityNode),
	CreateRole(CreateRoleNode),
	Grant(GrantNode),
	Revoke(RevokeNode),
	DropIdentity(DropIdentityNode),
	DropRole(DropRoleNode),
	CreateAuthentication(CreateAuthenticationNode),
	DropAuthentication(DropAuthenticationNode),
	CreatePolicy(CreatePolicyNode),
	AlterPolicy(AlterPolicyNode),
	DropPolicy(DropPolicyNode),
}

#[derive(Debug, Clone)]
pub enum CompiledViewStorageKind {
	Table,
	RingBuffer {
		capacity: u64,
		propagate_evictions: bool,
		partition_by: Vec<String>,
	},
	Series {
		key: SeriesKey,
	},
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewNode {
	pub namespace: Namespace, // FIXME REsolvedNamespace
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<QueryPlan>,
	pub storage_kind: CompiledViewStorageKind,
	pub tick: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewNode {
	pub namespace: Namespace, // FIXME REsolvedNamespace
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<QueryPlan>,
	pub storage_kind: CompiledViewStorageKind,
	pub tick: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceNode {
	pub segments: Vec<Fragment>,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateRemoteNamespaceNode {
	pub segments: Vec<Fragment>,
	pub if_not_exists: bool,
	pub grpc: Fragment,
	pub token: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct AlterRemoteNamespaceNode {
	pub namespace: Fragment,
	pub grpc: Fragment,
}

#[derive(Debug, Clone)]
pub struct CreateTableNode {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct CreateRingBufferNode {
	pub namespace: ResolvedNamespace,
	pub ringbuffer: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
	pub partition_by: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CreateDictionaryNode {
	pub namespace: Namespace,
	pub dictionary: Fragment,
	pub if_not_exists: bool,
	pub value_type: Type,
	pub id_type: Type,
}

#[derive(Debug, Clone)]
pub struct CreateSumTypeNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub if_not_exists: bool,
	pub variants: Vec<CreateSumTypeVariant>,
}

#[derive(Debug, Clone)]
pub struct CreateSumTypeVariant {
	pub name: String,
	pub columns: Vec<CreateSumTypeColumn>,
}

#[derive(Debug, Clone)]
pub struct CreateSumTypeColumn {
	pub name: String,
	pub column_type: TypeConstraint,
}

#[derive(Debug, Clone)]
pub struct SubscriptionColumnToCreate {
	pub name: String,
	pub ty: Type,
}

#[derive(Debug, Clone)]
pub struct CreateSubscriptionNode {
	pub columns: Vec<SubscriptionColumnToCreate>,
	pub as_clause: Option<Box<QueryPlan>>,
}

#[derive(Debug, Clone)]
pub struct AlterSequenceNode {
	pub sequence: ResolvedSequence,
	pub column: ResolvedColumn,
	pub value: Expression,
}

#[derive(Debug, Clone)]
pub struct AlterTableNode {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub action: AlterTableAction,
}

#[derive(Debug, Clone)]
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
}

// Create Primary Key node
#[derive(Debug, Clone)]
pub struct CreatePrimaryKeyNode {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub columns: Vec<PrimaryKeyColumn>,
}

// Create Procedure node
#[derive(Debug, Clone)]
pub struct CreateProcedureNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub params: Vec<ProcedureParam>,
	pub body_source: String,
	pub trigger: ProcedureTrigger,
	pub is_test: bool,
}

/// Physical node for CREATE SERIES
#[derive(Debug, Clone)]
pub struct CreateSeriesNode {
	pub namespace: ResolvedNamespace,
	pub series: Fragment,
	pub columns: Vec<SeriesColumnToCreate>,
	pub tag: Option<SumTypeId>,
	pub key: SeriesKey,
}

/// Physical node for CREATE EVENT
#[derive(Debug, Clone)]
pub struct CreateEventNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub variants: Vec<CreateSumTypeVariant>,
}

/// Physical node for CREATE TAG
#[derive(Debug, Clone)]
pub struct CreateTagNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub variants: Vec<CreateSumTypeVariant>,
}

/// A resolved key-value config pair
#[derive(Debug, Clone)]
pub struct ConfigPair {
	pub key: Fragment,
	pub value: Fragment,
}

/// Physical node for CREATE SOURCE
#[derive(Debug, Clone)]
pub struct CreateSourceNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub connector: Fragment,
	pub config: Vec<ConfigPair>,
	pub target_namespace: Namespace,
	pub target_name: Fragment,
}

/// Physical node for CREATE SINK
#[derive(Debug, Clone)]
pub struct CreateSinkNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub source_namespace: Namespace,
	pub source_name: Fragment,
	pub connector: Fragment,
	pub config: Vec<ConfigPair>,
}

/// Physical node for DROP SOURCE
#[derive(Debug, Clone)]
pub struct DropSourceNode {
	pub if_exists: bool,
	pub namespace: Namespace,
	pub name: Fragment,
	pub cascade: bool,
}

/// Physical node for DROP SINK
#[derive(Debug, Clone)]
pub struct DropSinkNode {
	pub if_exists: bool,
	pub namespace: Namespace,
	pub name: Fragment,
	pub cascade: bool,
}

// Assert Block node (multi-statement ASSERT or ASSERT ERROR)
#[derive(Debug, Clone)]
pub struct AssertBlockNode {
	pub rql: String,
	pub expect_error: bool,
	pub message: Option<String>,
}

// Create Test node
#[derive(Debug, Clone)]
pub struct CreateTestNode {
	pub namespace: Namespace,
	pub name: Fragment,
	pub cases: Option<String>,
	pub body_source: String,
}

// Run Tests node
#[derive(Debug, Clone)]
pub struct RunTestsNode {
	pub scope: RunTestsScope,
}

#[derive(Debug, Clone)]
pub enum RunTestsScope {
	All,
	Namespace(ResolvedNamespace),
	Single(ResolvedNamespace, String),
}

/// Physical node for CREATE MIGRATION
#[derive(Debug, Clone)]
pub struct CreateMigrationNode {
	pub name: String,
	pub body_source: String,
	pub rollback_body_source: Option<String>,
}

/// Physical node for MIGRATE
#[derive(Debug, Clone)]
pub struct MigrateNode {
	pub target: Option<String>,
}

/// Physical node for ROLLBACK MIGRATION
#[derive(Debug, Clone)]
pub struct RollbackMigrationNode {
	pub target: Option<String>,
}

/// Physical node for DISPATCH
#[derive(Debug, Clone)]
pub struct DispatchNode {
	pub namespace: Namespace,
	pub on_sumtype_id: SumTypeId,
	pub variant_name: String,
	pub fields: Vec<(String, Expression)>,
}

// Create Policy node
#[derive(Debug, Clone)]
pub struct CreateColumnPropertyNode {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub column: Fragment,
	pub properties: Vec<ColumnPropertyKind>,
}

#[derive(Debug, Clone)]
pub enum LetValue {
	Expression(Expression),
	Statement(QueryPlan),
	EmptyFrame,
}

impl fmt::Display for LetValue {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			LetValue::Expression(expr) => write!(f, "{}", expr),
			LetValue::Statement(query) => write!(f, "Statement({:?})", query),
			LetValue::EmptyFrame => write!(f, "EmptyFrame"),
		}
	}
}

#[derive(Debug, Clone)]
pub struct DeclareNode {
	pub name: Fragment,
	pub value: LetValue,
}

#[derive(Debug, Clone)]
pub enum AssignValue {
	Expression(Expression),
	Statement(QueryPlan),
}

impl fmt::Display for AssignValue {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			AssignValue::Expression(expr) => write!(f, "{}", expr),
			AssignValue::Statement(query) => write!(f, "Statement({:?})", query),
		}
	}
}

#[derive(Debug, Clone)]
pub struct AssignNode {
	pub name: Fragment,
	pub value: AssignValue,
}

#[derive(Debug, Clone)]
pub struct VariableNode {
	pub variable_expr: VariableExpression,
}

#[derive(Debug, Clone)]
pub struct EnvironmentNode {}

/// A function parameter in the physical plan
#[derive(Debug, Clone)]
pub struct FunctionParameter {
	/// Parameter name (includes $)
	pub name: Fragment,
	/// Optional type constraint
	pub type_constraint: Option<TypeConstraint>,
}

#[derive(Debug, Clone)]
pub struct ScalarizeNode {
	pub input: Box<QueryPlan>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
	pub input: Box<QueryPlan>,
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct DistinctNode {
	pub input: Box<QueryPlan>,
	pub columns: Vec<ResolvedColumn>,
}

#[derive(Debug, Clone)]
pub struct AssertNode {
	pub input: Option<Box<QueryPlan>>,
	pub conditions: Vec<Expression>,
	pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
	pub input: Box<QueryPlan>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct GateNode {
	pub input: Box<QueryPlan>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct DeleteTableNode {
	pub input: Option<Box<QueryPlan>>,
	pub target: Option<ResolvedTable>,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct InsertTableNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedTable,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct InsertRingBufferNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedRingBuffer,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct InsertDictionaryNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedDictionary,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct UpdateTableNode {
	pub input: Box<QueryPlan>,
	pub target: Option<ResolvedTable>,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct DeleteRingBufferNode {
	pub input: Option<Box<QueryPlan>>,
	pub target: ResolvedRingBuffer,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct UpdateRingBufferNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedRingBuffer,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct UpdateSeriesNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedSeries,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct JoinInnerNode {
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode {
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct JoinNaturalNode {
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub join_type: JoinType,
	pub alias: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct AppendQueryNode {
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct SortNode {
	pub input: Box<QueryPlan>,
	pub by: Vec<SortKey>,
}

#[derive(Debug, Clone)]
pub struct MapNode {
	pub input: Option<Box<QueryPlan>>,
	pub map: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExtendNode {
	pub input: Option<Box<QueryPlan>>,
	pub extend: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct PatchNode {
	pub input: Option<Box<QueryPlan>>,
	pub assignments: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct ApplyNode {
	pub input: Option<Box<QueryPlan>>,
	pub operator: Fragment, // FIXME becomes OperatorIdentifier
	pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct InlineDataNode {
	pub rows: Vec<Vec<AliasExpression>>,
}

#[derive(Debug, Clone)]
pub struct IndexScanNode {
	pub source: ResolvedTable,
	pub index_name: String,
}

#[derive(Debug, Clone)]
pub struct RemoteScanNode {
	pub address: String,
	pub token: Option<String>,
	pub remote_rql: String,
	pub local_namespace: String,
	pub remote_name: String,
	pub variables: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
	pub source: ResolvedTable,
}

#[derive(Debug, Clone)]
pub struct ViewScanNode {
	pub source: ResolvedView,
}

#[derive(Debug, Clone)]
pub struct RingBufferScanNode {
	pub source: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct DictionaryScanNode {
	pub source: ResolvedDictionary,
}

#[derive(Debug, Clone)]
pub struct SeriesScanNode {
	pub source: ResolvedSeries,
	pub key_range_start: Option<u64>,
	pub key_range_end: Option<u64>,
	pub variant_tag: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct InsertSeriesNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedSeries,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct DeleteSeriesNode {
	pub input: Option<Box<QueryPlan>>,
	pub target: ResolvedSeries,
	pub returning: Option<Vec<Expression>>,
}

#[derive(Debug, Clone)]
pub struct GeneratorNode {
	pub name: Fragment,
	pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct TableVirtualScanNode {
	pub source: ResolvedTableVirtual,
	pub pushdown_context: Option<TableVirtualPushdownContext>,
}

#[derive(Debug, Clone)]
pub struct TableVirtualPushdownContext {
	pub filters: Vec<Expression>,
	pub projections: Vec<Expression>,
	pub order_by: Vec<SortKey>,
	pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum TakeLimit {
	Literal(usize),
	Variable(String),
}

impl fmt::Display for TakeLimit {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			TakeLimit::Literal(n) => write!(f, "{}", n),
			TakeLimit::Variable(name) => write!(f, "${}", name),
		}
	}
}

#[derive(Debug, Clone)]
pub struct TakeNode {
	pub input: Box<QueryPlan>,
	pub take: TakeLimit,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub input: Option<Box<QueryPlan>>,
	pub kind: WindowKind,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub ts: Option<String>,
}

/// O(1) point lookup by row number: `filter rownum == N`
#[derive(Debug, Clone)]
pub struct RowPointLookupNode {
	/// The source to look up in (table, ring buffer, etc.)
	pub source: ResolvedShape,
	/// The row number to fetch
	pub row_number: u64,
}

/// O(k) list lookup by row numbers: `filter rownum in [a, b, c]`
#[derive(Debug, Clone)]
pub struct RowListLookupNode {
	/// The source to look up in
	pub source: ResolvedShape,
	/// The row numbers to fetch
	pub row_numbers: Vec<u64>,
}

/// Range scan by row numbers: `filter rownum between X and Y`
#[derive(Debug, Clone)]
pub struct RowRangeScanNode {
	/// The source to scan
	pub source: ResolvedShape,
	/// Start of the range (inclusive)
	pub start: u64,
	/// End of the range (inclusive)
	pub end: u64,
}

/// APPEND statement physical plan node
#[derive(Debug, Clone)]
pub enum AppendPhysicalNode {
	IntoVariable {
		target: Fragment,
		source: AppendPhysicalSource,
	},
	Query {
		left: Box<QueryPlan>,
		right: Box<QueryPlan>,
	},
}

/// Source for an APPEND physical plan
#[derive(Debug, Clone)]
pub enum AppendPhysicalSource {
	Statement(Vec<PhysicalPlan>),
	Inline(InlineDataNode),
}

#[derive(Debug, Clone)]
pub struct ConditionalNode {
	pub condition: Expression,
	pub then_branch: Box<PhysicalPlan>,
	pub else_ifs: Vec<ElseIfBranch>,
	pub else_branch: Option<Box<PhysicalPlan>>,
}

#[derive(Debug, Clone)]
pub struct ElseIfBranch {
	pub condition: Expression,
	pub then_branch: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LoopPhysicalNode {
	pub body: Vec<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct WhilePhysicalNode {
	pub condition: Expression,
	pub body: Vec<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct ForPhysicalNode {
	pub variable_name: Fragment,
	pub iterable: Box<PhysicalPlan>,
	pub body: Vec<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct DefineFunctionNode {
	pub name: Fragment,
	pub parameters: Vec<FunctionParameter>,
	pub return_type: Option<TypeConstraint>,
	pub body: Vec<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct ReturnNode {
	pub value: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct CallFunctionNode {
	pub name: Fragment,
	pub arguments: Vec<Expression>,
	pub is_procedure_call: bool,
}

#[derive(Debug, Clone)]
pub struct DropNamespaceNode {
	pub namespace_name: Fragment,
	pub namespace_id: Option<NamespaceId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropTableNode {
	pub namespace_name: Fragment,
	pub table_name: Fragment,
	pub table_id: Option<TableId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropViewNode {
	pub namespace_name: Fragment,
	pub view_name: Fragment,
	pub view_id: Option<ViewId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropRingBufferNode {
	pub namespace_name: Fragment,
	pub ringbuffer_name: Fragment,
	pub ringbuffer_id: Option<RingBufferId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropDictionaryNode {
	pub namespace_name: Fragment,
	pub dictionary_name: Fragment,
	pub dictionary_id: Option<DictionaryId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropSumTypeNode {
	pub namespace_name: Fragment,
	pub sumtype_name: Fragment,
	pub sumtype_id: Option<SumTypeId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropSubscriptionNode {
	pub subscription_name: Fragment,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct DropSeriesNode {
	pub namespace_name: Fragment,
	pub series_name: Fragment,
	pub series_id: Option<SeriesId>,
	pub if_exists: bool,
	pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIdentityNode {
	pub name: Fragment,
}

#[derive(Debug, Clone)]
pub struct CreateRoleNode {
	pub name: Fragment,
}

#[derive(Debug, Clone)]
pub struct GrantNode {
	pub role: Fragment,
	pub user: Fragment,
}

#[derive(Debug, Clone)]
pub struct RevokeNode {
	pub role: Fragment,
	pub user: Fragment,
}

#[derive(Debug, Clone)]
pub struct DropIdentityNode {
	pub name: Fragment,
	pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropRoleNode {
	pub name: Fragment,
	pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateAuthenticationNode {
	pub user: Fragment,
	pub method: Fragment,
	pub config: collections::HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DropAuthenticationNode {
	pub user: Fragment,
	pub method: Fragment,
	pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreatePolicyNode {
	pub name: Option<Fragment>,
	pub target_type: String,
	pub scope_namespace: Option<Fragment>,
	pub scope_object: Option<Fragment>,
	pub operations: Vec<PolicyOperationNode>,
}

#[derive(Debug, Clone)]
pub struct PolicyOperationNode {
	pub operation: String,
	pub body_source: String,
}

#[derive(Debug, Clone)]
pub struct AlterPolicyNode {
	pub target_type: String,
	pub name: Fragment,
	pub enable: bool,
}

#[derive(Debug, Clone)]
pub struct DropPolicyNode {
	pub target_type: String,
	pub name: Fragment,
	pub if_exists: bool,
}
