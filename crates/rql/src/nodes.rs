// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::{
	ringbuffer::RingBufferColumnToCreate, subscription::SubscriptionColumnToCreate, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	common::{JoinType, WindowSize, WindowSlide, WindowType},
	interface::{
		catalog::{namespace::NamespaceDef, procedure::ProcedureParamDef},
		resolved::{
			ResolvedColumn, ResolvedDictionary, ResolvedFlow, ResolvedNamespace, ResolvedPrimitive,
			ResolvedRingBuffer, ResolvedSequence, ResolvedTable, ResolvedTableVirtual, ResolvedView,
		},
	},
	sort::{SortDirection, SortKey},
};
use reifydb_type::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, r#type::Type},
};

use crate::{
	expression::{AliasExpression, Expression, VariableExpression},
	query::QueryPlan,
};

/// Owned primary key definition for physical plan nodes (materialized from bump-allocated logical plan)
#[derive(Debug, Clone)]
pub struct PrimaryKeyDef {
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
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateFlow(CreateFlowNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSumType(CreateSumTypeNode),
	CreateSubscription(CreateSubscriptionNode),
	CreatePrimaryKey(CreatePrimaryKeyNode),
	CreatePolicy(CreatePolicyNode),
	CreateProcedure(CreateProcedureNode),
	// Alter
	AlterSequence(AlterSequenceNode),
	AlterFlow(AlterFlowNode),
	// Mutate
	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),
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
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	FlowScan(FlowScanNode),
	DictionaryScan(DictionaryScanNode),
	Generator(GeneratorNode),
	Window(WindowNode),
	// Auto-scalarization for 1x1 frames
	Scalarize(ScalarizeNode),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewNode {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateFlowNode {
	pub namespace: NamespaceDef,
	pub flow: Fragment,
	pub if_not_exists: bool,
	pub as_clause: Box<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewNode {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceNode {
	pub segments: Vec<Fragment>,
	pub if_not_exists: bool,
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
}

#[derive(Debug, Clone)]
pub struct CreateDictionaryNode {
	pub namespace: NamespaceDef,
	pub dictionary: Fragment,
	pub if_not_exists: bool,
	pub value_type: Type,
	pub id_type: Type,
}

#[derive(Debug, Clone)]
pub struct CreateSumTypeNode {
	pub namespace: NamespaceDef,
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

// Alter Flow types

#[derive(Debug, Clone)]
pub struct AlterFlowNode {
	pub flow: AlterFlowIdentifier,
	pub action: AlterFlowAction,
}

#[derive(Debug, Clone)]
pub struct AlterFlowIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
}

#[derive(Debug, Clone)]
pub enum AlterFlowAction {
	Rename {
		new_name: Fragment,
	},
	SetQuery {
		query: Box<QueryPlan>,
	},
	Pause,
	Resume,
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
	pub namespace: NamespaceDef,
	pub name: Fragment,
	pub params: Vec<ProcedureParamDef>,
	pub body_source: String,
}

// Create Policy node
#[derive(Debug, Clone)]
pub struct CreatePolicyNode {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub column: Fragment,
	pub policies: Vec<reifydb_core::interface::catalog::policy::ColumnPolicyKind>,
}

#[derive(Debug, Clone)]
pub enum LetValue {
	Expression(Expression),
	Statement(QueryPlan),
	EmptyFrame,
}

impl std::fmt::Display for LetValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Display for AssignValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
pub struct DeleteTableNode {
	pub input: Option<Box<QueryPlan>>,
	pub target: Option<ResolvedTable>,
}

#[derive(Debug, Clone)]
pub struct InsertTableNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedTable,
}

#[derive(Debug, Clone)]
pub struct InsertRingBufferNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct InsertDictionaryNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedDictionary,
}

#[derive(Debug, Clone)]
pub struct UpdateTableNode {
	pub input: Box<QueryPlan>,
	pub target: Option<ResolvedTable>,
}

#[derive(Debug, Clone)]
pub struct DeleteRingBufferNode {
	pub input: Option<Box<QueryPlan>>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct UpdateRingBufferNode {
	pub input: Box<QueryPlan>,
	pub target: ResolvedRingBuffer,
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
pub struct FlowScanNode {
	pub source: ResolvedFlow,
}

#[derive(Debug, Clone)]
pub struct DictionaryScanNode {
	pub source: ResolvedDictionary,
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
pub struct TakeNode {
	pub input: Box<QueryPlan>,
	pub take: usize,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub input: Option<Box<QueryPlan>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub min_events: usize,
	pub max_window_count: Option<usize>,
	pub max_window_age: Option<std::time::Duration>,
}

/// O(1) point lookup by row number: `filter rownum == N`
#[derive(Debug, Clone)]
pub struct RowPointLookupNode {
	/// The source to look up in (table, ring buffer, etc.)
	pub source: ResolvedPrimitive,
	/// The row number to fetch
	pub row_number: u64,
}

/// O(k) list lookup by row numbers: `filter rownum in [a, b, c]`
#[derive(Debug, Clone)]
pub struct RowListLookupNode {
	/// The source to look up in
	pub source: ResolvedPrimitive,
	/// The row numbers to fetch
	pub row_numbers: Vec<u64>,
}

/// Range scan by row numbers: `filter rownum between X and Y`
#[derive(Debug, Clone)]
pub struct RowRangeScanNode {
	/// The source to scan
	pub source: ResolvedPrimitive,
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

// --- Control flow and function nodes (owned, for PhysicalPlan enum) ---

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
}
