// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod alter;
pub mod create;
pub mod drop;
pub mod mutate;

use std::iter::once;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::{JoinType, WindowSize, WindowSlide, WindowType},
	error::diagnostic::catalog::{dictionary_not_found, ringbuffer_not_found, table_not_found},
	interface::{
		catalog::{
			column::{ColumnDef, ColumnIndex},
			id::{ColumnId, NamespaceId, TableId},
			namespace::NamespaceDef,
			table::TableDef,
		},
		resolved::{
			ResolvedColumn, ResolvedDictionary, ResolvedNamespace, ResolvedPrimitive, ResolvedRingBuffer,
			ResolvedSeries, ResolvedTable, ResolvedView,
		},
	},
	sort::SortKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	return_error,
	value::{constraint::TypeConstraint, r#type::Type},
};
use tracing::instrument;

use crate::{
	bump::{Bump, BumpBox},
	error::RqlError,
	expression::{ConstantExpression, Expression, Expression::Constant, VariableExpression},
	nodes::{
		self, AlterSequenceNode, CreateDictionaryNode, CreateNamespaceNode, CreateRingBufferNode,
		CreateSumTypeNode, CreateTableNode, DictionaryScanNode, EnvironmentNode, FlowScanNode, GeneratorNode,
		IndexScanNode, InlineDataNode, RingBufferScanNode, RowListLookupNode, RowPointLookupNode,
		RowRangeScanNode, SeriesScanNode, TableScanNode, TableVirtualScanNode, VariableNode, ViewScanNode,
	},
	plan::{
		logical,
		logical::{
			LogicalPlan,
			row_predicate::{RowPredicate, extract_row_predicate},
			series_predicate::extract_series_predicate,
		},
	},
};

// ============================================================================
// Bump-allocated PhysicalPlan types
// ============================================================================

/// Bump-allocated physical plan — the intermediate representation between
/// logical planning and instruction compilation. Uses `BumpBox`/`Vec` for
/// tree structure while keeping `Fragment` (Arc<str>) for identifiers
/// (already materialized from `BumpFragment` during physical compilation).
#[derive(Debug)]
pub enum PhysicalPlan<'bump> {
	// DDL
	CreateDeferredView(CreateDeferredViewNode<'bump>),
	CreateTransactionalView(CreateTransactionalViewNode<'bump>),
	CreateNamespace(CreateNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateFlow(CreateFlowNode<'bump>),
	CreateDictionary(CreateDictionaryNode),
	CreateSumType(CreateSumTypeNode),
	CreateSubscription(CreateSubscriptionNode<'bump>),
	CreatePrimaryKey(nodes::CreatePrimaryKeyNode),
	CreatePolicy(nodes::CreatePolicyNode),
	CreateProcedure(nodes::CreateProcedureNode),
	CreateEvent(nodes::CreateEventNode),

	CreateSeries(nodes::CreateSeriesNode),
	CreateTag(nodes::CreateTagNode),
	CreateMigration(nodes::CreateMigrationNode),
	Migrate(nodes::MigrateNode),
	RollbackMigration(nodes::RollbackMigrationNode),
	Dispatch(nodes::DispatchNode),
	// Drop
	DropNamespace(nodes::DropNamespaceNode),
	DropTable(nodes::DropTableNode),
	DropView(nodes::DropViewNode),
	DropRingBuffer(nodes::DropRingBufferNode),
	DropDictionary(nodes::DropDictionaryNode),
	DropSumType(nodes::DropSumTypeNode),
	DropFlow(nodes::DropFlowNode),
	DropSubscription(nodes::DropSubscriptionNode),
	DropSeries(nodes::DropSeriesNode),
	// Alter
	AlterSequence(AlterSequenceNode),
	AlterFlow(AlterFlowNode<'bump>),
	AlterTable(AlterTableNode<'bump>),
	// Mutate
	Delete(DeleteTableNode<'bump>),
	DeleteRingBuffer(DeleteRingBufferNode<'bump>),
	DeleteSeries(DeleteSeriesNode<'bump>),
	InsertTable(InsertTableNode<'bump>),
	InsertRingBuffer(InsertRingBufferNode<'bump>),
	InsertDictionary(InsertDictionaryNode<'bump>),
	InsertSeries(InsertSeriesNode<'bump>),
	Update(UpdateTableNode<'bump>),
	UpdateRingBuffer(UpdateRingBufferNode<'bump>),
	UpdateSeries(UpdateSeriesNode<'bump>),
	// Variable assignment
	Declare(DeclareNode<'bump>),
	Assign(AssignNode<'bump>),
	Append(AppendPhysicalNode<'bump>),
	// Variable resolution
	Variable(VariableNode),
	Environment(EnvironmentNode),
	// Control flow
	Conditional(ConditionalNode<'bump>),
	Loop(LoopNode<'bump>),
	While(WhileNode<'bump>),
	For(ForNode<'bump>),
	Break,
	Continue,
	// User-defined functions
	DefineFunction(DefineFunctionNode<'bump>),
	Return(ReturnNode),
	CallFunction(CallFunctionNode),
	// Closures
	DefineClosure(DefineClosureNode<'bump>),
	// Query
	Aggregate(AggregateNode<'bump>),
	Assert(AssertNode<'bump>),
	Distinct(DistinctNode<'bump>),
	Filter(FilterNode<'bump>),
	IndexScan(IndexScanNode),
	RowPointLookup(RowPointLookupNode),
	RowListLookup(RowListLookupNode),
	RowRangeScan(RowRangeScanNode),
	JoinInner(JoinInnerNode<'bump>),
	JoinLeft(JoinLeftNode<'bump>),
	JoinNatural(JoinNaturalNode<'bump>),
	Take(TakeNode<'bump>),
	Sort(SortNode<'bump>),
	Map(MapNode<'bump>),
	Extend(ExtendNode<'bump>),
	Patch(PatchNode<'bump>),
	Apply(ApplyNode<'bump>),
	InlineData(InlineDataNode),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	FlowScan(FlowScanNode),
	DictionaryScan(DictionaryScanNode),
	SeriesScan(SeriesScanNode),
	Generator(GeneratorNode),
	Window(WindowNode<'bump>),
	Scalarize(ScalarizeNode<'bump>),
	// Auth/Permissions
	CreateUser(nodes::CreateUserNode),
	CreateRole(nodes::CreateRoleNode),
	Grant(nodes::GrantNode),
	Revoke(nodes::RevokeNode),
	DropUser(nodes::DropUserNode),
	DropRole(nodes::DropRoleNode),
	CreateSecurityPolicy(nodes::CreateSecurityPolicyNode),
	AlterSecurityPolicy(nodes::AlterSecurityPolicyNode),
	DropSecurityPolicy(nodes::DropSecurityPolicyNode),
}

// --- Nodes with recursive children (bump-allocated) ---

#[derive(Debug)]
pub struct CreateDeferredViewNode<'bump> {
	pub namespace: NamespaceDef,
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<reifydb_catalog::catalog::view::ViewColumnToCreate>,
	pub as_clause: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct CreateTransactionalViewNode<'bump> {
	pub namespace: NamespaceDef,
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<reifydb_catalog::catalog::view::ViewColumnToCreate>,
	pub as_clause: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct CreateFlowNode<'bump> {
	pub namespace: NamespaceDef,
	pub flow: Fragment,
	pub if_not_exists: bool,
	pub as_clause: BumpBox<'bump, PhysicalPlan<'bump>>,
}

#[derive(Debug)]
pub struct CreateSubscriptionNode<'bump> {
	pub columns: Vec<reifydb_catalog::catalog::subscription::SubscriptionColumnToCreate>,
	pub as_clause: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct AlterFlowNode<'bump> {
	pub flow: nodes::AlterFlowIdentifier,
	pub action: AlterFlowAction<'bump>,
}

#[derive(Debug)]
pub struct AlterTableNode<'bump> {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub action: AlterTableAction,
	pub _phantom: std::marker::PhantomData<&'bump ()>,
}

#[derive(Debug)]
pub enum AlterTableAction {
	AddColumn {
		column: reifydb_catalog::catalog::table::TableColumnToCreate,
	},
	DropColumn {
		column: Fragment,
	},
	RenameColumn {
		old_name: Fragment,
		new_name: Fragment,
	},
}

#[derive(Debug)]
pub enum AlterFlowAction<'bump> {
	Rename {
		new_name: Fragment,
	},
	SetQuery {
		query: BumpBox<'bump, PhysicalPlan<'bump>>,
	},
	Pause,
	Resume,
}

#[derive(Debug)]
pub struct DeleteTableNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub target: Option<ResolvedTable>,
}

#[derive(Debug)]
pub struct DeleteRingBufferNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug)]
pub struct InsertTableNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedTable,
}

#[derive(Debug)]
pub struct InsertRingBufferNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug)]
pub struct InsertDictionaryNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedDictionary,
}

#[derive(Debug)]
pub struct InsertSeriesNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedSeries,
}

#[derive(Debug)]
pub struct DeleteSeriesNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub target: ResolvedSeries,
}

#[derive(Debug)]
pub struct UpdateTableNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: Option<ResolvedTable>,
}

#[derive(Debug)]
pub struct UpdateRingBufferNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug)]
pub struct UpdateSeriesNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub target: ResolvedSeries,
}

#[derive(Debug)]
pub enum LetValue<'bump> {
	Expression(Expression),
	Statement(BumpBox<'bump, PhysicalPlan<'bump>>),
	EmptyFrame,
}

impl std::fmt::Display for LetValue<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Display for AssignValue<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
	pub then_branch: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub else_ifs: Vec<ElseIfBranch<'bump>>,
	pub else_branch: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
}

#[derive(Debug)]
pub struct ElseIfBranch<'bump> {
	pub condition: Expression,
	pub then_branch: BumpBox<'bump, PhysicalPlan<'bump>>,
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
pub struct ReturnNode {
	pub value: Option<Expression>,
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
}

#[derive(Debug)]
pub struct DistinctNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub columns: Vec<ResolvedColumn>,
}

#[derive(Debug)]
pub struct AssertNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub conditions: Vec<Expression>,
	pub message: Option<String>,
}

#[derive(Debug)]
pub struct FilterNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug)]
pub struct JoinInnerNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug)]
pub struct JoinLeftNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug)]
pub struct JoinNaturalNode<'bump> {
	pub left: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub right: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub join_type: JoinType,
	pub alias: Option<Fragment>,
}

#[derive(Debug)]
pub struct TakeNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub take: usize,
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
	pub expressions: Vec<Expression>,
}

#[derive(Debug)]
pub struct WindowNode<'bump> {
	pub input: Option<BumpBox<'bump, PhysicalPlan<'bump>>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub min_events: usize,
	pub max_window_count: Option<usize>,
	pub max_window_age: Option<std::time::Duration>,
}

#[derive(Debug)]
pub struct ScalarizeNode<'bump> {
	pub input: BumpBox<'bump, PhysicalPlan<'bump>>,
	pub fragment: Fragment,
}

// ============================================================================
// Compiler
// ============================================================================

pub(crate) struct Compiler<'bump> {
	pub catalog: Catalog,
	pub interner: crate::bump::FragmentInterner,
	pub bump: &'bump Bump,
}

#[instrument(name = "rql::compile::physical", level = "trace", skip(bump, catalog, rx, logical))]
pub fn compile_physical<'b>(
	bump: &'b Bump,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
	logical: impl IntoIterator<Item = LogicalPlan<'b>>,
) -> crate::Result<Option<PhysicalPlan<'b>>> {
	Compiler {
		catalog: catalog.clone(),
		interner: crate::bump::FragmentInterner::new(),
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
	) -> crate::Result<Option<PhysicalPlan<'bump>>> {
		let mut stack: Vec<PhysicalPlan<'bump>> = Vec::new();
		for plan in logical {
			match plan {
				LogicalPlan::Aggregate(aggregate) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Aggregate(AggregateNode {
						by: aggregate.by,
						map: aggregate.map,
						input: self.bump_box(input),
					}));
				}

				LogicalPlan::CreateNamespace(create) => {
					stack.push(self.compile_create_namespace(rx, create)?);
				}

				LogicalPlan::CreateTable(create) => {
					stack.push(self.compile_create_table(rx, create)?);
				}

				LogicalPlan::CreateRingBuffer(create) => {
					stack.push(self.compile_create_ringbuffer(rx, create)?);
				}

				LogicalPlan::CreateFlow(create) => {
					stack.push(self.compile_create_flow(rx, create)?);
				}

				LogicalPlan::CreateDeferredView(create) => {
					stack.push(self.compile_create_deferred(rx, create)?);
				}

				LogicalPlan::CreateTransactionalView(create) => {
					stack.push(self.compile_create_transactional(rx, create)?);
				}

				LogicalPlan::CreateDictionary(create) => {
					stack.push(self.compile_create_dictionary(rx, create)?);
				}

				LogicalPlan::CreateSumType(create) => {
					stack.push(self.compile_create_sumtype(rx, create)?);
				}

				LogicalPlan::CreateSubscription(create) => {
					stack.push(self.compile_create_subscription(rx, create)?);
				}

				LogicalPlan::AlterSequence(alter) => {
					stack.push(self.compile_alter_sequence(rx, alter)?);
				}

				LogicalPlan::CreatePrimaryKey(create) => {
					stack.push(self.compile_create_primary_key(rx, create)?);
				}

				LogicalPlan::CreatePolicy(create) => {
					stack.push(self.compile_create_policy(rx, create)?);
				}

				LogicalPlan::CreateProcedure(create) => {
					stack.push(self.compile_create_procedure(rx, create)?);
				}

				LogicalPlan::CreateSeries(create) => {
					stack.push(self.compile_create_series(rx, create)?);
				}

				LogicalPlan::CreateEvent(create) => {
					stack.push(self.compile_create_event(rx, create)?);
				}

				LogicalPlan::CreateTag(create) => {
					stack.push(self.compile_create_tag(rx, create)?);
				}

				LogicalPlan::CreateMigration(create) => {
					stack.push(PhysicalPlan::CreateMigration(nodes::CreateMigrationNode {
						name: create.name,
						body_source: create.body_source,
						rollback_body_source: create.rollback_body_source,
					}));
				}

				LogicalPlan::Migrate(node) => {
					stack.push(PhysicalPlan::Migrate(nodes::MigrateNode {
						target: node.target,
					}));
				}

				LogicalPlan::RollbackMigration(node) => {
					stack.push(PhysicalPlan::RollbackMigration(nodes::RollbackMigrationNode {
						target: node.target,
					}));
				}

				LogicalPlan::Dispatch(dispatch) => {
					stack.push(self.compile_dispatch(rx, dispatch)?);
				}

				LogicalPlan::AlterFlow(alter) => {
					stack.push(self.compile_alter_flow(rx, alter)?);
				}
				LogicalPlan::AlterTable(alter) => {
					stack.push(self.compile_alter_table(rx, alter)?);
				}

				// Drop
				LogicalPlan::DropNamespace(drop) => {
					stack.push(self.compile_drop_namespace(rx, drop)?);
				}
				LogicalPlan::DropTable(drop) => {
					stack.push(self.compile_drop_table(rx, drop)?);
				}
				LogicalPlan::DropView(drop) => {
					stack.push(self.compile_drop_view(rx, drop)?);
				}
				LogicalPlan::DropRingBuffer(drop) => {
					stack.push(self.compile_drop_ringbuffer(rx, drop)?);
				}
				LogicalPlan::DropDictionary(drop) => {
					stack.push(self.compile_drop_dictionary(rx, drop)?);
				}
				LogicalPlan::DropSumType(drop) => {
					stack.push(self.compile_drop_sumtype(rx, drop)?);
				}
				LogicalPlan::DropFlow(drop) => {
					stack.push(self.compile_drop_flow(rx, drop)?);
				}
				LogicalPlan::DropSubscription(drop) => {
					stack.push(self.compile_drop_subscription(rx, drop)?);
				}
				LogicalPlan::DropSeries(drop) => {
					stack.push(self.compile_drop_series(rx, drop)?);
				}

				// Auth/Permissions - pass through logical to physical directly
				LogicalPlan::CreateUser(node) => {
					stack.push(PhysicalPlan::CreateUser(nodes::CreateUserNode {
						name: self.interner.intern_fragment(&node.name),
						password: self.interner.intern_fragment(&node.password),
					}));
				}
				LogicalPlan::CreateRole(node) => {
					stack.push(PhysicalPlan::CreateRole(nodes::CreateRoleNode {
						name: self.interner.intern_fragment(&node.name),
					}));
				}
				LogicalPlan::Grant(node) => {
					stack.push(PhysicalPlan::Grant(nodes::GrantNode {
						role: self.interner.intern_fragment(&node.role),
						user: self.interner.intern_fragment(&node.user),
					}));
				}
				LogicalPlan::Revoke(node) => {
					stack.push(PhysicalPlan::Revoke(nodes::RevokeNode {
						role: self.interner.intern_fragment(&node.role),
						user: self.interner.intern_fragment(&node.user),
					}));
				}
				LogicalPlan::DropUser(node) => {
					stack.push(PhysicalPlan::DropUser(nodes::DropUserNode {
						name: self.interner.intern_fragment(&node.name),
						if_exists: node.if_exists,
					}));
				}
				LogicalPlan::DropRole(node) => {
					stack.push(PhysicalPlan::DropRole(nodes::DropRoleNode {
						name: self.interner.intern_fragment(&node.name),
						if_exists: node.if_exists,
					}));
				}
				LogicalPlan::CreateSecurityPolicy(node) => {
					let name = node.name.map(|n| self.interner.intern_fragment(&n));
					let target_type = format!("{:?}", node.target_type);
					let (scope_namespace, scope_object) = match &node.scope {
						crate::ast::ast::AstPolicyScope::Specific(segments) => {
							if segments.len() >= 2 {
								(
									Some(self
										.interner
										.intern_fragment(&segments[0])),
									Some(self.interner.intern_fragment(
										&segments[segments.len() - 1],
									)),
								)
							} else if segments.len() == 1 {
								(
									Some(self
										.interner
										.intern_fragment(&segments[0])),
									None,
								)
							} else {
								(None, None)
							}
						}
						crate::ast::ast::AstPolicyScope::NamespaceWide(ns) => {
							(Some(self.interner.intern_fragment(ns)), None)
						}
						crate::ast::ast::AstPolicyScope::Global => (None, None),
					};
					let operations = node
						.operations
						.iter()
						.map(|op| {
							nodes::SecurityPolicyOperationNode {
								operation: op.operation.text().to_string(),
								body_source: String::new(), /* Body source captured
								                             * separately */
							}
						})
						.collect();
					stack.push(PhysicalPlan::CreateSecurityPolicy(
						nodes::CreateSecurityPolicyNode {
							name,
							target_type,
							scope_namespace,
							scope_object,
							operations,
						},
					));
				}
				LogicalPlan::AlterSecurityPolicy(node) => {
					let enable = node.action == crate::ast::ast::AstAlterPolicyAction::Enable;
					stack.push(PhysicalPlan::AlterSecurityPolicy(nodes::AlterSecurityPolicyNode {
						target_type: format!("{:?}", node.target_type),
						name: self.interner.intern_fragment(&node.name),
						enable,
					}));
				}
				LogicalPlan::DropSecurityPolicy(node) => {
					stack.push(PhysicalPlan::DropSecurityPolicy(nodes::DropSecurityPolicyNode {
						target_type: format!("{:?}", node.target_type),
						name: self.interner.intern_fragment(&node.name),
						if_exists: node.if_exists,
					}));
				}

				LogicalPlan::Assert(assert_node) => {
					let input = stack.pop().map(|p| self.bump_box(p));
					stack.push(PhysicalPlan::Assert(AssertNode {
						conditions: vec![assert_node.condition],
						message: assert_node.message,
						input,
					}));
				}

				LogicalPlan::Filter(filter) => {
					let input = stack.pop().unwrap(); // FIXME

					// Try to optimize rownum predicates for O(1)/O(k) access
					if let Some(predicate) = extract_row_predicate(&filter.condition) {
						// Check if input is a scan node we can optimize
						let source = match &input {
							PhysicalPlan::TableScan(scan) => {
								Some(ResolvedPrimitive::Table(scan.source.clone()))
							}
							PhysicalPlan::ViewScan(scan) => {
								Some(ResolvedPrimitive::View(scan.source.clone()))
							}
							PhysicalPlan::RingBufferScan(scan) => {
								Some(ResolvedPrimitive::RingBuffer(scan.source.clone()))
							}
							_ => None,
						};

						if let Some(source) = source {
							match predicate {
								RowPredicate::Point(row_number) => {
									stack.push(PhysicalPlan::RowPointLookup(
										RowPointLookupNode {
											source,
											row_number,
										},
									));
									continue;
								}
								RowPredicate::List(row_numbers) => {
									stack.push(PhysicalPlan::RowListLookup(
										RowListLookupNode {
											source,
											row_numbers,
										},
									));
									continue;
								}
								RowPredicate::Range {
									start,
									end,
								} => {
									stack.push(PhysicalPlan::RowRangeScan(
										RowRangeScanNode {
											source,
											start,
											end,
										},
									));
									continue;
								}
							}
						}
					}

					// Try to push down timestamp/tag predicates into SeriesScan
					if let PhysicalPlan::SeriesScan(ref scan) = input {
						if let Some(sp) = extract_series_predicate(&filter.condition) {
							let rewritten = PhysicalPlan::SeriesScan(SeriesScanNode {
								source: scan.source.clone(),
								time_range_start: sp
									.time_start
									.or(scan.time_range_start),
								time_range_end: sp.time_end.or(scan.time_range_end),
								variant_tag: sp.variant_tag.or(scan.variant_tag),
							});
							if sp.remaining.is_empty() {
								stack.push(rewritten);
							} else {
								stack.push(PhysicalPlan::Filter(FilterNode {
									conditions: sp.remaining,
									input: self.bump_box(rewritten),
								}));
							}
							continue;
						}
					}

					// Default: generic filter
					stack.push(PhysicalPlan::Filter(FilterNode {
						conditions: vec![filter.condition],
						input: self.bump_box(input),
					}));
				}

				LogicalPlan::InlineData(inline) => {
					stack.push(PhysicalPlan::InlineData(InlineDataNode {
						rows: inline.rows,
					}));
				}

				LogicalPlan::Generator(generator) => {
					stack.push(PhysicalPlan::Generator(GeneratorNode {
						name: self.interner.intern_fragment(&generator.name),
						expressions: generator.expressions,
					}));
				}

				LogicalPlan::DeleteTable(delete) => {
					let input = if let Some(delete_input) = delete.input {
						let sub_plan = self
							.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(delete_input)),
							)?
							.expect("Delete input must produce a plan");
						Some(self.bump_box(sub_plan))
					} else {
						stack.pop().map(|i| self.bump_box(i))
					};

					let target = if let Some(table_id) = delete.target {
						let namespace_name = if table_id.namespace.is_empty() {
							"default".to_string()
						} else {
							table_id.namespace
								.iter()
								.map(|n| n.text())
								.collect::<Vec<_>>()
								.join(".")
						};
						let namespace_def = self
							.catalog
							.find_namespace_by_name(rx, &namespace_name)?
							.unwrap();
						let Some(table_def) = self.catalog.find_table_by_name(
							rx,
							namespace_def.id,
							table_id.name.text(),
						)?
						else {
							return_error!(table_not_found(
								self.interner.intern_fragment(&table_id.name),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id = if let Some(n) = table_id.namespace.first() {
							let interned = self.interner.intern_fragment(n);
							interned.with_text(&namespace_def.name)
						} else {
							Fragment::internal(namespace_def.name.clone())
						};
						let resolved_namespace =
							ResolvedNamespace::new(namespace_id, namespace_def);
						Some(ResolvedTable::new(
							self.interner.intern_fragment(&table_id.name),
							resolved_namespace,
							table_def,
						))
					} else {
						None
					};

					stack.push(PhysicalPlan::Delete(DeleteTableNode {
						input,
						target,
					}))
				}

				LogicalPlan::DeleteRingBuffer(delete) => {
					let input = if let Some(delete_input) = delete.input {
						let sub_plan = self
							.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(delete_input)),
							)?
							.expect("Delete input must produce a plan");
						Some(self.bump_box(sub_plan))
					} else {
						stack.pop().map(|i| self.bump_box(i))
					};

					let ringbuffer_id = delete.target;
					let namespace_name = if ringbuffer_id.namespace.is_empty() {
						"default".to_string()
					} else {
						ringbuffer_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(ringbuffer_def) = self.catalog.find_ringbuffer_by_name(
						rx,
						namespace_def.id,
						ringbuffer_id.name.text(),
					)?
					else {
						return_error!(ringbuffer_not_found(
							self.interner.intern_fragment(&ringbuffer_id.name),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = if let Some(n) = ringbuffer_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						self.interner.intern_fragment(&ringbuffer_id.name),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
						input,
						target,
					}))
				}

				LogicalPlan::InsertTable(insert) => {
					let input = self
						.compile(rx, once(crate::bump::BumpBox::into_inner(insert.source)))?
						.expect("Insert source must produce a plan");

					let table = insert.target;
					let namespace_name = if table.namespace.is_empty() {
						"default".to_string()
					} else {
						table.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(table_def) = self.catalog.find_table_by_name(
						rx,
						namespace_def.id,
						table.name.text(),
					)?
					else {
						return_error!(table_not_found(
							self.interner.intern_fragment(&table.name),
							&namespace_def.name,
							table.name.text()
						));
					};

					let namespace_id = if let Some(n) = table.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedTable::new(
						self.interner.intern_fragment(&table.name),
						resolved_namespace,
						table_def,
					);

					stack.push(PhysicalPlan::InsertTable(InsertTableNode {
						input: self.bump_box(input),
						target,
					}))
				}

				LogicalPlan::InsertRingBuffer(insert_rb) => {
					let input = self
						.compile(rx, once(crate::bump::BumpBox::into_inner(insert_rb.source)))?
						.expect("Insert source must produce a plan");

					let ringbuffer_id = insert_rb.target;
					let namespace_name = if ringbuffer_id.namespace.is_empty() {
						"default".to_string()
					} else {
						ringbuffer_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(ringbuffer_def) = self.catalog.find_ringbuffer_by_name(
						rx,
						namespace_def.id,
						ringbuffer_id.name.text(),
					)?
					else {
						return_error!(ringbuffer_not_found(
							self.interner.intern_fragment(&ringbuffer_id.name),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = if let Some(n) = ringbuffer_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						self.interner.intern_fragment(&ringbuffer_id.name),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::InsertRingBuffer(InsertRingBufferNode {
						input: self.bump_box(input),
						target,
					}))
				}

				LogicalPlan::InsertDictionary(insert_dict) => {
					let input = self
						.compile(
							rx,
							once(crate::bump::BumpBox::into_inner(insert_dict.source)),
						)?
						.expect("Insert source must produce a plan");

					let dictionary_id = insert_dict.target;
					let namespace_name = if dictionary_id.namespace.is_empty() {
						"default".to_string()
					} else {
						dictionary_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(dictionary_def) = self.catalog.find_dictionary_by_name(
						rx,
						namespace_def.id,
						dictionary_id.name.text(),
					)?
					else {
						return_error!(dictionary_not_found(
							self.interner.intern_fragment(&dictionary_id.name),
							&namespace_def.name,
							dictionary_id.name.text()
						));
					};

					let namespace_id = if let Some(n) = dictionary_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedDictionary::new(
						self.interner.intern_fragment(&dictionary_id.name),
						resolved_namespace,
						dictionary_def,
					);

					stack.push(PhysicalPlan::InsertDictionary(InsertDictionaryNode {
						input: self.bump_box(input),
						target,
					}))
				}

				LogicalPlan::InsertSeries(insert_series) => {
					let input = self
						.compile(
							rx,
							once(crate::bump::BumpBox::into_inner(insert_series.source)),
						)?
						.expect("Insert source must produce a plan");

					let series_id = insert_series.target;
					let namespace_name = if series_id.namespace.is_empty() {
						"default".to_string()
					} else {
						series_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(series_def) = self.catalog.find_series_by_name(
						rx,
						namespace_def.id,
						series_id.name.text(),
					)?
					else {
						return_error!(
							reifydb_core::error::diagnostic::catalog::series_not_found(
								self.interner.intern_fragment(&series_id.name),
								&namespace_def.name,
								series_id.name.text()
							)
						);
					};

					let namespace_id = if let Some(n) = series_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedSeries::new(
						self.interner.intern_fragment(&series_id.name),
						resolved_namespace,
						series_def,
					);

					stack.push(PhysicalPlan::InsertSeries(InsertSeriesNode {
						input: self.bump_box(input),
						target,
					}))
				}

				LogicalPlan::DeleteSeries(delete_series) => {
					let input = if let Some(delete_input) = delete_series.input {
						let sub_plan = self
							.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(delete_input)),
							)?
							.expect("Delete input must produce a plan");
						Some(self.bump_box(sub_plan))
					} else {
						stack.pop().map(|i| self.bump_box(i))
					};

					let series_id = delete_series.target;
					let namespace_name = if series_id.namespace.is_empty() {
						"default".to_string()
					} else {
						series_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(series_def) = self.catalog.find_series_by_name(
						rx,
						namespace_def.id,
						series_id.name.text(),
					)?
					else {
						return_error!(
							reifydb_core::error::diagnostic::catalog::series_not_found(
								self.interner.intern_fragment(&series_id.name),
								&namespace_def.name,
								series_id.name.text()
							)
						);
					};

					let namespace_id = if let Some(n) = series_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedSeries::new(
						self.interner.intern_fragment(&series_id.name),
						resolved_namespace,
						series_def,
					);

					stack.push(PhysicalPlan::DeleteSeries(DeleteSeriesNode {
						input,
						target,
					}))
				}

				LogicalPlan::Update(update) => {
					let input = if let Some(update_input) = update.input {
						let sub_plan = self
							.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(update_input)),
							)?
							.expect("Update input must produce a plan");
						self.bump_box(sub_plan)
					} else {
						self.bump_box(stack.pop().expect("Update requires input"))
					};

					let target = if let Some(table_id) = update.target {
						let namespace_name = if table_id.namespace.is_empty() {
							"default".to_string()
						} else {
							table_id.namespace
								.iter()
								.map(|n| n.text())
								.collect::<Vec<_>>()
								.join(".")
						};
						let namespace_def = self
							.catalog
							.find_namespace_by_name(rx, &namespace_name)?
							.unwrap();
						let Some(table_def) = self.catalog.find_table_by_name(
							rx,
							namespace_def.id,
							table_id.name.text(),
						)?
						else {
							return_error!(table_not_found(
								self.interner.intern_fragment(&table_id.name),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id = if let Some(n) = table_id.namespace.first() {
							let interned = self.interner.intern_fragment(n);
							interned.with_text(&namespace_def.name)
						} else {
							Fragment::internal(namespace_def.name.clone())
						};
						let resolved_namespace =
							ResolvedNamespace::new(namespace_id, namespace_def);
						Some(ResolvedTable::new(
							self.interner.intern_fragment(&table_id.name),
							resolved_namespace,
							table_def,
						))
					} else {
						None
					};

					stack.push(PhysicalPlan::Update(UpdateTableNode {
						input,
						target,
					}))
				}

				LogicalPlan::UpdateRingBuffer(update_rb) => {
					let input = if let Some(update_input) = update_rb.input {
						let sub_plan = self
							.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(update_input)),
							)?
							.expect("UpdateRingBuffer input must produce a plan");
						self.bump_box(sub_plan)
					} else {
						self.bump_box(stack.pop().expect("UpdateRingBuffer requires input"))
					};

					let ringbuffer_id = update_rb.target;
					let namespace_name = if ringbuffer_id.namespace.is_empty() {
						"default".to_string()
					} else {
						ringbuffer_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(ringbuffer_def) = self.catalog.find_ringbuffer_by_name(
						rx,
						namespace_def.id,
						ringbuffer_id.name.text(),
					)?
					else {
						return_error!(ringbuffer_not_found(
							self.interner.intern_fragment(&ringbuffer_id.name),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = if let Some(n) = ringbuffer_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						self.interner.intern_fragment(&ringbuffer_id.name),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
						input,
						target,
					}))
				}

				LogicalPlan::UpdateSeries(update_series) => {
					let input = if let Some(update_input) = update_series.input {
						let sub_plan = self
							.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(update_input)),
							)?
							.expect("UpdateSeries input must produce a plan");
						self.bump_box(sub_plan)
					} else {
						self.bump_box(stack.pop().expect("UpdateSeries requires input"))
					};

					let series_id = update_series.target;
					let namespace_name = if series_id.namespace.is_empty() {
						"default".to_string()
					} else {
						series_id
							.namespace
							.iter()
							.map(|n| n.text())
							.collect::<Vec<_>>()
							.join(".")
					};
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, &namespace_name)?.unwrap();
					let Some(series_def) = self.catalog.find_series_by_name(
						rx,
						namespace_def.id,
						series_id.name.text(),
					)?
					else {
						return_error!(
							reifydb_core::error::diagnostic::catalog::series_not_found(
								self.interner.intern_fragment(&series_id.name),
								&namespace_def.name,
								series_id.name.text()
							)
						);
					};

					let namespace_id = if let Some(n) = series_id.namespace.first() {
						let interned = self.interner.intern_fragment(n);
						interned.with_text(&namespace_def.name)
					} else {
						Fragment::internal(namespace_def.name.clone())
					};
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedSeries::new(
						self.interner.intern_fragment(&series_id.name),
						resolved_namespace,
						series_def,
					);

					stack.push(PhysicalPlan::UpdateSeries(UpdateSeriesNode {
						input,
						target,
					}))
				}

				LogicalPlan::JoinInner(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = self.compile(rx, join.with)?.unwrap();
					let alias = join.alias.map(|a| self.interner.intern_fragment(&a));
					stack.push(PhysicalPlan::JoinInner(JoinInnerNode {
						left: self.bump_box(left),
						right: self.bump_box(right),
						on: join.on,
						alias,
					}));
				}

				LogicalPlan::JoinLeft(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = self.compile(rx, join.with)?.unwrap();
					let alias = join.alias.map(|a| self.interner.intern_fragment(&a));
					stack.push(PhysicalPlan::JoinLeft(JoinLeftNode {
						left: self.bump_box(left),
						right: self.bump_box(right),
						on: join.on,
						alias,
					}));
				}

				LogicalPlan::JoinNatural(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = self.compile(rx, join.with)?.unwrap();
					let alias = join.alias.map(|a| self.interner.intern_fragment(&a));
					stack.push(PhysicalPlan::JoinNatural(JoinNaturalNode {
						left: self.bump_box(left),
						right: self.bump_box(right),
						join_type: join.join_type,
						alias,
					}));
				}

				LogicalPlan::Order(order) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Sort(SortNode {
						by: order.by,
						input: self.bump_box(input),
					}));
				}

				LogicalPlan::Distinct(distinct) => {
					let input = stack.pop().unwrap(); // FIXME

					let mut resolved_columns = Vec::with_capacity(distinct.columns.len());
					for col in distinct.columns {
						let namespace = ResolvedNamespace::new(
							Fragment::internal("_context"),
							NamespaceDef {
								id: NamespaceId(1),
								name: "_context".to_string(),
								parent_id: NamespaceId::ROOT,
							},
						);

						let table_def = TableDef {
							id: TableId(1),
							namespace: NamespaceId(1),
							name: "_context".to_string(),
							columns: vec![],
							primary_key: None,
						};

						let resolved_table = ResolvedTable::new(
							Fragment::internal("_context"),
							namespace,
							table_def,
						);

						let resolved_source = ResolvedPrimitive::Table(resolved_table);

						let column_def = ColumnDef {
							id: ColumnId(1),
							name: col.name.text().to_string(),
							constraint: TypeConstraint::unconstrained(Type::Utf8),
							policies: vec![],
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

					stack.push(PhysicalPlan::Distinct(DistinctNode {
						columns: resolved_columns,
						input: self.bump_box(input),
					}));
				}

				LogicalPlan::Map(map) => {
					let input = stack.pop().map(|p| self.bump_box(p));
					stack.push(PhysicalPlan::Map(MapNode {
						map: map.map,
						input,
					}));
				}

				LogicalPlan::Extend(extend) => {
					let input = stack.pop().map(|p| self.bump_box(p));
					stack.push(PhysicalPlan::Extend(ExtendNode {
						extend: extend.extend,
						input,
					}));
				}

				LogicalPlan::Patch(patch) => {
					let input = stack.pop().map(|p| self.bump_box(p));
					stack.push(PhysicalPlan::Patch(PatchNode {
						assignments: patch.assignments,
						input,
					}));
				}

				LogicalPlan::Apply(apply) => {
					let input = stack.pop().map(|p| self.bump_box(p));
					stack.push(PhysicalPlan::Apply(ApplyNode {
						operator: self.interner.intern_fragment(&apply.operator),
						expressions: apply.arguments,
						input,
					}));
				}

				LogicalPlan::PrimitiveScan(scan) => match &scan.source {
					ResolvedPrimitive::Table(resolved_table) => {
						if let Some(index) = &scan.index {
							stack.push(PhysicalPlan::IndexScan(IndexScanNode {
								source: resolved_table.clone(),
								index_name: index.identifier().text().to_string(),
							}));
						} else {
							stack.push(PhysicalPlan::TableScan(TableScanNode {
								source: resolved_table.clone(),
							}));
						}
					}
					ResolvedPrimitive::View(resolved_view) => {
						if scan.index.is_some() {
							unimplemented!("views do not support indexes yet");
						}
						stack.push(PhysicalPlan::ViewScan(ViewScanNode {
							source: resolved_view.clone(),
						}));
					}
					ResolvedPrimitive::DeferredView(resolved_view) => {
						if scan.index.is_some() {
							unimplemented!("views do not support indexes yet");
						}
						let view = ResolvedView::new(
							resolved_view.identifier().clone(),
							resolved_view.namespace().clone(),
							resolved_view.def().clone(),
						);
						stack.push(PhysicalPlan::ViewScan(ViewScanNode {
							source: view,
						}));
					}
					ResolvedPrimitive::TransactionalView(resolved_view) => {
						if scan.index.is_some() {
							unimplemented!("views do not support indexes yet");
						}
						let view = ResolvedView::new(
							resolved_view.identifier().clone(),
							resolved_view.namespace().clone(),
							resolved_view.def().clone(),
						);
						stack.push(PhysicalPlan::ViewScan(ViewScanNode {
							source: view,
						}));
					}

					ResolvedPrimitive::TableVirtual(resolved_virtual) => {
						if scan.index.is_some() {
							unimplemented!("virtual tables do not support indexes yet");
						}
						stack.push(PhysicalPlan::TableVirtualScan(TableVirtualScanNode {
							source: resolved_virtual.clone(),
							pushdown_context: None,
						}));
					}
					ResolvedPrimitive::RingBuffer(resolved_ringbuffer) => {
						if scan.index.is_some() {
							unimplemented!("ring buffers do not support indexes yet");
						}
						stack.push(PhysicalPlan::RingBufferScan(RingBufferScanNode {
							source: resolved_ringbuffer.clone(),
						}));
					}
					ResolvedPrimitive::Flow(resolved_flow) => {
						if scan.index.is_some() {
							unimplemented!("flows do not support indexes yet");
						}
						stack.push(PhysicalPlan::FlowScan(FlowScanNode {
							source: resolved_flow.clone(),
						}));
					}
					ResolvedPrimitive::Dictionary(resolved_dictionary) => {
						if scan.index.is_some() {
							unimplemented!("dictionaries do not support indexes");
						}
						stack.push(PhysicalPlan::DictionaryScan(DictionaryScanNode {
							source: resolved_dictionary.clone(),
						}));
					}
					ResolvedPrimitive::Series(resolved_series) => {
						if scan.index.is_some() {
							unimplemented!("series do not support indexes");
						}
						stack.push(PhysicalPlan::SeriesScan(SeriesScanNode {
							source: resolved_series.clone(),
							time_range_start: None,
							time_range_end: None,
							variant_tag: None,
						}));
					}
				},

				LogicalPlan::Take(take) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Take(TakeNode {
						take: take.take,
						input: self.bump_box(input),
					}));
				}

				LogicalPlan::Window(window) => {
					let input = stack.pop().map(|p| self.bump_box(p));
					stack.push(PhysicalPlan::Window(WindowNode {
						window_type: window.window_type,
						size: window.size,
						slide: window.slide,
						group_by: window.group_by,
						aggregations: window.aggregations,
						min_events: window.min_events,
						max_window_count: window.max_window_count,
						max_window_age: window.max_window_age,
						input,
					}));
				}

				LogicalPlan::Pipeline(pipeline) => {
					let pipeline_result = self.compile(rx, pipeline.steps)?;
					if let Some(result) = pipeline_result {
						stack.push(result);
					}
				}

				LogicalPlan::Declare(declare_node) => {
					let value = match declare_node.value {
						logical::LetValue::Expression(expr) => LetValue::Expression(expr),
						logical::LetValue::Statement(logical_plans) => {
							let mut last_plan = None;
							for logical_plan in logical_plans {
								if let Some(physical_plan) =
									self.compile(rx, once(logical_plan))?
								{
									last_plan = Some(physical_plan);
								}
							}
							match last_plan {
								Some(plan) => LetValue::Statement(self.bump_box(plan)),
								None => LetValue::Expression(Constant(
									ConstantExpression::None {
										fragment: Fragment::internal("none"),
									},
								)),
							}
						}
						logical::LetValue::EmptyFrame => LetValue::EmptyFrame,
					};

					stack.push(PhysicalPlan::Declare(DeclareNode {
						name: self.interner.intern_fragment(&declare_node.name),
						value,
					}));
				}

				LogicalPlan::Assign(assign_node) => {
					let value = match assign_node.value {
						logical::AssignValue::Expression(expr) => AssignValue::Expression(expr),
						logical::AssignValue::Statement(logical_plans) => {
							let mut last_plan = None;
							for logical_plan in logical_plans {
								if let Some(physical_plan) =
									self.compile(rx, once(logical_plan))?
								{
									last_plan = Some(physical_plan);
								}
							}
							match last_plan {
								Some(plan) => {
									AssignValue::Statement(self.bump_box(plan))
								}
								None => AssignValue::Expression(Constant(
									ConstantExpression::None {
										fragment: Fragment::internal("none"),
									},
								)),
							}
						}
					};

					stack.push(PhysicalPlan::Assign(AssignNode {
						name: self.interner.intern_fragment(&assign_node.name),
						value,
					}));
				}

				LogicalPlan::Append(append_node) => match append_node {
					logical::AppendNode::IntoVariable {
						target,
						source,
					} => {
						let source = match source {
							logical::AppendSourcePlan::Statement(logical_plans) => {
								let mut physical_plans = Vec::new();
								for logical_plan in logical_plans {
									if let Some(physical_plan) = self.compile(
										rx,
										std::iter::once(logical_plan),
									)? {
										physical_plans.push(physical_plan);
									}
								}
								AppendPhysicalSource::Statement(physical_plans)
							}
							logical::AppendSourcePlan::Inline(inline) => {
								AppendPhysicalSource::Inline(InlineDataNode {
									rows: inline.rows,
								})
							}
						};
						stack.push(PhysicalPlan::Append(AppendPhysicalNode::IntoVariable {
							target: self.interner.intern_fragment(&target),
							source,
						}));
					}
					logical::AppendNode::Query {
						with,
					} => {
						let left = stack.pop().unwrap();
						let right = self.compile(rx, with)?.unwrap();
						stack.push(PhysicalPlan::Append(AppendPhysicalNode::Query {
							left: self.bump_box(left),
							right: self.bump_box(right),
						}));
					}
				},

				LogicalPlan::VariableSource(source) => {
					let variable_expr = VariableExpression {
						fragment: self.interner.intern_fragment(&source.name),
					};

					stack.push(PhysicalPlan::Variable(VariableNode {
						variable_expr,
					}));
				}

				LogicalPlan::Environment(_) => {
					stack.push(PhysicalPlan::Environment(EnvironmentNode {}));
				}

				LogicalPlan::Conditional(conditional_node) => {
					let then_branch = if let Some(then_plan) = self.compile(
						rx,
						once(crate::bump::BumpBox::into_inner(conditional_node.then_branch)),
					)? {
						self.bump_box(then_plan)
					} else {
						return Err(RqlError::InternalFunctionError {
							name: "compile_physical".to_string(),
							fragment: Fragment::internal("compile_physical"),
							details: "Failed to compile conditional then branch"
								.to_string(),
						}
						.into());
					};

					let mut else_ifs = Vec::new();
					for else_if in conditional_node.else_ifs {
						let condition = else_if.condition;
						let then_branch = if let Some(plan) = self.compile(
							rx,
							once(crate::bump::BumpBox::into_inner(else_if.then_branch)),
						)? {
							self.bump_box(plan)
						} else {
							return Err(RqlError::InternalFunctionError {
								name: "compile_physical".to_string(),
								fragment: Fragment::internal("compile_physical"),
								details: "Failed to compile conditional else if branch"
									.to_string(),
							}
							.into());
						};
						else_ifs.push(ElseIfBranch {
							condition,
							then_branch,
						});
					}

					let else_branch =
						if let Some(else_logical) = conditional_node.else_branch {
							if let Some(plan) = self.compile(
								rx,
								once(crate::bump::BumpBox::into_inner(else_logical)),
							)? {
								Some(self.bump_box(plan))
							} else {
								return Err(RqlError::InternalFunctionError {
							name: "compile_physical".to_string(),
							fragment: Fragment::internal("compile_physical"),
							details: "Failed to compile conditional else branch".to_string(),
						}.into());
							}
						} else {
							None
						};

					stack.push(PhysicalPlan::Conditional(ConditionalNode {
						condition: conditional_node.condition,
						then_branch,
						else_ifs,
						else_branch,
					}));
				}

				LogicalPlan::Scalarize(scalarize_node) => {
					let input_plan = if let Some(plan) = self.compile(
						rx,
						once(crate::bump::BumpBox::into_inner(scalarize_node.input)),
					)? {
						self.bump_box(plan)
					} else {
						return Err(RqlError::InternalFunctionError {
							name: "compile_physical".to_string(),
							fragment: Fragment::internal("compile_physical"),
							details: "Failed to compile scalarize input".to_string(),
						}
						.into());
					};

					stack.push(PhysicalPlan::Scalarize(ScalarizeNode {
						input: input_plan,
						fragment: self.interner.intern_fragment(&scalarize_node.fragment),
					}));
				}

				LogicalPlan::Loop(loop_node) => {
					let mut body = Vec::new();
					for statement_plans in loop_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}
					stack.push(PhysicalPlan::Loop(LoopNode {
						body,
					}));
				}

				LogicalPlan::While(while_node) => {
					let mut body = Vec::new();
					for statement_plans in while_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}
					stack.push(PhysicalPlan::While(WhileNode {
						condition: while_node.condition,
						body,
					}));
				}

				LogicalPlan::For(for_node) => {
					let iterable = self
						.compile(rx, for_node.iterable)?
						.expect("For iterable must produce a plan");
					let mut body = Vec::new();
					for statement_plans in for_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}
					stack.push(PhysicalPlan::For(ForNode {
						variable_name: self.interner.intern_fragment(&for_node.variable_name),
						iterable: self.bump_box(iterable),
						body,
					}));
				}

				LogicalPlan::Break => {
					stack.push(PhysicalPlan::Break);
				}

				LogicalPlan::Continue => {
					stack.push(PhysicalPlan::Continue);
				}

				LogicalPlan::DefineFunction(def_node) => {
					let mut parameters = Vec::with_capacity(def_node.parameters.len());
					for p in def_node.parameters {
						parameters.push(nodes::FunctionParameter {
							name: self.interner.intern_fragment(&p.name),
							type_constraint: p.type_constraint,
						});
					}

					let mut body = Vec::new();
					for statement_plans in def_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}

					stack.push(PhysicalPlan::DefineFunction(DefineFunctionNode {
						name: self.interner.intern_fragment(&def_node.name),
						parameters,
						return_type: def_node.return_type,
						body,
					}));
				}

				LogicalPlan::Return(ret_node) => {
					stack.push(PhysicalPlan::Return(ReturnNode {
						value: ret_node.value,
					}));
				}

				LogicalPlan::CallFunction(call_node) => {
					stack.push(PhysicalPlan::CallFunction(CallFunctionNode {
						name: self.interner.intern_fragment(&call_node.name),
						arguments: call_node.arguments,
						is_procedure_call: call_node.is_procedure_call,
					}));
				}

				LogicalPlan::DefineClosure(closure_node) => {
					let mut parameters = Vec::with_capacity(closure_node.parameters.len());
					for p in closure_node.parameters {
						parameters.push(nodes::FunctionParameter {
							name: self.interner.intern_fragment(&p.name),
							type_constraint: p.type_constraint,
						});
					}

					let mut body = Vec::new();
					for statement_plans in closure_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}

					stack.push(PhysicalPlan::DefineClosure(DefineClosureNode {
						parameters,
						body,
					}));
				}

				_ => unimplemented!(),
			}
		}

		if stack.is_empty() {
			return Ok(None);
		}

		if stack.len() != 1 {
			dbg!(&stack);
			panic!("logical plan did not reduce to a single physical plan"); // FIXME
		}

		Ok(Some(stack.pop().unwrap()))
	}
}
