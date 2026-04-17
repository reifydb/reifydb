// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	value::{Value, constraint::TypeConstraint, r#type::Type},
};

use crate::{
	nodes::{
		self, AlterSequenceNode, CreateColumnPropertyNode, CreateDeferredViewNode, CreateDictionaryNode,
		CreateEventNode, CreateMigrationNode, CreateNamespaceNode, CreatePrimaryKeyNode, CreateProcedureNode,
		CreateRemoteNamespaceNode, CreateRingBufferNode, CreateSeriesNode, CreateSubscriptionNode,
		CreateSumTypeNode, CreateTableNode, CreateTagNode, CreateTestNode, CreateTransactionalViewNode,
		DeleteRingBufferNode, DeleteSeriesNode, DeleteTableNode, DispatchNode, FunctionParameter,
		InsertDictionaryNode, InsertRingBufferNode, InsertSeriesNode, InsertTableNode, MigrateNode,
		RollbackMigrationNode, UpdateRingBufferNode, UpdateSeriesNode, UpdateTableNode,
	},
	query::QueryPlan,
};

/// Address in the instruction stream (for jumps)
pub type Addr = usize;

/// A compiled user-defined function with pre-compiled body instructions
#[derive(Debug, Clone)]
pub struct CompiledFunction {
	/// Function name
	pub name: Fragment,
	/// Function parameters
	pub parameters: Vec<FunctionParameter>,
	/// Optional return type constraint
	pub return_type: Option<TypeConstraint>,
	/// Pre-compiled function body instructions
	pub body: Vec<Instruction>,
}

/// A compiled closure with captured environment
#[derive(Debug, Clone)]
pub struct CompiledClosure {
	/// Closure parameters
	pub parameters: Vec<FunctionParameter>,
	/// Pre-compiled closure body instructions
	pub body: Vec<Instruction>,
	/// Variable names referenced from the enclosing scope (free variables)
	pub captures: Vec<Fragment>,
}

/// Different types of scopes for variable management
#[derive(Debug, Clone, PartialEq)]
pub enum ScopeType {
	/// Global scope (cannot be exited)
	Global,
	/// Function scope
	Function,
	/// Block scope
	Block,
	/// Conditional scope (if/else)
	Conditional,
	/// Loop scope
	Loop,
}

#[derive(Debug, Clone)]
pub enum Instruction {
	PushConst(Value),
	PushNone,
	Pop,
	Dup,

	LoadVar(Fragment),
	StoreVar(Fragment),
	DeclareVar(Fragment),
	/// Load a field from a variable (e.g., $row.name)
	FieldAccess {
		object: Fragment,
		field: Fragment,
	},

	Add,
	Sub,
	Mul,
	Div,
	Rem,

	Negate,
	LogicNot,

	CmpEq,
	CmpNe,
	CmpLt,
	CmpLe,
	CmpGt,
	CmpGe,

	LogicAnd,
	LogicOr,
	LogicXor,

	Between,
	InList {
		count: u16,
		negated: bool,
	},
	Cast(Type),

	Jump(Addr),
	JumpIfFalsePop(Addr),
	JumpIfTruePop(Addr),
	EnterScope(ScopeType),
	ExitScope,
	Break {
		exit_scopes: usize,
		addr: Addr,
	},
	Continue {
		exit_scopes: usize,
		addr: Addr,
	},

	ForInit {
		variable_name: Fragment,
	},
	ForNext {
		variable_name: Fragment,
		addr: Addr,
	},

	DefineFunction(CompiledFunction),
	Call {
		name: Fragment,
		arity: u8,
		is_procedure_call: bool,
	},
	ReturnValue,
	ReturnVoid,

	DefineClosure(CompiledClosure),

	Query(QueryPlan),

	CreateNamespace(CreateNamespaceNode),
	CreateRemoteNamespace(CreateRemoteNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSumType(CreateSumTypeNode),
	CreateSubscription(CreateSubscriptionNode),
	CreatePrimaryKey(CreatePrimaryKeyNode),
	CreateColumnProperty(CreateColumnPropertyNode),
	CreateProcedure(CreateProcedureNode),
	CreateSeries(CreateSeriesNode),
	CreateEvent(CreateEventNode),
	CreateTag(CreateTagNode),
	CreateSource(nodes::CreateSourceNode),
	CreateSink(nodes::CreateSinkNode),
	CreateBinding(nodes::CreateBindingNode),
	CreateTest(CreateTestNode),
	AssertBlock(nodes::AssertBlockNode),

	CreateMigration(CreateMigrationNode),
	Migrate(MigrateNode),
	RollbackMigration(RollbackMigrationNode),
	Dispatch(DispatchNode),
	AlterSequence(AlterSequenceNode),
	AlterTable(nodes::AlterTableNode),
	AlterRemoteNamespace(nodes::AlterRemoteNamespaceNode),

	DropNamespace(nodes::DropNamespaceNode),
	DropTable(nodes::DropTableNode),
	DropView(nodes::DropViewNode),
	DropRingBuffer(nodes::DropRingBufferNode),
	DropDictionary(nodes::DropDictionaryNode),
	DropSumType(nodes::DropSumTypeNode),
	DropSubscription(nodes::DropSubscriptionNode),
	DropSeries(nodes::DropSeriesNode),
	DropSource(nodes::DropSourceNode),
	DropSink(nodes::DropSinkNode),
	DropProcedure(nodes::DropProcedureNode),
	DropHandler(nodes::DropHandlerNode),
	DropTest(nodes::DropTestNode),
	DropBinding(nodes::DropBindingNode),

	CreateIdentity(nodes::CreateIdentityNode),
	CreateRole(nodes::CreateRoleNode),
	CreateAuthentication(nodes::CreateAuthenticationNode),
	Grant(nodes::GrantNode),
	Revoke(nodes::RevokeNode),
	DropIdentity(nodes::DropIdentityNode),
	DropRole(nodes::DropRoleNode),
	DropAuthentication(nodes::DropAuthenticationNode),
	CreatePolicy(nodes::CreatePolicyNode),
	AlterPolicy(nodes::AlterPolicyNode),
	DropPolicy(nodes::DropPolicyNode),

	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	DeleteSeries(DeleteSeriesNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	InsertSeries(InsertSeriesNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),
	UpdateSeries(UpdateSeriesNode),

	Append {
		target: Fragment,
	},

	Emit,

	Nop,
	Halt,
}
