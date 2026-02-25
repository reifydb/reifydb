// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	value::{Value, constraint::TypeConstraint, r#type::Type},
};

use crate::{
	nodes::{
		self, AlterFlowNode, AlterSequenceNode, CreateDeferredViewNode, CreateDictionaryNode, CreateEventNode,
		CreateFlowNode, CreateHandlerNode, CreateNamespaceNode, CreatePolicyNode, CreatePrimaryKeyNode,
		CreateProcedureNode, CreateRingBufferNode, CreateSeriesNode, CreateSubscriptionNode, CreateSumTypeNode,
		CreateTableNode, CreateTagNode, CreateTransactionalViewNode, DeleteRingBufferNode, DeleteSeriesNode,
		DeleteTableNode, DispatchNode, FunctionParameter, InsertDictionaryNode, InsertRingBufferNode,
		InsertSeriesNode, InsertTableNode, UpdateRingBufferNode, UpdateTableNode,
	},
	query::QueryPlan,
};

/// Address in the instruction stream (for jumps)
pub type Addr = usize;

/// A compiled user-defined function with pre-compiled body instructions
#[derive(Debug, Clone)]
pub struct CompiledFunctionDef {
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
pub struct CompiledClosureDef {
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
	// === Stack ===
	PushConst(Value),
	PushNone,
	Pop,
	Dup,

	// === Variables ===
	LoadVar(Fragment),
	StoreVar(Fragment),
	DeclareVar(Fragment),
	/// Load a field from a variable (e.g., $row.name)
	FieldAccess {
		object: Fragment,
		field: Fragment,
	},

	// === Arithmetic (pop 2, push 1) ===
	Add,
	Sub,
	Mul,
	Div,
	Rem,

	// === Unary ===
	Negate,
	LogicNot,

	// === Comparison (pop 2, push Boolean) ===
	CmpEq,
	CmpNe,
	CmpLt,
	CmpLe,
	CmpGt,
	CmpGe,

	// === Logic ===
	LogicAnd,
	LogicOr,
	LogicXor,

	// === Compound ===
	Between,
	InList {
		count: u16,
		negated: bool,
	},
	Cast(Type),

	// === Control flow ===
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

	// === Loops ===
	ForInit {
		variable_name: Fragment,
	},
	ForNext {
		variable_name: Fragment,
		addr: Addr,
	},

	// === Functions ===
	DefineFunction(CompiledFunctionDef),
	Call {
		name: Fragment,
		arity: u8,
		is_procedure_call: bool,
	},
	ReturnValue,
	ReturnVoid,

	// === Closures ===
	DefineClosure(CompiledClosureDef),

	// === Query (volcano model) ===
	Query(QueryPlan),

	// === DDL ===
	CreateNamespace(CreateNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateFlow(CreateFlowNode),
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSumType(CreateSumTypeNode),
	CreateSubscription(CreateSubscriptionNode),
	CreatePrimaryKey(CreatePrimaryKeyNode),
	CreatePolicy(CreatePolicyNode),
	CreateProcedure(CreateProcedureNode),
	CreateSeries(CreateSeriesNode),
	CreateEvent(CreateEventNode),
	CreateTag(CreateTagNode),
	CreateHandler(CreateHandlerNode),
	Dispatch(DispatchNode),
	AlterSequence(AlterSequenceNode),
	AlterFlow(AlterFlowNode),

	// === DDL (Drop) ===
	DropNamespace(nodes::DropNamespaceNode),
	DropTable(nodes::DropTableNode),
	DropView(nodes::DropViewNode),
	DropRingBuffer(nodes::DropRingBufferNode),
	DropDictionary(nodes::DropDictionaryNode),
	DropSumType(nodes::DropSumTypeNode),
	DropFlow(nodes::DropFlowNode),
	DropSubscription(nodes::DropSubscriptionNode),
	DropSeries(nodes::DropSeriesNode),

	// === DML ===
	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	DeleteSeries(DeleteSeriesNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	InsertSeries(InsertSeriesNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),

	// === Append ===
	Append {
		target: Fragment,
	},

	// === Output ===
	Emit,

	// === Control ===
	Nop,
	Halt,
}
