// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	value::{Value, constraint::TypeConstraint, r#type::Type},
};

use crate::{
	nodes::{
		AlterFlowNode, AlterReducerNode, AlterSequenceNode, AlterTableNode, AlterViewNode,
		CreateDeferredViewNode, CreateDictionaryNode, CreateFlowNode, CreateNamespaceNode, CreateReducerNode,
		CreateRingBufferNode, CreateSubscriptionNode, CreateTableNode, CreateTransactionalViewNode,
		DeleteRingBufferNode, DeleteTableNode, FunctionParameter, InsertDictionaryNode, InsertRingBufferNode,
		InsertTableNode, UpdateRingBufferNode, UpdateTableNode,
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
	},
	ReturnValue,
	ReturnVoid,

	// === Query (volcano model) ===
	Query(QueryPlan),

	// === DDL ===
	CreateNamespace(CreateNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateFlow(CreateFlowNode),
	CreateReducer(CreateReducerNode),
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSubscription(CreateSubscriptionNode),
	AlterSequence(AlterSequenceNode),
	AlterTable(AlterTableNode),
	AlterView(AlterViewNode),
	AlterFlow(AlterFlowNode),
	AlterReducer(AlterReducerNode),

	// === DML ===
	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
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
