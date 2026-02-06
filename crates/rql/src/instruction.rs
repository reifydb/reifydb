// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{fragment::Fragment, value::constraint::TypeConstraint};

use crate::{
	expression::Expression,
	nodes::{
		AlterSequenceNode, AssignNode, CallFunctionNode, CreateDeferredViewNode, CreateDictionaryNode,
		CreateFlowNode, CreateNamespaceNode, CreateRingBufferNode, CreateSubscriptionNode, CreateTableNode,
		CreateTransactionalViewNode, DeclareNode, DeleteRingBufferNode, DeleteTableNode, FunctionParameter,
		InsertDictionaryNode, InsertRingBufferNode, InsertTableNode, ReturnNode, UpdateRingBufferNode,
		UpdateTableNode,
	},
	plan::physical::alter::{flow::AlterFlowNode, table::AlterTableNode, view::AlterViewNode},
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
	CreateNamespace(CreateNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateFlow(CreateFlowNode),
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateDictionary(CreateDictionaryNode),
	CreateSubscription(CreateSubscriptionNode),
	AlterSequence(AlterSequenceNode),
	AlterTable(AlterTableNode),
	AlterView(AlterViewNode),
	AlterFlow(AlterFlowNode),

	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),

	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),

	Query(QueryPlan),

	Declare(DeclareNode),
	Assign(AssignNode),

	Jump(Addr),
	EvalCondition(Expression),
	JumpIfFalsePop(Addr),
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

	DefineFunction(CompiledFunctionDef),
	CallFunction(CallFunctionNode),
	Return(ReturnNode),

	Emit,
	Pop,
	Dup,

	Nop,
	Halt,
}
