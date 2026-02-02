// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod compile;
pub(crate) mod ddl;
pub(crate) mod dml;

use reifydb_rql::{
	expression::Expression,
	plan::physical::{
		self, PhysicalPlan,
		alter::{flow::AlterFlowNode, table::AlterTableNode, view::AlterViewNode},
	},
};
use reifydb_type::fragment::Fragment;

use crate::vm::stack::ScopeType;

pub type Addr = usize;

pub enum Instruction {
	CreateNamespace(physical::CreateNamespaceNode),
	CreateTable(physical::CreateTableNode),
	CreateRingBuffer(physical::CreateRingBufferNode),
	CreateFlow(physical::CreateFlowNode),
	CreateDeferredView(physical::CreateDeferredViewNode),
	CreateTransactionalView(physical::CreateTransactionalViewNode),
	CreateDictionary(physical::CreateDictionaryNode),
	CreateSubscription(physical::CreateSubscriptionNode),
	AlterSequence(physical::AlterSequenceNode),
	AlterTable(AlterTableNode),
	AlterView(AlterViewNode),
	AlterFlow(AlterFlowNode),

	Delete(physical::DeleteTableNode),
	DeleteRingBuffer(physical::DeleteRingBufferNode),
	InsertTable(physical::InsertTableNode),
	InsertRingBuffer(physical::InsertRingBufferNode),
	InsertDictionary(physical::InsertDictionaryNode),
	Update(physical::UpdateTableNode),
	UpdateRingBuffer(physical::UpdateRingBufferNode),

	Query(PhysicalPlan),

	Declare(physical::DeclareNode),
	Assign(physical::AssignNode),

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

	Emit,
	Pop,
	Dup,

	Nop,
	Halt,
}
