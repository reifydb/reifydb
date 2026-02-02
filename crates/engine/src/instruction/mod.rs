// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod compile;

use reifydb_rql::{
	expression::Expression,
	plan::physical::{
		self, PhysicalPlan,
		alter::{flow::AlterFlowNode, table::AlterTableNode, view::AlterViewNode},
	},
};
use reifydb_type::fragment::Fragment;

use crate::stack::ScopeType;

pub type Addr = usize;

pub enum Instruction {
	// === DDL (1:1 from PhysicalPlan) ===
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

	// === DML (1:1 from PhysicalPlan) ===
	Delete(physical::DeleteTableNode),
	DeleteRingBuffer(physical::DeleteRingBufferNode),
	InsertTable(physical::InsertTableNode),
	InsertRingBuffer(physical::InsertRingBufferNode),
	InsertDictionary(physical::InsertDictionaryNode),
	Update(physical::UpdateTableNode),
	UpdateRingBuffer(physical::UpdateRingBufferNode),

	// === Query (opaque pipeline) ===
	Query(PhysicalPlan),

	// === Variables ===
	Declare(physical::DeclareNode),
	Assign(physical::AssignNode),

	// === Linearized Control Flow ===
	Jump(Addr),
	JumpIfFalse {
		condition: Expression,
		addr: Addr,
	},
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

	// === FOR loop support ===
	ForInit {
		variable_name: Fragment,
		iterable: PhysicalPlan,
	},
	ForNext {
		variable_name: Fragment,
		addr: Addr,
	},

	// === Misc ===
	Nop,
	Halt,
}
