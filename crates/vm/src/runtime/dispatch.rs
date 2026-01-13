// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode dispatch and execution.

use reifydb_rqlv2::bytecode::Opcode;

use crate::error::Result;
use crate::handler::{self, HandlerContext, OpcodeHandler};
use crate::pipeline::Pipeline;

/// Result of dispatching a single instruction.
pub enum DispatchResult {
	/// Continue to next instruction.
	Continue,
	/// Halt execution.
	Halt,
	/// Yield a pipeline result (for top-level expression).
	Yield(Pipeline),
}

/// Build the dispatch table at compile time.
const fn build_dispatch_table() -> [OpcodeHandler; 256] {
	let mut table: [OpcodeHandler; 256] = [handler::handle_invalid; 256];

	// Control
	table[Opcode::Nop as usize] = handler::control::nop;
	table[Opcode::Halt as usize] = handler::control::halt;

	// Scope
	table[Opcode::EnterScope as usize] = handler::scope::enter_scope;
	table[Opcode::ExitScope as usize] = handler::scope::exit_scope;

	// Integer math
	table[Opcode::IntAdd as usize] = handler::math::int_add;
	table[Opcode::IntSub as usize] = handler::math::int_sub;
	table[Opcode::IntMul as usize] = handler::math::int_mul;
	table[Opcode::IntDiv as usize] = handler::math::int_div;
	table[Opcode::IntLt as usize] = handler::math::int_lt;
	table[Opcode::IntLe as usize] = handler::math::int_le;
	table[Opcode::IntGt as usize] = handler::math::int_gt;
	table[Opcode::IntGe as usize] = handler::math::int_ge;
	table[Opcode::IntEq as usize] = handler::math::int_eq;
	table[Opcode::IntNe as usize] = handler::math::int_ne;

	// Stack operations
	table[Opcode::PushConst as usize] = handler::stack::push_const;
	table[Opcode::PushExpr as usize] = handler::stack::push_expr;
	table[Opcode::PushColRef as usize] = handler::stack::push_col_ref;
	table[Opcode::PushColList as usize] = handler::stack::push_col_list;
	table[Opcode::PushSortSpec as usize] = handler::stack::push_sort_spec;
	table[Opcode::PushExtSpec as usize] = handler::stack::push_ext_spec;

	// Variables
	table[Opcode::LoadVar as usize] = handler::variables::load_var;
	table[Opcode::StoreVar as usize] = handler::variables::store_var;
	table[Opcode::UpdateVar as usize] = handler::variables::update_var;
	table[Opcode::LoadPipeline as usize] = handler::variables::load_pipeline;
	table[Opcode::StorePipeline as usize] = handler::variables::store_pipeline;
	table[Opcode::LoadInternalVar as usize] = handler::variables::load_internal_var;
	table[Opcode::StoreInternalVar as usize] = handler::variables::store_internal_var;

	// Jumps
	table[Opcode::Jump as usize] = handler::jumps::jump;
	table[Opcode::JumpIf as usize] = handler::jumps::jump_if;
	table[Opcode::JumpIfNot as usize] = handler::jumps::jump_if_not;

	// Function calls
	table[Opcode::Call as usize] = handler::calls::call;
	table[Opcode::Return as usize] = handler::calls::return_op;
	table[Opcode::CallBuiltin as usize] = handler::calls::call_builtin;

	// Frame/Record operations
	table[Opcode::FrameLen as usize] = handler::frame::frame_len;
	table[Opcode::FrameRow as usize] = handler::frame::frame_row;
	table[Opcode::GetField as usize] = handler::frame::get_field;

	// Pipeline operations
	table[Opcode::Source as usize] = handler::pipeline::source;
	table[Opcode::Inline as usize] = handler::pipeline::inline;
	table[Opcode::Apply as usize] = handler::pipeline::apply;
	table[Opcode::Collect as usize] = handler::pipeline::collect;
	table[Opcode::PopPipeline as usize] = handler::pipeline::pop_pipeline;
	table[Opcode::Merge as usize] = handler::pipeline::merge;
	table[Opcode::EvalMapWithoutInput as usize] = handler::pipeline::eval_without_input;
	table[Opcode::EvalExpandWithoutInput as usize] = handler::pipeline::eval_without_input;
	table[Opcode::FetchBatch as usize] = handler::pipeline::fetch_batch;
	table[Opcode::CheckComplete as usize] = handler::pipeline::check_complete;

	// DDL operations
	table[Opcode::CreateNamespace as usize] = handler::ddl::create_namespace;
	table[Opcode::CreateTable as usize] = handler::ddl::create_table;
	table[Opcode::DropObject as usize] = handler::ddl::drop_object;
	table[Opcode::CreateView as usize] = handler::ddl::unsupported_ddl;
	table[Opcode::CreateIndex as usize] = handler::ddl::unsupported_ddl;
	table[Opcode::CreateSequence as usize] = handler::ddl::unsupported_ddl;
	table[Opcode::CreateRingBuffer as usize] = handler::ddl::unsupported_ddl;
	table[Opcode::CreateDictionary as usize] = handler::ddl::unsupported_ddl;

	// DML operations
	table[Opcode::InsertRow as usize] = handler::dml::insert_row;
	table[Opcode::UpdateRow as usize] = handler::dml::update_row;
	table[Opcode::DeleteRow as usize] = handler::dml::delete_row;

	// Subquery operations
	table[Opcode::ExecSubqueryExists as usize] = handler::subquery::exec_subquery_exists;
	table[Opcode::ExecSubqueryIn as usize] = handler::subquery::exec_subquery_in;
	table[Opcode::ExecSubqueryScalar as usize] = handler::subquery::exec_subquery_scalar;

	table
}

/// The dispatch table mapping opcode bytes to handler.
pub static DISPATCH_TABLE: [OpcodeHandler; 256] = build_dispatch_table();

/// Execute a single instruction using the dispatch table.
pub fn dispatch_step<'tx>(ctx: &mut HandlerContext<'_, 'tx, '_>, opcode: u8) -> Result<DispatchResult> {
	let handler = DISPATCH_TABLE[opcode as usize];
	handler(ctx)
}
