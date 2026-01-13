// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Opcode handler for the bytecode VM.
//!
//! Handlers are grouped by category:
//! - `control`: Nop, Halt
//! - `scope`: EnterScope, ExitScope
//! - `int_math`: IntAdd, IntSub, IntMul, IntDiv, IntLt, IntLe, IntGt, IntGe, IntEq, IntNe
//! - `stack`: PushConst, PushExpr, PushColRef, PushColList, PushSortSpec, PushExtSpec
//! - `variables`: LoadVar, StoreVar, UpdateVar, LoadPipeline, StorePipeline, LoadInternalVar, StoreInternalVar
//! - `jumps`: Jump, JumpIf, JumpIfNot
//! - `calls`: Call, Return, CallBuiltin
//! - `frame`: FrameLen, FrameRow, GetField
//! - `pipeline`: Source, Inline, Apply, Collect, PopPipeline, Merge, EvalMapWithoutInput, EvalExpandWithoutInput, FetchBatch, CheckComplete
//! - `ddl`: CreateNamespace, CreateTable, DropObject
//! - `dml`: InsertRow, UpdateRow, DeleteRow
//! - `subquery`: ExecSubqueryExists, ExecSubqueryIn, ExecSubqueryScalar

use reifydb_rqlv2::bytecode::BytecodeReader;
use reifydb_transaction::StandardTransaction;

use crate::error::Result;
use crate::runtime::dispatch::DispatchResult;
use crate::runtime::state::VmState;

/// Context passed to opcode handler.
pub struct HandlerContext<'vm, 'tx, 'a> {
	/// The VM state.
	pub vm: &'vm mut VmState,
	/// Bytecode reader positioned after the opcode byte.
	pub reader: &'vm mut BytecodeReader<'a>,
	/// Optional transaction for storage operations.
	pub tx: Option<&'vm mut StandardTransaction<'tx>>,
}

impl<'vm, 'tx, 'a> HandlerContext<'vm, 'tx, 'a> {
	/// Create a new handler context.
	pub fn new(
		vm: &'vm mut VmState,
		reader: &'vm mut BytecodeReader<'a>,
		tx: Option<&'vm mut StandardTransaction<'tx>>,
	) -> Self {
		Self { vm, reader, tx }
	}

	/// Read a u8 from the bytecode.
	pub fn read_u8(&mut self) -> Result<u8> {
		self.reader.read_u8().ok_or_else(|| crate::error::VmError::InvalidBytecode {
			position: self.reader.position(),
		})
	}

	/// Read a u16 from the bytecode.
	pub fn read_u16(&mut self) -> Result<u16> {
		self.reader.read_u16().ok_or_else(|| crate::error::VmError::InvalidBytecode {
			position: self.reader.position(),
		})
	}

	/// Read an i16 from the bytecode.
	pub fn read_i16(&mut self) -> Result<i16> {
		self.reader.read_i16().ok_or_else(|| crate::error::VmError::InvalidBytecode {
			position: self.reader.position(),
		})
	}

	/// Read a u32 from the bytecode.
	pub fn read_u32(&mut self) -> Result<u32> {
		self.reader.read_u32().ok_or_else(|| crate::error::VmError::InvalidBytecode {
			position: self.reader.position(),
		})
	}

	/// Advance the IP to the current reader position and return Continue.
	pub fn advance_and_continue(&mut self) -> DispatchResult {
		self.vm.ip = self.reader.position();
		DispatchResult::Continue
	}

	/// Get a mutable reference to the transaction, or error if not available.
	pub fn require_tx(&mut self) -> Result<&mut StandardTransaction<'tx>> {
		match &mut self.tx {
			Some(tx) => Ok(*tx),
			None => Err(crate::error::VmError::UnsupportedOperation {
				operation: "Operation requires a transaction".into(),
			}),
		}
	}
}

/// Type signature for opcode handler.
pub type OpcodeHandler = for<'vm, 'tx, 'a> fn(&mut HandlerContext<'vm, 'tx, 'a>) -> Result<DispatchResult>;

/// Invalid opcode handler - called for undefined opcodes.
pub fn handle_invalid<'vm, 'tx, 'a>(ctx: &mut HandlerContext<'vm, 'tx, 'a>) -> Result<DispatchResult> {
	Err(crate::error::VmError::InvalidBytecode {
		position: ctx.vm.ip,
	})
}

// Handler modules grouped by category
pub mod calls;
pub mod control;
pub mod ddl;
pub mod dml;
pub mod frame;
pub mod jumps;
pub mod math;
pub mod pipeline;
pub mod scope;
pub mod stack;
pub mod subquery;
pub mod variables;
