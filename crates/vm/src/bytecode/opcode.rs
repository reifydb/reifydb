// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Bytecode opcodes for the VM.

/// Bytecode opcodes for the VM.
/// Each opcode is a single byte, operands follow inline.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
	// ─────────────────────────────────────────────────────────────
	// Stack Operations
	// ─────────────────────────────────────────────────────────────
	/// Push constant from pool onto operand stack
	/// Operand: u16 (constant index)
	PushConst = 0x01,

	/// Push expression reference onto operand stack
	/// Operand: u16 (expression index)
	PushExpr = 0x02,

	/// Push column reference onto operand stack
	/// Operand: u16 (column name index in constants)
	PushColRef = 0x03,

	/// Push column list onto operand stack
	/// Operand: u16 (column list index)
	PushColList = 0x04,

	/// Pop top of operand stack
	Pop = 0x05,

	/// Duplicate top of operand stack
	Dup = 0x06,

	/// Push sort specification onto operand stack
	/// Operand: u16 (sort spec index)
	PushSortSpec = 0x07,

	/// Push extension specification onto operand stack
	/// Operand: u16 (extension spec index)
	PushExtSpec = 0x08,

	// ─────────────────────────────────────────────────────────────
	// Variable Operations
	// ─────────────────────────────────────────────────────────────
	/// Load variable onto operand stack
	/// Operand: u16 (variable name index)
	LoadVar = 0x10,

	/// Store operand stack top into variable
	/// Operand: u16 (variable name index)
	StoreVar = 0x11,

	/// Store pipeline stack top into variable
	/// Operand: u16 (variable name index)
	StorePipeline = 0x12,

	/// Load pipeline variable onto pipeline stack
	/// Operand: u16 (variable name index)
	LoadPipeline = 0x13,

	/// Update existing variable (searches all scopes)
	/// Operand: u16 (variable name index)
	UpdateVar = 0x14,

	// ─────────────────────────────────────────────────────────────
	// Pipeline Operations
	// ─────────────────────────────────────────────────────────────
	/// Push table scan onto pipeline stack
	/// Operand: u16 (source index)
	Source = 0x20,

	/// Push empty/inline pipeline onto pipeline stack
	Inline = 0x21,

	/// Apply operator to pipeline (uses operand stack for config)
	/// Operand: u8 (operator kind)
	Apply = 0x22,

	/// Pop pipeline, collect to Columns, push to operand stack
	Collect = 0x23,

	/// Merge top two pipelines
	Merge = 0x24,

	/// Pop pipeline from pipeline stack
	PopPipeline = 0x25,

	/// Duplicate top of pipeline stack
	DupPipeline = 0x26,

	// ─────────────────────────────────────────────────────────────
	// Control Flow
	// ─────────────────────────────────────────────────────────────
	/// Unconditional jump
	/// Operand: i16 (relative offset)
	Jump = 0x40,

	/// Jump if top of operand stack is truthy (pop)
	/// Operand: i16 (relative offset)
	JumpIf = 0x41,

	/// Jump if top of operand stack is falsy (pop)
	/// Operand: i16 (relative offset)
	JumpIfNot = 0x42,

	// ─────────────────────────────────────────────────────────────
	// Function Calls
	// ─────────────────────────────────────────────────────────────
	/// Call user-defined function
	/// Operand: u16 (function index)
	Call = 0x50,

	/// Return from function
	Return = 0x51,

	/// Call built-in function
	/// Operand: u16 (builtin id), u8 (arg count)
	CallBuiltin = 0x52,

	// ─────────────────────────────────────────────────────────────
	// Scope Management
	// ─────────────────────────────────────────────────────────────
	/// Enter new scope
	EnterScope = 0x60,

	/// Exit current scope
	ExitScope = 0x61,

	// ─────────────────────────────────────────────────────────────
	// Frame/Record Operations (for iteration)
	// ─────────────────────────────────────────────────────────────
	/// Get row count from Frame on operand stack
	/// Pops Frame, pushes Int8(row_count)
	FrameLen = 0x70,

	/// Get row as Record from Frame at given index
	/// Pops index (Int8), pops Frame, pushes Record
	FrameRow = 0x71,

	/// Get field from Record by name
	/// Operand: u16 (field name index in constants)
	/// Pops Record, pushes field value
	GetField = 0x72,

	// ─────────────────────────────────────────────────────────────
	// Scalar Arithmetic and Comparison
	// ─────────────────────────────────────────────────────────────
	/// Add two integers on operand stack
	/// Pops two Int8 values, pushes Int8 result
	IntAdd = 0x80,

	/// Compare two integers: less than
	/// Pops two Int8 values (b, a), pushes Boolean(a < b)
	IntLt = 0x81,

	/// Compare two integers: equal
	/// Pops two Int8 values (b, a), pushes Boolean(a == b)
	IntEq = 0x82,

	// ─────────────────────────────────────────────────────────────
	// I/O Operations
	// ─────────────────────────────────────────────────────────────
	/// Print value from operand stack (for console::log)
	/// Pops value, prints it to stdout
	PrintOut = 0x90,

	// ─────────────────────────────────────────────────────────────
	// Control
	// ─────────────────────────────────────────────────────────────
	/// No operation
	Nop = 0xFE,

	/// Halt execution
	Halt = 0xFF,
}

impl TryFrom<u8> for Opcode {
	type Error = u8;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0x01 => Ok(Opcode::PushConst),
			0x02 => Ok(Opcode::PushExpr),
			0x03 => Ok(Opcode::PushColRef),
			0x04 => Ok(Opcode::PushColList),
			0x05 => Ok(Opcode::Pop),
			0x06 => Ok(Opcode::Dup),
			0x07 => Ok(Opcode::PushSortSpec),
			0x08 => Ok(Opcode::PushExtSpec),
			0x10 => Ok(Opcode::LoadVar),
			0x11 => Ok(Opcode::StoreVar),
			0x12 => Ok(Opcode::StorePipeline),
			0x13 => Ok(Opcode::LoadPipeline),
			0x14 => Ok(Opcode::UpdateVar),
			0x20 => Ok(Opcode::Source),
			0x21 => Ok(Opcode::Inline),
			0x22 => Ok(Opcode::Apply),
			0x23 => Ok(Opcode::Collect),
			0x24 => Ok(Opcode::Merge),
			0x25 => Ok(Opcode::PopPipeline),
			0x26 => Ok(Opcode::DupPipeline),
			0x40 => Ok(Opcode::Jump),
			0x41 => Ok(Opcode::JumpIf),
			0x42 => Ok(Opcode::JumpIfNot),
			0x50 => Ok(Opcode::Call),
			0x51 => Ok(Opcode::Return),
			0x52 => Ok(Opcode::CallBuiltin),
			0x60 => Ok(Opcode::EnterScope),
			0x61 => Ok(Opcode::ExitScope),
			0x70 => Ok(Opcode::FrameLen),
			0x71 => Ok(Opcode::FrameRow),
			0x72 => Ok(Opcode::GetField),
			0x80 => Ok(Opcode::IntAdd),
			0x81 => Ok(Opcode::IntLt),
			0x82 => Ok(Opcode::IntEq),
			0x90 => Ok(Opcode::PrintOut),
			0xFE => Ok(Opcode::Nop),
			0xFF => Ok(Opcode::Halt),
			_ => Err(value),
		}
	}
}

/// Operator kinds for the Apply opcode
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKind {
	Filter = 0,
	Select = 1,
	Extend = 2,
	Take = 3,
	Sort = 4,
}

impl TryFrom<u8> for OperatorKind {
	type Error = u8;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(OperatorKind::Filter),
			1 => Ok(OperatorKind::Select),
			2 => Ok(OperatorKind::Extend),
			3 => Ok(OperatorKind::Take),
			4 => Ok(OperatorKind::Sort),
			_ => Err(value),
		}
	}
}
