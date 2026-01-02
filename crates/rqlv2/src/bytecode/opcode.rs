// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Bytecode opcodes for the VM.
/// ! Each opcode is a single byte, operands follow inline.
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
	// Variable Operations (by name - legacy)
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
	// Variable Operations (by ID - faster lookup)
	// ─────────────────────────────────────────────────────────────
	/// Load variable onto operand stack by ID
	/// Operand: u32 (variable ID)
	LoadVarById = 0x15,

	/// Store operand stack top into variable by ID
	/// Operand: u32 (variable ID)
	StoreVarById = 0x16,

	/// Update existing variable by ID (searches all scopes)
	/// Operand: u32 (variable ID)
	UpdateVarById = 0x17,

	/// Load pipeline variable onto pipeline stack by ID
	/// Operand: u32 (variable ID)
	LoadPipelineById = 0x18,

	/// Store pipeline stack top into variable by ID
	/// Operand: u32 (variable ID)
	StorePipelineById = 0x19,

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

	/// Fetch next batch from active scan
	/// Operand: u16 (source index)
	/// Pushes batch onto pipeline stack, pushes boolean (has_more) onto operand stack
	FetchBatch = 0x27,

	/// Check if query is complete (for early termination)
	/// No operand - pops boolean from operand stack
	/// Used by TAKE and other limiting operators
	CheckComplete = 0x28,

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
	// Scalar Arithmetic and Comparison (for control flow)
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

	/// Subtract two integers on operand stack
	/// Pops two Int8 values (b, a), pushes Int8 result (a - b)
	IntSub = 0x83,

	/// Multiply two integers on operand stack
	/// Pops two Int8 values, pushes Int8 result
	IntMul = 0x84,

	/// Divide two integers on operand stack
	/// Pops two Int8 values (b, a), pushes Int8 result (a / b)
	IntDiv = 0x85,

	// ─────────────────────────────────────────────────────────────
	// I/O Operations
	// ─────────────────────────────────────────────────────────────
	/// Print value from operand stack (for console::log)
	/// Pops value, prints it to stdout
	PrintOut = 0x90,

	// ─────────────────────────────────────────────────────────────
	// Columnar Arithmetic and Comparison (for expressions)
	// ─────────────────────────────────────────────────────────────
	/// Add two columns (or scalar+column with broadcast)
	/// Pops two Column/Scalar values, pushes Column result
	ColAdd = 0xA0,

	/// Subtract two columns
	ColSub = 0xA1,

	/// Multiply two columns
	ColMul = 0xA2,

	/// Divide two columns
	ColDiv = 0xA3,

	/// Compare columns: less than
	ColLt = 0xA4,

	/// Compare columns: less than or equal
	ColLe = 0xA5,

	/// Compare columns: greater than
	ColGt = 0xA6,

	/// Compare columns: greater than or equal
	ColGe = 0xA7,

	/// Compare columns: equal
	ColEq = 0xA8,

	/// Compare columns: not equal
	ColNe = 0xA9,

	/// Logical AND on boolean columns
	ColAnd = 0xAA,

	/// Logical OR on boolean columns
	ColOr = 0xAB,

	/// Logical NOT on boolean column
	ColNot = 0xAC,

	// ─────────────────────────────────────────────────────────────
	// DML Operations
	// ─────────────────────────────────────────────────────────────
	/// Insert rows into table
	/// Operand: u16 (target index)
	InsertRow = 0xB0,

	/// Update rows in table
	/// Operand: u16 (target index)
	UpdateRow = 0xB1,

	/// Delete rows from table
	/// Operand: u16 (target index)
	DeleteRow = 0xB2,

	// ─────────────────────────────────────────────────────────────
	// DDL Operations
	// ─────────────────────────────────────────────────────────────
	/// Create namespace
	/// Operand: u16 (definition index)
	CreateNamespace = 0xC0,

	/// Create table
	/// Operand: u16 (definition index)
	CreateTable = 0xC1,

	/// Create view
	/// Operand: u16 (definition index)
	CreateView = 0xC2,

	/// Create index
	/// Operand: u16 (definition index)
	CreateIndex = 0xC3,

	/// Create sequence
	/// Operand: u16 (definition index)
	CreateSequence = 0xC4,

	/// Create ring buffer
	/// Operand: u16 (definition index)
	CreateRingBuffer = 0xC5,

	/// Create dictionary
	/// Operand: u16 (definition index)
	CreateDictionary = 0xC6,

	/// Alter table
	/// Operand: u16 (definition index)
	AlterTable = 0xC8,

	/// Alter sequence
	/// Operand: u16 (definition index)
	AlterSequence = 0xC9,

	/// Drop object
	/// Operand: u16 (definition index), u8 (object type)
	DropObject = 0xD0,

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
			// Stack Operations
			0x01 => Ok(Opcode::PushConst),
			0x02 => Ok(Opcode::PushExpr),
			0x03 => Ok(Opcode::PushColRef),
			0x04 => Ok(Opcode::PushColList),
			0x05 => Ok(Opcode::Pop),
			0x06 => Ok(Opcode::Dup),
			0x07 => Ok(Opcode::PushSortSpec),
			0x08 => Ok(Opcode::PushExtSpec),
			// Variable Operations (by name)
			0x10 => Ok(Opcode::LoadVar),
			0x11 => Ok(Opcode::StoreVar),
			0x12 => Ok(Opcode::StorePipeline),
			0x13 => Ok(Opcode::LoadPipeline),
			0x14 => Ok(Opcode::UpdateVar),
			// Variable Operations (by ID)
			0x15 => Ok(Opcode::LoadVarById),
			0x16 => Ok(Opcode::StoreVarById),
			0x17 => Ok(Opcode::UpdateVarById),
			0x18 => Ok(Opcode::LoadPipelineById),
			0x19 => Ok(Opcode::StorePipelineById),
			// Pipeline Operations
			0x20 => Ok(Opcode::Source),
			0x21 => Ok(Opcode::Inline),
			0x22 => Ok(Opcode::Apply),
			0x23 => Ok(Opcode::Collect),
			0x24 => Ok(Opcode::Merge),
			0x25 => Ok(Opcode::PopPipeline),
			0x26 => Ok(Opcode::DupPipeline),
			0x27 => Ok(Opcode::FetchBatch),
			0x28 => Ok(Opcode::CheckComplete),
			// Control Flow
			0x40 => Ok(Opcode::Jump),
			0x41 => Ok(Opcode::JumpIf),
			0x42 => Ok(Opcode::JumpIfNot),
			// Function Calls
			0x50 => Ok(Opcode::Call),
			0x51 => Ok(Opcode::Return),
			0x52 => Ok(Opcode::CallBuiltin),
			// Scope Management
			0x60 => Ok(Opcode::EnterScope),
			0x61 => Ok(Opcode::ExitScope),
			// Frame/Record Operations
			0x70 => Ok(Opcode::FrameLen),
			0x71 => Ok(Opcode::FrameRow),
			0x72 => Ok(Opcode::GetField),
			// Scalar Arithmetic
			0x80 => Ok(Opcode::IntAdd),
			0x81 => Ok(Opcode::IntLt),
			0x82 => Ok(Opcode::IntEq),
			0x83 => Ok(Opcode::IntSub),
			0x84 => Ok(Opcode::IntMul),
			0x85 => Ok(Opcode::IntDiv),
			// I/O
			0x90 => Ok(Opcode::PrintOut),
			// Columnar Operations
			0xA0 => Ok(Opcode::ColAdd),
			0xA1 => Ok(Opcode::ColSub),
			0xA2 => Ok(Opcode::ColMul),
			0xA3 => Ok(Opcode::ColDiv),
			0xA4 => Ok(Opcode::ColLt),
			0xA5 => Ok(Opcode::ColLe),
			0xA6 => Ok(Opcode::ColGt),
			0xA7 => Ok(Opcode::ColGe),
			0xA8 => Ok(Opcode::ColEq),
			0xA9 => Ok(Opcode::ColNe),
			0xAA => Ok(Opcode::ColAnd),
			0xAB => Ok(Opcode::ColOr),
			0xAC => Ok(Opcode::ColNot),
			// DML Operations
			0xB0 => Ok(Opcode::InsertRow),
			0xB1 => Ok(Opcode::UpdateRow),
			0xB2 => Ok(Opcode::DeleteRow),
			// DDL Operations
			0xC0 => Ok(Opcode::CreateNamespace),
			0xC1 => Ok(Opcode::CreateTable),
			0xC2 => Ok(Opcode::CreateView),
			0xC3 => Ok(Opcode::CreateIndex),
			0xC4 => Ok(Opcode::CreateSequence),
			0xC5 => Ok(Opcode::CreateRingBuffer),
			0xC6 => Ok(Opcode::CreateDictionary),
			0xC8 => Ok(Opcode::AlterTable),
			0xC9 => Ok(Opcode::AlterSequence),
			0xD0 => Ok(Opcode::DropObject),
			// Control
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
	Distinct = 5,
	Aggregate = 6,
	JoinInner = 7,
	JoinLeft = 8,
	JoinNatural = 9,
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
			5 => Ok(OperatorKind::Distinct),
			6 => Ok(OperatorKind::Aggregate),
			7 => Ok(OperatorKind::JoinInner),
			8 => Ok(OperatorKind::JoinLeft),
			9 => Ok(OperatorKind::JoinNatural),
			_ => Err(value),
		}
	}
}

/// Object type for DropObject opcode
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
	Namespace = 0,
	Table = 1,
	View = 2,
	Index = 3,
	Sequence = 4,
	RingBuffer = 5,
	Dictionary = 6,
}

impl TryFrom<u8> for ObjectType {
	type Error = u8;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(ObjectType::Namespace),
			1 => Ok(ObjectType::Table),
			2 => Ok(ObjectType::View),
			3 => Ok(ObjectType::Index),
			4 => Ok(ObjectType::Sequence),
			5 => Ok(ObjectType::RingBuffer),
			6 => Ok(ObjectType::Dictionary),
			_ => Err(value),
		}
	}
}
