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

	/// Push sort specification onto operand stack
	/// Operand: u16 (sort spec index)
	PushSortSpec = 0x07,

	/// Push extension specification onto operand stack
	/// Operand: u16 (extension spec index)
	PushExtSpec = 0x08,

	// ─────────────────────────────────────────────────────────────
	// Variable Operations
	// ─────────────────────────────────────────────────────────────
	/// Load variable onto operand stack by ID
	/// Operand: u32
	LoadVar = 0x15,

	/// Store operand stack top into variable by ID
	/// Operand: u32
	StoreVar = 0x16,

	/// Update existing variable by ID (searches all scopes)
	/// Operand: u32
	UpdateVar = 0x17,

	/// Load pipeline variable onto pipeline stack by ID
	/// Operand: u32
	LoadPipeline = 0x18,

	/// Store pipeline stack top into variable by ID
	/// Operand: u32
	StorePipeline = 0x19,

	/// Load internal (compiler-generated) variable onto operand stack
	/// Operand: u16 (internal variable ID)
	LoadInternalVar = 0x1A,

	/// Store operand stack top into internal variable
	/// Operand: u16 (internal variable ID)
	StoreInternalVar = 0x1B,

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

	/// Evaluate MAP expressions without input and create a single-row pipeline
	/// Used for MAP {expr} without FROM clause
	/// Pops extension spec from operand stack, pushes pipeline to pipeline stack
	EvalMapWithoutInput = 0x26,

	/// Evaluate EXTEND expressions without input and create a single-row pipeline
	/// Used for EXTEND {expr} without FROM clause
	/// Pops extension spec from operand stack, pushes pipeline to pipeline stack
	EvalExpandWithoutInput = 0x27,

	/// Fetch next batch from active scan
	/// Operand: u16 (source index)
	/// Pushes batch onto pipeline stack, pushes boolean (has_more) onto operand stack
	FetchBatch = 0x28,

	/// Check if query is complete (for early termination)
	/// No operand - pops boolean from operand stack
	/// Used by TAKE and other limiting operators
	CheckComplete = 0x29,

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

	/// Compare two integers: not equal
	/// Pops two Int8 values (b, a), pushes Boolean(a != b)
	IntNe = 0x86,

	/// Compare two integers: less than or equal
	/// Pops two Int8 values (b, a), pushes Boolean(a <= b)
	IntLe = 0x87,

	/// Compare two integers: greater than
	/// Pops two Int8 values (b, a), pushes Boolean(a > b)
	IntGt = 0x88,

	/// Compare two integers: greater than or equal
	/// Pops two Int8 values (b, a), pushes Boolean(a >= b)
	IntGe = 0x89,

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

	/// Drop object
	/// Operand: u16 (definition index), u8 (object type)
	DropObject = 0xD0,

	// ─────────────────────────────────────────────────────────────
	// Subquery Operations
	// ─────────────────────────────────────────────────────────────
	/// Execute subquery and check if result has any rows (EXISTS)
	/// Operand: u16 (subquery index), u8 (negated: 0=exists, 1=not exists)
	/// Pushes Boolean onto operand stack
	ExecSubqueryExists = 0xE0,

	/// Execute subquery and check if value is in result (IN)
	/// Operand: u16 (subquery index), u8 (negated: 0=in, 1=not in)
	/// Pops value from operand stack, pushes Boolean result
	ExecSubqueryIn = 0xE1,

	/// Execute subquery and return scalar result
	/// Operand: u16 (subquery index)
	/// Pushes scalar value onto operand stack (error if >1 row)
	ExecSubqueryScalar = 0xE2,

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
			0x07 => Ok(Opcode::PushSortSpec),
			0x08 => Ok(Opcode::PushExtSpec),
			// Variable Operations (by name)
			0x10 => Ok(Opcode::LoadVar),
			0x11 => Ok(Opcode::StoreVar),
			// Variable Operations (by ID)
			0x15 => Ok(Opcode::LoadVar),
			0x16 => Ok(Opcode::StoreVar),
			0x17 => Ok(Opcode::UpdateVar),
			0x18 => Ok(Opcode::LoadPipeline),
			0x19 => Ok(Opcode::StorePipeline),
			0x1A => Ok(Opcode::LoadInternalVar),
			0x1B => Ok(Opcode::StoreInternalVar),
			// Pipeline Operations
			0x20 => Ok(Opcode::Source),
			0x21 => Ok(Opcode::Inline),
			0x22 => Ok(Opcode::Apply),
			0x23 => Ok(Opcode::Collect),
			0x24 => Ok(Opcode::Merge),
			0x25 => Ok(Opcode::PopPipeline),
			0x26 => Ok(Opcode::EvalMapWithoutInput),
			0x27 => Ok(Opcode::EvalExpandWithoutInput),
			0x28 => Ok(Opcode::FetchBatch),
			0x29 => Ok(Opcode::CheckComplete),
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
			0x86 => Ok(Opcode::IntNe),
			0x87 => Ok(Opcode::IntLe),
			0x88 => Ok(Opcode::IntGt),
			0x89 => Ok(Opcode::IntGe),
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
			0xD0 => Ok(Opcode::DropObject),
			// Subquery Operations
			0xE0 => Ok(Opcode::ExecSubqueryExists),
			0xE1 => Ok(Opcode::ExecSubqueryIn),
			0xE2 => Ok(Opcode::ExecSubqueryScalar),
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
	Map = 3,
	Take = 4,
	Sort = 5,
	Distinct = 6,
	Aggregate = 7,
	JoinInner = 8,
	JoinLeft = 9,
	JoinNatural = 10,
}

impl TryFrom<u8> for OperatorKind {
	type Error = u8;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(OperatorKind::Filter),
			1 => Ok(OperatorKind::Select),
			2 => Ok(OperatorKind::Extend),
			3 => Ok(OperatorKind::Map),
			4 => Ok(OperatorKind::Take),
			5 => Ok(OperatorKind::Sort),
			6 => Ok(OperatorKind::Distinct),
			7 => Ok(OperatorKind::Aggregate),
			8 => Ok(OperatorKind::JoinInner),
			9 => Ok(OperatorKind::JoinLeft),
			10 => Ok(OperatorKind::JoinNatural),
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
