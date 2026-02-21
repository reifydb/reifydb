// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::module::{
	BranchingDepth, FunctionIndex, FunctionTypeIndex, GlobalIndex, LocalIndex, MemoryIndex, TableIndex,
	memory::MemoryOffset,
};

// ---------------------------------------------------------------------------
// MemoryArgument
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryArgument {
	/// The alignment of the memory access, expressed as a power of two.
	///
	/// For example, an alignment of 4 means the memory access is aligned to 2^4 = 16 bytes.
	/// The default alignment is the natural alignment for the type being accessed.
	pub align: u32,

	/// The offset to add to the address before accessing memory.
	///
	/// This is an immediate constant value added to the base address specified by the instruction.
	pub offset: MemoryOffset,
}

// ---------------------------------------------------------------------------
// FunctionType
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct FunctionType {
	pub params: ValueTypes,
	pub results: ValueTypes,
}

impl FunctionType {
	pub fn new(params: ValueTypes, results: ValueTypes) -> Self {
		Self {
			params,
			results,
		}
	}
}

// ---------------------------------------------------------------------------
// ValueType
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
	I32,
	I64,
	F32,
	F64,
	RefExtern,
	RefFunc,
}

impl std::fmt::Display for ValueType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ValueType::I32 => write!(f, "i32"),
			ValueType::I64 => write!(f, "i64"),
			ValueType::F32 => write!(f, "f32"),
			ValueType::F64 => write!(f, "f64"),
			ValueType::RefExtern => write!(f, "extern"),
			ValueType::RefFunc => write!(f, "func"),
		}
	}
}

impl ValueType {
	pub fn to_str(&self) -> &'static str {
		match self {
			ValueType::I32 => "i32",
			ValueType::I64 => "i64",
			ValueType::F32 => "f32",
			ValueType::F64 => "f64",
			ValueType::RefExtern => "extern",
			ValueType::RefFunc => "func",
		}
	}
}

pub type ValueTypes = Box<[ValueType]>;

// ---------------------------------------------------------------------------
// Instruction (compiled form)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Instruction {
	Unreachable,
	Nop,
	Block {
		result_types: Box<[ValueType]>,
		body: Box<[Instruction]>,
	},
	Loop {
		param_types: Box<[ValueType]>,
		result_types: Box<[ValueType]>,
		body: Box<[Instruction]>,
	},
	If {
		result_types: Box<[ValueType]>,
		then: Box<[Instruction]>,
		otherwise: Box<[Instruction]>,
	},
	Else,
	Br(BranchingDepth),
	BrIf(BranchingDepth),
	BrTable {
		cases: Box<[BranchingDepth]>,
		default: BranchingDepth,
	},
	Return,
	Call(FunctionIndex),
	CallIndirect(FunctionTypeIndex, TableIndex),

	Drop,
	Select,

	LocalGet(LocalIndex),
	LocalSet(LocalIndex),
	LocalTee(LocalIndex),

	GlobalGet(GlobalIndex),
	GlobalSet(GlobalIndex),

	I32Load(MemoryArgument),
	I64Load(MemoryArgument),
	F32Load(MemoryArgument),
	F64Load(MemoryArgument),
	I32Load8S(MemoryArgument),
	I32Load8U(MemoryArgument),
	I32Load16S(MemoryArgument),
	I32Load16U(MemoryArgument),
	I64Load8S(MemoryArgument),
	I64Load8U(MemoryArgument),
	I64Load16S(MemoryArgument),
	I64Load16U(MemoryArgument),
	I64Load32S(MemoryArgument),
	I64Load32U(MemoryArgument),
	I32Store(MemoryArgument),
	I64Store(MemoryArgument),
	F32Store(MemoryArgument),
	F64Store(MemoryArgument),
	I32Store8(MemoryArgument),
	I32Store16(MemoryArgument),
	I64Store8(MemoryArgument),
	I64Store16(MemoryArgument),
	I64Store32(MemoryArgument),

	MemorySize(MemoryIndex),
	MemoryGrow(MemoryIndex),

	I32Const(i32),
	I64Const(i64),
	F32Const(f32),
	F64Const(f64),

	I32Eqz,
	I32Eq,
	I32Ne,
	I32LtS,
	I32LtU,
	I32GtS,
	I32GtU,
	I32LeS,
	I32LeU,
	I32GeS,
	I32GeU,
	I64Eqz,
	I64Eq,
	I64Ne,
	I64LtS,
	I64LtU,
	I64GtS,
	I64GtU,
	I64LeS,
	I64LeU,
	I64GeS,
	I64GeU,
	F32Eq,
	F32Ne,
	F32Lt,
	F32Gt,
	F32Le,
	F32Ge,
	F64Eq,
	F64Ne,
	F64Lt,
	F64Gt,
	F64Le,
	F64Ge,

	I32Clz,
	I32Ctz,
	I32Popcnt,
	I32Add,
	I32Sub,
	I32Mul,
	I32DivS,
	I32DivU,
	I32RemS,
	I32RemU,
	I32And,
	I32Or,
	I32Xor,
	I32Shl,
	I32ShrS,
	I32ShrU,
	I32Rotl,
	I32Rotr,
	I64Clz,
	I64Ctz,
	I64Popcnt,
	I64Add,
	I64Sub,
	I64Mul,
	I64DivS,
	I64DivU,
	I64RemS,
	I64RemU,
	I64And,
	I64Or,
	I64Xor,
	I64Shl,
	I64ShrS,
	I64ShrU,
	I64Rotl,
	I64Rotr,
	F32Abs,
	F32Neg,
	F32Ceil,
	F32Floor,
	F32Trunc,
	F32Nearest,
	F32Sqrt,
	F32Add,
	F32Sub,
	F32Mul,
	F32Div,
	F32Min,
	F32Max,
	F32Copysign,
	F64Abs,
	F64Neg,
	F64Ceil,
	F64Floor,
	F64Trunc,
	F64Nearest,
	F64Sqrt,
	F64Add,
	F64Sub,
	F64Mul,
	F64Div,
	F64Min,
	F64Max,
	F64Copysign,

	I32WrapI64,
	I32TruncF32S,
	I32TruncSatF32S,
	I32TruncF32U,
	I32TruncSatF32U,
	I32TruncF64S,
	I32TruncSatF64S,
	I32TruncF64U,
	I32TruncSatF64U,
	I64ExtendI32S,
	I64ExtendI32U,
	I64TruncF32S,
	I64TruncSatF32S,
	I64TruncF32U,
	I64TruncSatF32U,
	I64TruncF64S,
	I64TruncSatF64S,
	I64TruncF64U,
	I64TruncSatF64U,
	F32ConvertI32S,
	F32ConvertI32U,
	F32ConvertI64S,
	F32ConvertI64U,
	F32DemoteF64,
	F64ConvertI32S,
	F64ConvertI32U,
	F64ConvertI64S,
	F64ConvertI64U,
	F64PromoteF32,

	I32ReinterpretF32,
	I64ReinterpretF64,
	F32ReinterpretI32,
	F64ReinterpretI64,

	I32Extend8S,
	I32Extend16S,
	I64Extend8S,
	I64Extend16S,
	I64Extend32S,

	MemoryCopy,
	MemoryFill,
	MemoryInit(u32),
	DataDrop(u32),

	TableGet(TableIndex),
	TableSet(TableIndex),
	TableGrow(TableIndex),
	TableSize(TableIndex),
	TableFill(TableIndex),
	TableCopy(TableIndex, TableIndex),
	TableInit(TableIndex, u32),
	ElemDrop(u32),

	RefNull(ValueType),
	RefIsNull,
	RefFunc(FunctionIndex),
}
