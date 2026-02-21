// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod binary;
pub mod spec;
pub mod text;
pub mod validate;

pub use binary::WasmParser;

use crate::util::{byte_reader, leb128::Leb128Error};

// ---------------------------------------------------------------------------
// Result type alias
// ---------------------------------------------------------------------------

pub type Result<T> = core::result::Result<T, WasmParseError>;

// ---------------------------------------------------------------------------
// WasmParseError
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum WasmParseError {
	InvalidMagicNumber,
	UnsupportedVersion(u32),
	UnexpectedEndOfFile,
	InvalidLEB128Encoding,
	InvalidSectionCode(u8),
	OutOfBounds,
	InvalidValueType(u8),
	InvalidElementMode(u8),
	InvalidDataMode(u8),
	InvalidImportDescriptor(u8),
	InvalidExportDescriptor(u8),
	InvalidOpcode(u8),
	InvalidOpcodeExtension(u8, u32),
	InvalidMutability(u8),
	InvalidGlobalInit(WasmInstruction),
	UnsupportedOpcode(Opcode),
	NotAnInstruction(Opcode),
}

impl From<byte_reader::Error> for WasmParseError {
	fn from(value: byte_reader::Error) -> Self {
		match value {
			byte_reader::Error::OutOfBounds => WasmParseError::OutOfBounds,
			byte_reader::Error::UnexpectedEndOfFile => WasmParseError::UnexpectedEndOfFile,
			byte_reader::Error::InvalidLEB128Encoding => WasmParseError::InvalidLEB128Encoding,
		}
	}
}

impl core::fmt::Display for WasmParseError {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match self {
			WasmParseError::InvalidMagicNumber => write!(f, "Invalid magic number"),
			WasmParseError::UnsupportedVersion(version) => {
				write!(f, "Unsupported version: {}", version)
			}
			WasmParseError::UnexpectedEndOfFile => write!(f, "Unexpected end of file"),
			WasmParseError::OutOfBounds => write!(f, "Index out of bounds"),
			WasmParseError::InvalidLEB128Encoding => write!(f, "Invalid encoding"),
			WasmParseError::InvalidSectionCode(code) => {
				write!(f, "Invalid section code {}", code)
			}
			WasmParseError::InvalidValueType(value_type) => {
				write!(f, "Invalid value types: {}", value_type)
			}
			WasmParseError::InvalidElementMode(mode) => {
				write!(f, "Invalid element mode: {}", mode)
			}
			WasmParseError::InvalidDataMode(mode) => {
				write!(f, "Invalid data mode: {}", mode)
			}
			WasmParseError::InvalidImportDescriptor(descriptor) => {
				write!(f, "Invalid import descriptor: {}", descriptor)
			}
			WasmParseError::InvalidExportDescriptor(descriptor) => {
				write!(f, "Invalid export descriptor: {}", descriptor)
			}
			WasmParseError::InvalidOpcode(opcode) => write!(f, "Invalid opcode: {}", opcode),
			WasmParseError::InvalidOpcodeExtension(opcode, extension) => {
				write!(f, "Invalid extension of opcode {}: {}", opcode, extension)
			}
			WasmParseError::NotAnInstruction(opcode) => {
				write!(f, "Opcode is not an instruction: {:?}", opcode)
			}
			WasmParseError::UnsupportedOpcode(opcode) => {
				write!(f, "Unsupported opcode: {:?}", opcode)
			}
			WasmParseError::InvalidMutability(value) => {
				write!(f, "Invalid mutability: {}", value)
			}
			WasmParseError::InvalidGlobalInit(instruction) => {
				write!(f, "Invalid global init: {:?}", instruction)
			}
		}
	}
}

impl From<Leb128Error> for WasmParseError {
	fn from(e: Leb128Error) -> Self {
		match e {
			Leb128Error::InvalidEncoding => WasmParseError::InvalidLEB128Encoding,
			Leb128Error::IncompleteEncoding => WasmParseError::UnexpectedEndOfFile,
		}
	}
}

// ---------------------------------------------------------------------------
// WasmModule
// ---------------------------------------------------------------------------

/// Represents a complete WebAssembly module, containing all standard sections.
#[derive(Debug, PartialEq)]
pub struct WasmModule {
	/// The magic number identifying the file as a WebAssembly module (`"\0asm"`).
	pub magic: Box<[u8]>,

	/// The version of the WebAssembly module (usually `0x1` for current modules).
	pub version: u32,

	/// A boxed slice of custom sections, which can contain arbitrary data.
	pub customs: Box<[WasmCustom]>,

	/// A boxed slice of function signatures.
	pub types: Box<[WasmFunc]>,

	/// A boxed slice of imports (functions, tables, memories, globals).
	pub imports: Box<[WasmImport]>,

	/// A boxed slice of function indices, each referring to a function signature in the types section.
	pub functions: Box<[u32]>,

	/// A boxed slice of globals, each referring to a variable accessible in the module.
	pub globals: Box<[WasmGlobal]>,

	/// A boxed slice of table types, specifying the types of elements in the table and its limits.
	pub tables: Box<[WasmTable]>,

	/// A boxed slice of memory types, each defining the limits for the memory.
	pub memories: Box<[WasmMemory]>,

	/// A boxed slice of exports, each with a name and description of what is being exported.
	pub exports: Box<[WasmExport]>,

	/// The index of the function to be called as the start function.
	pub start_function: Option<u32>,

	/// A boxed slice of elements, each with a table index, offset, and initialization data.
	pub elements: Box<[WasmElement]>,

	/// A boxed slice of function bodies, each containing local variable declarations and code.
	pub codes: Box<[WasmFunctionBody]>,

	/// A boxed slice of data segments, each with a memory index, offset, and data.
	pub data: Box<[WasmData]>,
}

// ---------------------------------------------------------------------------
// WasmCustom
// ---------------------------------------------------------------------------

/// Represents a custom section in the Wasm module, containing arbitrary data.
#[derive(Debug, PartialEq)]
pub struct WasmCustom {
	/// The name of the custom section.
	pub name: String,

	/// The raw data of the custom section.
	pub data: Box<[u8]>,
}

// ---------------------------------------------------------------------------
// WasmFunc
// ---------------------------------------------------------------------------

/// Represents a function signature, defining the parameter and return types.
#[derive(Debug, Default, PartialEq)]
pub struct WasmFunc {
	/// A boxed slice of parameter types.
	pub params: Box<[WasmValueType]>,

	/// A boxed slice of result types.
	pub results: Box<[WasmValueType]>,
}

// ---------------------------------------------------------------------------
// WasmImport
// ---------------------------------------------------------------------------

/// Represents an import, specifying a module, name, and description of the imported item.
#[derive(Debug, PartialEq)]
pub struct WasmImport {
	/// The module from which the item is imported.
	pub module: Box<[u8]>,

	/// The name of the item being imported.
	pub name: Box<[u8]>,

	/// A description of the imported item (function, table, memory, or global).
	pub desc: WasmImportDescriptor,
}

// ---------------------------------------------------------------------------
// WasmImportDescriptor
// ---------------------------------------------------------------------------

/// Describes the types of an import (function, table, memory, or global).
#[derive(Debug, PartialEq)]
pub enum WasmImportDescriptor {
	/// Import a function with the given types index.
	Function(u32),

	/// Import a table with the given table types.
	Table(WasmTable),

	/// Import a memory with the given memory types.
	Memory(WasmMemory),

	/// Import a global.
	Global(WasmGlobalType),
}

// ---------------------------------------------------------------------------
// WasmTable
// ---------------------------------------------------------------------------

/// Represents the types of a table, specifying the types of elements and limits on the table size.
#[derive(Debug, PartialEq)]
pub struct WasmTable {
	/// The types of elements in the table.
	pub element_types: WasmValueType,

	/// The limits on the table's size.
	pub limits: WasmResizableLimit,
}

// ---------------------------------------------------------------------------
// WasmMemory
// ---------------------------------------------------------------------------

/// Represents the types of a memory, specifying the limits on its size.
#[derive(Debug, PartialEq)]
pub struct WasmMemory {
	/// The limits on the memory's size.
	pub limits: WasmResizableLimit,
}

// ---------------------------------------------------------------------------
// WasmExport
// ---------------------------------------------------------------------------

/// Represents an export, specifying the name and description of what is being exported.
#[derive(Debug, PartialEq)]
pub struct WasmExport {
	/// The name of the exported item.
	pub name: Box<[u8]>,

	/// A description of the exported item (function, table, memory, or global).
	pub desc: WasmExportDescriptor,
}

// ---------------------------------------------------------------------------
// WasmExportDescriptor
// ---------------------------------------------------------------------------

/// Describes the types of an export (function, table, memory, or global).
#[derive(Debug, PartialEq)]
pub enum WasmExportDescriptor {
	/// Export a function with the given index.
	Func(u32),

	/// Export a table with the given index.
	Table(u32),

	/// Export a memory with the given index.
	Memory(u32),

	/// Export a global with the given index.
	Global(u32),
}

// ---------------------------------------------------------------------------
// WasmElement
// ---------------------------------------------------------------------------

/// Represents an element in the element section, which is used to initialize tables.
#[derive(Debug, PartialEq)]
pub struct WasmElement {
	pub mode: WasmElementMode,
	/// The list of function indices to place in the table.
	pub init: Box<[u32]>,
}

#[derive(Debug, PartialEq)]
pub enum WasmElementMode {
	/// The element segment is passive.
	Passive,
	/// The element segment is active.
	Active {
		/// The index of the table being initialized.
		table: u32,
		/// The initial expression of the element segment.
		offset: Box<[WasmInstruction]>,
	},
	/// The element segment is declared.
	Declarative,
}

// ---------------------------------------------------------------------------
// WasmFunctionBody
// ---------------------------------------------------------------------------

/// Represents a function body in the code section, including local variable declarations and code.
#[derive(Debug, PartialEq)]
pub struct WasmFunctionBody {
	/// A boxed slice of local variable declarations (count and types).
	pub locals: Box<[(u32, WasmValueType)]>,

	/// The instructions (opcodes) that make up the function body.
	pub code: Box<[WasmInstruction]>,
}

// ---------------------------------------------------------------------------
// WasmData
// ---------------------------------------------------------------------------

/// Represents a data segment in the data section, which initializes a portion of memory.
#[derive(Debug, PartialEq)]
pub struct WasmData {
	/// The mode of this data segment.
	pub mode: WasmDataMode,

	/// The raw data to be placed in the memory.
	pub data: Box<[u8]>,
}

/// The mode of a data segment.
#[derive(Debug, PartialEq)]
pub enum WasmDataMode {
	/// Active data segment — copied into memory during instantiation.
	Active {
		/// The index of the memory to initialize.
		index: u32,
		/// The offset in the memory where the data begins.
		offset: u32,
	},
	/// Passive data segment — only used by memory.init.
	Passive,
}

// ---------------------------------------------------------------------------
// WasmResizableLimit
// ---------------------------------------------------------------------------

/// Represents the limits on a resizable item (table or memory).
#[derive(Debug, PartialEq)]
pub struct WasmResizableLimit {
	/// The minimum size of the table or memory.
	pub min: u32,

	/// The maximum size of the table or memory (optional).
	pub max: Option<u32>,
}

// ---------------------------------------------------------------------------
// WasmGlobal
// ---------------------------------------------------------------------------

/// Global type descriptor (used in imports where there is no init expression).
#[derive(Debug, PartialEq)]
pub struct WasmGlobalType {
	pub value_type: WasmValueType,
	pub mutable: bool,
}

#[derive(Debug, PartialEq)]
pub struct WasmGlobal {
	/// The type of the global (e.g., i32, i64, f32, f64).
	pub value_type: WasmValueType,
	/// Whether the global is mutable.
	pub mutable: bool,
	/// The initialization value of the global, which could be a constant or another global's value.
	pub init: WasmGlobalInit,
}

// ---------------------------------------------------------------------------
// WasmGlobalInit
// ---------------------------------------------------------------------------

/// Represents an initialization value for a WebAssembly global.
#[derive(Debug, PartialEq)]
pub enum WasmGlobalInit {
	/// Initialize with a 32-bit integer constant.
	I32(i32),
	/// Initialize with a 64-bit integer constant.
	I64(i64),
	/// Initialize with a 32-bit floating-point constant.
	F32(f32),
	/// Initialize with a 64-bit floating-point constant.
	F64(f64),
	/// Initialize with the value of another global, referenced by its index.
	Global(u32),
	/// Initialize with a null reference of the specified value type.
	NullRef(WasmValueType),
	/// Initialize with a reference to a function, specified by its index.
	FuncRef(u32),
}

// ---------------------------------------------------------------------------
// WasmValueType
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Clone)]
pub enum WasmValueType {
	I32,
	I64,
	F32,
	F64,
	FuncRef,
	ExternRef,
}

impl TryFrom<u8> for WasmValueType {
	type Error = u8;
	fn try_from(value: u8) -> core::result::Result<Self, Self::Error> {
		match value {
			0x7F => Ok(WasmValueType::I32),
			0x7E => Ok(WasmValueType::I64),
			0x7D => Ok(WasmValueType::F32),
			0x7C => Ok(WasmValueType::F64),
			0x70 => Ok(WasmValueType::FuncRef),
			0x6F => Ok(WasmValueType::ExternRef),
			_ => Err(value),
		}
	}
}

// ---------------------------------------------------------------------------
// WasmValue
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum WasmValue {
	I32(i32),
	I64(i64),
	F32(f32),
	F64(f64),
	RefFunc(u32),
	RefExtern(u32),
	RefNull(u32),
}

// ---------------------------------------------------------------------------
// WasmMemoryArgument
// ---------------------------------------------------------------------------

/// Represents a memory argument (`WasmMemoryArgument`) in WebAssembly instructions.
///
/// `WasmMemoryArgument` is used in load and store instructions to specify the memory offset
/// and alignment.
#[derive(Clone, Debug, PartialEq)]
pub struct WasmMemoryArgument {
	/// The offset to add to the address before accessing memory.
	///
	/// This is an immediate constant value added to the base address specified by the instruction.
	pub offset: u32,

	/// The alignment of the memory access, expressed as a power of two.
	///
	/// For example, an alignment of 4 means the memory access is aligned to 2^4 = 16 bytes.
	/// The default alignment is the natural alignment for the type being accessed.
	pub align: u32,
}

// ---------------------------------------------------------------------------
// WasmResultType
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum WasmResultType {
	None,
	FromValue(WasmValueType),
	FromType(u32),
}

// ---------------------------------------------------------------------------
// WasmInstruction
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum WasmInstruction {
	Unreachable,
	Nop,
	Block {
		result_type: WasmResultType,
		body: Box<[WasmInstruction]>,
	},
	Loop {
		result_type: WasmResultType,
		body: Box<[WasmInstruction]>,
	},
	If {
		result_type: WasmResultType,
		then: Box<[WasmInstruction]>,
		otherwise: Box<[WasmInstruction]>,
	},
	Else,
	Br(u32),
	BrIf(u32),
	BrTable {
		cases: Box<[u32]>,
		default: u32,
	},
	Return,
	Call(u32),
	CallIndirect(u32, u32),

	Drop,
	Select,

	LocalGet(u32),
	LocalSet(u32),
	LocalTee(u32),

	GlobalGet(u32),
	GlobalSet(u32),

	I32Load(WasmMemoryArgument),
	I64Load(WasmMemoryArgument),
	F32Load(WasmMemoryArgument),
	F64Load(WasmMemoryArgument),
	I32Load8S(WasmMemoryArgument),
	I32Load8U(WasmMemoryArgument),
	I32Load16S(WasmMemoryArgument),
	I32Load16U(WasmMemoryArgument),
	I64Load8S(WasmMemoryArgument),
	I64Load8U(WasmMemoryArgument),
	I64Load16S(WasmMemoryArgument),
	I64Load16U(WasmMemoryArgument),
	I64Load32S(WasmMemoryArgument),
	I64Load32U(WasmMemoryArgument),
	I32Store(WasmMemoryArgument),
	I64Store(WasmMemoryArgument),
	F32Store(WasmMemoryArgument),
	F64Store(WasmMemoryArgument),
	I32Store8(WasmMemoryArgument),
	I32Store16(WasmMemoryArgument),
	I64Store8(WasmMemoryArgument),
	I64Store16(WasmMemoryArgument),
	I64Store32(WasmMemoryArgument),

	MemorySize(u32),
	MemoryGrow(u32),

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

	TableGet(u32),
	TableSet(u32),
	TableGrow(u32),
	TableSize(u32),
	TableFill(u32),
	TableCopy(u32, u32),
	TableInit(u32, u32),
	ElemDrop(u32),

	RefNull(WasmValueType),
	RefIsNull,
	RefFunc(u32),
}

// ---------------------------------------------------------------------------
// Opcode
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum Opcode {
	Unreachable = 0x00,
	Nop = 0x01,
	Block = 0x02,
	Loop = 0x03,
	If = 0x04,
	Else = 0x05,
	End = 0x0B,
	Br = 0x0C,
	BrIf = 0x0D,
	BrTable = 0x0E,
	Return = 0x0F,
	Call = 0x10,
	CallIndirect = 0x11,
	Drop = 0x1A,
	Select = 0x1B,
	SelectT = 0x1C,
	LocalGet = 0x20,
	LocalSet = 0x21,
	LocalTee = 0x22,
	GlobalGet = 0x23,
	GlobalSet = 0x24,
	TableGet = 0x25,
	TableSet = 0x26,
	I32Load = 0x28,
	I64Load = 0x29,
	F32Load = 0x2A,
	F64Load = 0x2B,
	I32Load8S = 0x2C,
	I32Load8U = 0x2D,
	I32Load16S = 0x2E,
	I32Load16U = 0x2F,
	I64Load8S = 0x30,
	I64Load8U = 0x31,
	I64Load16S = 0x32,
	I64Load16U = 0x33,
	I64Load32S = 0x34,
	I64Load32U = 0x35,
	I32Store = 0x36,
	I64Store = 0x37,
	F32Store = 0x38,
	F64Store = 0x39,
	I32Store8 = 0x3A,
	I32Store16 = 0x3B,
	I64Store8 = 0x3C,
	I64Store16 = 0x3D,
	I64Store32 = 0x3E,
	MemorySize = 0x3F,
	MemoryGrow = 0x40,
	I32Const = 0x41,
	I64Const = 0x42,
	F32Const = 0x43,
	F64Const = 0x44,
	I32Eqz = 0x45,
	I32Eq = 0x46,
	I32Ne = 0x47,
	I32LtS = 0x48,
	I32LtU = 0x49,
	I32GtS = 0x4A,
	I32GtU = 0x4B,
	I32LeS = 0x4C,
	I32LeU = 0x4D,
	I32GeS = 0x4E,
	I32GeU = 0x4F,
	I64Eqz = 0x50,
	I64Eq = 0x51,
	I64Ne = 0x52,
	I64LtS = 0x53,
	I64LtU = 0x54,
	I64GtS = 0x55,
	I64GtU = 0x56,
	I64LeS = 0x57,
	I64LeU = 0x58,
	I64GeS = 0x59,
	I64GeU = 0x5A,
	F32Eq = 0x5B,
	F32Ne = 0x5C,
	F32Lt = 0x5D,
	F32Gt = 0x5E,
	F32Le = 0x5F,
	F32Ge = 0x60,
	F64Eq = 0x61,
	F64Ne = 0x62,
	F64Lt = 0x63,
	F64Gt = 0x64,
	F64Le = 0x65,
	F64Ge = 0x66,
	I32Clz = 0x67,
	I32Ctz = 0x68,
	I32Popcnt = 0x69,
	I32Add = 0x6A,
	I32Sub = 0x6B,
	I32Mul = 0x6C,
	I32DivS = 0x6D,
	I32DivU = 0x6E,
	I32RemS = 0x6F,
	I32RemU = 0x70,
	I32And = 0x71,
	I32Or = 0x72,
	I32Xor = 0x73,
	I32Shl = 0x74,
	I32ShrS = 0x75,
	I32ShrU = 0x76,
	I32Rotl = 0x77,
	I32Rotr = 0x78,
	I64Clz = 0x79,
	I64Ctz = 0x7A,
	I64Popcnt = 0x7B,
	I64Add = 0x7C,
	I64Sub = 0x7D,
	I64Mul = 0x7E,
	I64DivS = 0x7F,
	I64DivU = 0x80,
	I64RemS = 0x81,
	I64RemU = 0x82,
	I64And = 0x83,
	I64Or = 0x84,
	I64Xor = 0x85,
	I64Shl = 0x86,
	I64ShrS = 0x87,
	I64ShrU = 0x88,
	I64Rotl = 0x89,
	I64Rotr = 0x8A,
	F32Abs = 0x8B,
	F32Neg = 0x8C,
	F32Ceil = 0x8D,
	F32Floor = 0x8E,
	F32Trunc = 0x8F,
	F32Nearest = 0x90,
	F32Sqrt = 0x91,
	F32Add = 0x92,
	F32Sub = 0x93,
	F32Mul = 0x94,
	F32Div = 0x95,
	F32Min = 0x96,
	F32Max = 0x97,
	F32Copysign = 0x98,
	F64Abs = 0x99,
	F64Neg = 0x9A,
	F64Ceil = 0x9B,
	F64Floor = 0x9C,
	F64Trunc = 0x9D,
	F64Nearest = 0x9E,
	F64Sqrt = 0x9F,
	F64Add = 0xA0,
	F64Sub = 0xA1,
	F64Mul = 0xA2,
	F64Div = 0xA3,
	F64Min = 0xA4,
	F64Max = 0xA5,
	F64Copysign = 0xA6,
	I32WrapI64 = 0xA7,
	I32TruncF32S = 0xA8,
	I32TruncSatF32S = 0xFC_00,
	I32TruncF32U = 0xA9,
	I32TruncSatF32U = 0xFC_01,
	I32TruncF64S = 0xAA,
	I32TruncSatF64S = 0xFC_02,
	I32TruncF64U = 0xAB,
	I32TruncSatF64U = 0xFC_03,

	I64TruncF32S = 0xAE,
	I64TruncSatF32S = 0xFC_04,
	I64TruncF32U = 0xAF,
	I64TruncSatF32U = 0xFC_05,
	I64TruncF64S = 0xB0,
	I64TruncSatF64S = 0xFC_06,
	I64TruncF64U = 0xB1,
	I64TruncSatF64U = 0xFC_07,

	I64ExtendI32S = 0xAC,
	I64ExtendI32U = 0xAD,
	F32ConvertI32S = 0xB2,
	F32ConvertI32U = 0xB3,
	F32ConvertI64S = 0xB4,
	F32ConvertI64U = 0xB5,
	F32DemoteF64 = 0xB6,
	F64ConvertI32S = 0xB7,
	F64ConvertI32U = 0xB8,
	F64ConvertI64S = 0xB9,
	F64ConvertI64U = 0xBA,
	F64PromoteF32 = 0xBB,
	I32ReinterpretF32 = 0xBC,
	I64ReinterpretF64 = 0xBD,
	F32ReinterpretI32 = 0xBE,
	F64ReinterpretI64 = 0xBF,
	I32Extend8S = 0xC0,
	I32Extend16S = 0xC1,
	I64Extend8S = 0xC2,
	I64Extend16S = 0xC3,
	I64Extend32S = 0xC4,
	NullRef = 0xD0,
	RefIsNull = 0xD1,
	FuncRef = 0xD2,
	MemoryInit = 0xFC_08,
	DataDrop = 0xFC_09,
	MemoryCopy = 0xFC_0A,
	MemoryFill = 0xFC_0B,
	TableInit = 0xFC_0C,
	ElemDrop = 0xFC_0D,
	TableCopy = 0xFC_0E,
	TableGrow = 0xFC_0F,
	TableSize = 0xFC_10,
	TableFill = 0xFC_11,
	LoadV128 = 0xFD_00,
	StoreV128 = 0xFD_0B,
	SplatI8x16 = 0xFD_0C,
	SplatI16x8 = 0xFD_0D,
	SplatI32x4 = 0xFD_0E,
	SplatI64x2 = 0xFD_0F,
	SplatF32x4 = 0xFD_10,
	SplatF64x2 = 0xFD_11,

	ExtractLaneSI8x16 = 0xFD_12,
	ExtractLaneUI8x16 = 0xFD_13,
	ExtractLaneSI16x8 = 0xFD_14,
	ExtractLaneUI16x8 = 0xFD_15,
	ExtractLaneI32x4 = 0xFD_16,
	ExtractLaneI64x2 = 0xFD_17,
	ExtractLaneF32x4 = 0xFD_18,
	ExtractLaneF64x2 = 0xFD_19,
	ReplaceLaneI8x16 = 0xFD_1A,
	ReplaceLaneI16x8 = 0xFD_1B,
	ReplaceLaneI32x4 = 0xFD_1C,
	ReplaceLaneI64x2 = 0xFD_1D,
	ReplaceLaneF32x4 = 0xFD_1E,
	ReplaceLaneF64x2 = 0xFD_1F,
}

impl Opcode {
	pub(crate) fn from_u8(value: u8, extension: u32) -> Result<Self> {
		match value {
			0x00 => Ok(Opcode::Unreachable),
			0x01 => Ok(Opcode::Nop),
			0x02 => Ok(Opcode::Block),
			0x03 => Ok(Opcode::Loop),
			0x04 => Ok(Opcode::If),
			0x05 => Ok(Opcode::Else),
			0x0B => Ok(Opcode::End),
			0x0C => Ok(Opcode::Br),
			0x0D => Ok(Opcode::BrIf),
			0x0E => Ok(Opcode::BrTable),
			0x0F => Ok(Opcode::Return),
			0x10 => Ok(Opcode::Call),
			0x11 => Ok(Opcode::CallIndirect),
			0x1A => Ok(Opcode::Drop),
			0x1B => Ok(Opcode::Select),
			0x1C => Ok(Opcode::SelectT),
			0x20 => Ok(Opcode::LocalGet),
			0x21 => Ok(Opcode::LocalSet),
			0x22 => Ok(Opcode::LocalTee),
			0x23 => Ok(Opcode::GlobalGet),
			0x24 => Ok(Opcode::GlobalSet),
			0x25 => Ok(Opcode::TableGet),
			0x26 => Ok(Opcode::TableSet),
			0x28 => Ok(Opcode::I32Load),
			0x29 => Ok(Opcode::I64Load),
			0x2A => Ok(Opcode::F32Load),
			0x2B => Ok(Opcode::F64Load),
			0x2C => Ok(Opcode::I32Load8S),
			0x2D => Ok(Opcode::I32Load8U),
			0x2E => Ok(Opcode::I32Load16S),
			0x2F => Ok(Opcode::I32Load16U),
			0x30 => Ok(Opcode::I64Load8S),
			0x31 => Ok(Opcode::I64Load8U),
			0x32 => Ok(Opcode::I64Load16S),
			0x33 => Ok(Opcode::I64Load16U),
			0x34 => Ok(Opcode::I64Load32S),
			0x35 => Ok(Opcode::I64Load32U),
			0x36 => Ok(Opcode::I32Store),
			0x37 => Ok(Opcode::I64Store),
			0x38 => Ok(Opcode::F32Store),
			0x39 => Ok(Opcode::F64Store),
			0x3A => Ok(Opcode::I32Store8),
			0x3B => Ok(Opcode::I32Store16),
			0x3C => Ok(Opcode::I64Store8),
			0x3D => Ok(Opcode::I64Store16),
			0x3E => Ok(Opcode::I64Store32),
			0x3F => Ok(Opcode::MemorySize),
			0x40 => Ok(Opcode::MemoryGrow),
			0x41 => Ok(Opcode::I32Const),
			0x42 => Ok(Opcode::I64Const),
			0x43 => Ok(Opcode::F32Const),
			0x44 => Ok(Opcode::F64Const),
			0x45 => Ok(Opcode::I32Eqz),
			0x46 => Ok(Opcode::I32Eq),
			0x47 => Ok(Opcode::I32Ne),
			0x48 => Ok(Opcode::I32LtS),
			0x49 => Ok(Opcode::I32LtU),
			0x4A => Ok(Opcode::I32GtS),
			0x4B => Ok(Opcode::I32GtU),
			0x4C => Ok(Opcode::I32LeS),
			0x4D => Ok(Opcode::I32LeU),
			0x4E => Ok(Opcode::I32GeS),
			0x4F => Ok(Opcode::I32GeU),
			0x50 => Ok(Opcode::I64Eqz),
			0x51 => Ok(Opcode::I64Eq),
			0x52 => Ok(Opcode::I64Ne),
			0x53 => Ok(Opcode::I64LtS),
			0x54 => Ok(Opcode::I64LtU),
			0x55 => Ok(Opcode::I64GtS),
			0x56 => Ok(Opcode::I64GtU),
			0x57 => Ok(Opcode::I64LeS),
			0x58 => Ok(Opcode::I64LeU),
			0x59 => Ok(Opcode::I64GeS),
			0x5A => Ok(Opcode::I64GeU),
			0x5B => Ok(Opcode::F32Eq),
			0x5C => Ok(Opcode::F32Ne),
			0x5D => Ok(Opcode::F32Lt),
			0x5E => Ok(Opcode::F32Gt),
			0x5F => Ok(Opcode::F32Le),
			0x60 => Ok(Opcode::F32Ge),
			0x61 => Ok(Opcode::F64Eq),
			0x62 => Ok(Opcode::F64Ne),
			0x63 => Ok(Opcode::F64Lt),
			0x64 => Ok(Opcode::F64Gt),
			0x65 => Ok(Opcode::F64Le),
			0x66 => Ok(Opcode::F64Ge),
			0x67 => Ok(Opcode::I32Clz),
			0x68 => Ok(Opcode::I32Ctz),
			0x69 => Ok(Opcode::I32Popcnt),
			0x6A => Ok(Opcode::I32Add),
			0x6B => Ok(Opcode::I32Sub),
			0x6C => Ok(Opcode::I32Mul),
			0x6D => Ok(Opcode::I32DivS),
			0x6E => Ok(Opcode::I32DivU),
			0x6F => Ok(Opcode::I32RemS),
			0x70 => Ok(Opcode::I32RemU),
			0x71 => Ok(Opcode::I32And),
			0x72 => Ok(Opcode::I32Or),
			0x73 => Ok(Opcode::I32Xor),
			0x74 => Ok(Opcode::I32Shl),
			0x75 => Ok(Opcode::I32ShrS),
			0x76 => Ok(Opcode::I32ShrU),
			0x77 => Ok(Opcode::I32Rotl),
			0x78 => Ok(Opcode::I32Rotr),
			0x79 => Ok(Opcode::I64Clz),
			0x7A => Ok(Opcode::I64Ctz),
			0x7B => Ok(Opcode::I64Popcnt),
			0x7C => Ok(Opcode::I64Add),
			0x7D => Ok(Opcode::I64Sub),
			0x7E => Ok(Opcode::I64Mul),
			0x7F => Ok(Opcode::I64DivS),
			0x80 => Ok(Opcode::I64DivU),
			0x81 => Ok(Opcode::I64RemS),
			0x82 => Ok(Opcode::I64RemU),
			0x83 => Ok(Opcode::I64And),
			0x84 => Ok(Opcode::I64Or),
			0x85 => Ok(Opcode::I64Xor),
			0x86 => Ok(Opcode::I64Shl),
			0x87 => Ok(Opcode::I64ShrS),
			0x88 => Ok(Opcode::I64ShrU),
			0x89 => Ok(Opcode::I64Rotl),
			0x8A => Ok(Opcode::I64Rotr),
			0x8B => Ok(Opcode::F32Abs),
			0x8C => Ok(Opcode::F32Neg),
			0x8D => Ok(Opcode::F32Ceil),
			0x8E => Ok(Opcode::F32Floor),
			0x8F => Ok(Opcode::F32Trunc),
			0x90 => Ok(Opcode::F32Nearest),
			0x91 => Ok(Opcode::F32Sqrt),
			0x92 => Ok(Opcode::F32Add),
			0x93 => Ok(Opcode::F32Sub),
			0x94 => Ok(Opcode::F32Mul),
			0x95 => Ok(Opcode::F32Div),
			0x96 => Ok(Opcode::F32Min),
			0x97 => Ok(Opcode::F32Max),
			0x98 => Ok(Opcode::F32Copysign),
			0x99 => Ok(Opcode::F64Abs),
			0x9A => Ok(Opcode::F64Neg),
			0x9B => Ok(Opcode::F64Ceil),
			0x9C => Ok(Opcode::F64Floor),
			0x9D => Ok(Opcode::F64Trunc),
			0x9E => Ok(Opcode::F64Nearest),
			0x9F => Ok(Opcode::F64Sqrt),
			0xA0 => Ok(Opcode::F64Add),
			0xA1 => Ok(Opcode::F64Sub),
			0xA2 => Ok(Opcode::F64Mul),
			0xA3 => Ok(Opcode::F64Div),
			0xA4 => Ok(Opcode::F64Min),
			0xA5 => Ok(Opcode::F64Max),
			0xA6 => Ok(Opcode::F64Copysign),
			0xA7 => Ok(Opcode::I32WrapI64),
			0xA8 => Ok(Opcode::I32TruncF32S),
			0xA9 => Ok(Opcode::I32TruncF32U),
			0xAA => Ok(Opcode::I32TruncF64S),
			0xAB => Ok(Opcode::I32TruncF64U),
			0xAC => Ok(Opcode::I64ExtendI32S),
			0xAD => Ok(Opcode::I64ExtendI32U),
			0xAE => Ok(Opcode::I64TruncF32S),
			0xAF => Ok(Opcode::I64TruncF32U),
			0xB0 => Ok(Opcode::I64TruncF64S),
			0xB1 => Ok(Opcode::I64TruncF64U),
			0xB2 => Ok(Opcode::F32ConvertI32S),
			0xB3 => Ok(Opcode::F32ConvertI32U),
			0xB4 => Ok(Opcode::F32ConvertI64S),
			0xB5 => Ok(Opcode::F32ConvertI64U),
			0xB6 => Ok(Opcode::F32DemoteF64),
			0xB7 => Ok(Opcode::F64ConvertI32S),
			0xB8 => Ok(Opcode::F64ConvertI32U),
			0xB9 => Ok(Opcode::F64ConvertI64S),
			0xBA => Ok(Opcode::F64ConvertI64U),
			0xBB => Ok(Opcode::F64PromoteF32),
			0xBC => Ok(Opcode::I32ReinterpretF32),
			0xBD => Ok(Opcode::I64ReinterpretF64),
			0xBE => Ok(Opcode::F32ReinterpretI32),
			0xBF => Ok(Opcode::F64ReinterpretI64),
			0xC0 => Ok(Opcode::I32Extend8S),
			0xC1 => Ok(Opcode::I32Extend16S),
			0xC2 => Ok(Opcode::I64Extend8S),
			0xC3 => Ok(Opcode::I64Extend16S),
			0xC4 => Ok(Opcode::I64Extend32S),
			0xD0 => Ok(Opcode::NullRef),
			0xD1 => Ok(Opcode::RefIsNull),
			0xD2 => Ok(Opcode::FuncRef),
			0xFC => match extension {
				0x00 => Ok(Opcode::I32TruncSatF32S),
				0x01 => Ok(Opcode::I32TruncSatF32U),
				0x02 => Ok(Opcode::I32TruncSatF64S),
				0x03 => Ok(Opcode::I32TruncSatF64U),
				0x04 => Ok(Opcode::I64TruncSatF32S),
				0x05 => Ok(Opcode::I64TruncSatF32U),
				0x06 => Ok(Opcode::I64TruncSatF64S),
				0x07 => Ok(Opcode::I64TruncSatF64U),
				0x08 => Ok(Opcode::MemoryInit),
				0x09 => Ok(Opcode::DataDrop),
				0x0A => Ok(Opcode::MemoryCopy),
				0x0B => Ok(Opcode::MemoryFill),
				0x0C => Ok(Opcode::TableInit),
				0x0D => Ok(Opcode::ElemDrop),
				0x0E => Ok(Opcode::TableCopy),
				0x0F => Ok(Opcode::TableGrow),
				0x10 => Ok(Opcode::TableSize),
				0x11 => Ok(Opcode::TableFill),
				_ => Err(WasmParseError::InvalidOpcodeExtension(value, extension)),
			},
			0xFD => match extension {
				0x00 => Ok(Opcode::LoadV128),
				0x0B => Ok(Opcode::StoreV128),
				0x0C => Ok(Opcode::SplatI8x16),
				0x0D => Ok(Opcode::SplatI16x8),
				0x0E => Ok(Opcode::SplatI32x4),
				0x0F => Ok(Opcode::SplatI64x2),
				0x10 => Ok(Opcode::SplatF32x4),
				0x11 => Ok(Opcode::SplatF64x2),
				0x12 => Ok(Opcode::ExtractLaneSI8x16),
				0x13 => Ok(Opcode::ExtractLaneUI8x16),
				0x14 => Ok(Opcode::ExtractLaneSI16x8),
				0x15 => Ok(Opcode::ExtractLaneUI16x8),
				0x16 => Ok(Opcode::ExtractLaneI32x4),
				0x17 => Ok(Opcode::ExtractLaneI64x2),
				0x18 => Ok(Opcode::ExtractLaneF32x4),
				0x19 => Ok(Opcode::ExtractLaneF64x2),
				0x1A => Ok(Opcode::ReplaceLaneI8x16),
				0x1B => Ok(Opcode::ReplaceLaneI16x8),
				0x1C => Ok(Opcode::ReplaceLaneI32x4),
				0x1D => Ok(Opcode::ReplaceLaneI64x2),
				0x1E => Ok(Opcode::ReplaceLaneF32x4),
				0x1F => Ok(Opcode::ReplaceLaneF64x2),
				_ => Err(WasmParseError::InvalidOpcodeExtension(value, extension)),
			},
			_ => Err(WasmParseError::InvalidOpcode(value)),
		}
	}
}
