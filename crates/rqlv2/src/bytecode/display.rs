// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pretty printing and disassembly for compiled bytecode programs.
//!
//! This module provides formatters for displaying [`CompiledProgram`] instances
//! in a human-readable IDA-style disassembly format with Unicode box drawing.

use std::{collections::HashSet, fmt};

use reifydb_type::util::unicode::UnicodeWidthStr;

use super::{
	instruction::BytecodeReader,
	opcode::{ObjectType, Opcode, OperatorKind},
	program::{CompiledProgram, Constant, DdlDef, SortDirection, SourceDef},
};

// Box and column width constants
const HEADER_BOX_WIDTH: usize = 120; // Total box width (matches DisplayConfig default)
const HEADER_CONTENT_WIDTH: usize = 116; // Content inside ║ ║ (120 - 4)

const BYTECODE_BOX_WIDTH: usize = 120; // Total box width
const BYTECODE_OFFSET_WIDTH: usize = 8; // "0x0000  "
const BYTECODE_OPERANDS_WIDTH: usize = 71; // Operands (normal) - adjusted for proper alignment
const BYTECODE_OPERANDS_JUMP_WIDTH: usize = 69; // Operands (jump targets) - adjusted for proper alignment

/// Pad or truncate text to exact display width.
fn pad_to_width(text: &str, width: usize) -> String {
	let current_width = text.width();

	if current_width > width {
		// Truncate with ellipsis
		if width >= 3 {
			let mut result = String::new();
			let mut w = 0;
			for ch in text.chars() {
				let ch_str = ch.to_string();
				let ch_w = ch_str.width();
				if w + ch_w + 3 > width {
					break;
				}
				result.push(ch);
				w += ch_w;
			}
			result.push_str("...");
			result
		} else {
			let mut result = String::new();
			let mut w = 0;
			for ch in text.chars() {
				let ch_str = ch.to_string();
				let ch_w = ch_str.width();
				if w + ch_w > width {
					break;
				}
				result.push(ch);
				w += ch_w;
			}
			result
		}
	} else if current_width < width {
		// Pad with spaces
		let padding = width - current_width;
		format!("{}{}", text, " ".repeat(padding))
	} else {
		text.to_string()
	}
}

/// Configuration for display formatting.
#[derive(Debug, Clone)]
pub struct DisplayConfig {
	/// Show hex bytes alongside instructions (default: true)
	pub show_hex: bool,
	/// Show bytecode offsets (default: true)
	pub show_offsets: bool,
	/// Show metadata sections (default: true)
	pub show_metadata: bool,
	/// Resolve references inline (default: true)
	pub resolve_refs: bool,
	/// Maximum column width (default: 120)
	pub max_width: usize,
}

impl Default for DisplayConfig {
	fn default() -> Self {
		Self {
			show_hex: true,
			show_offsets: true,
			show_metadata: true,
			resolve_refs: true,
			max_width: 120,
		}
	}
}

impl DisplayConfig {
	/// Create a new config with default values.
	pub fn new() -> Self {
		Self::default()
	}

	/// Create a minimal config (bytecode only, no metadata).
	pub fn minimal() -> Self {
		Self {
			show_hex: true,
			show_offsets: true,
			show_metadata: false,
			resolve_refs: false,
			max_width: 80,
		}
	}

	/// Create a verbose config (all details).
	pub fn verbose() -> Self {
		Self {
			show_hex: true,
			show_offsets: true,
			show_metadata: true,
			resolve_refs: true,
			max_width: 160,
		}
	}

	/// Set whether to show hex bytes.
	pub fn with_hex(mut self, show: bool) -> Self {
		self.show_hex = show;
		self
	}

	/// Set whether to show offsets.
	pub fn with_offsets(mut self, show: bool) -> Self {
		self.show_offsets = show;
		self
	}

	/// Set whether to show metadata sections.
	pub fn with_metadata(mut self, show: bool) -> Self {
		self.show_metadata = show;
		self
	}

	/// Set whether to resolve references.
	pub fn with_resolve(mut self, resolve: bool) -> Self {
		self.resolve_refs = resolve;
		self
	}

	/// Set maximum width.
	pub fn with_max_width(mut self, width: usize) -> Self {
		self.max_width = width;
		self
	}
}

/// Main formatter for [`CompiledProgram`].
pub struct ProgramFormatter<'a> {
	program: &'a CompiledProgram,
	config: DisplayConfig,
}

impl<'a> ProgramFormatter<'a> {
	/// Create a new formatter with default configuration.
	pub fn new(program: &'a CompiledProgram) -> Self {
		Self {
			program,
			config: DisplayConfig::default(),
		}
	}

	/// Create a formatter with custom configuration.
	pub fn with_config(program: &'a CompiledProgram, config: DisplayConfig) -> Self {
		Self {
			program,
			config,
		}
	}

	/// Format the program into a string.
	pub fn format(&self) -> String {
		let mut output = String::new();

		// Program header
		output.push_str(&self.format_header());
		output.push('\n');

		// Metadata sections
		if self.config.show_metadata {
			if !self.program.constants.is_empty() {
				output.push('\n');
				output.push_str(&format_constants_section(&self.program));
			}
			if !self.program.sources.is_empty() {
				output.push('\n');
				output.push_str(&format_sources_section(&self.program));
			}
			if !self.program.column_lists.is_empty() {
				output.push('\n');
				output.push_str(&format_column_lists_section(&self.program));
			}
			if !self.program.sort_specs.is_empty() {
				output.push('\n');
				output.push_str(&format_sort_specs_section(&self.program));
			}
			if !self.program.extension_specs.is_empty() {
				output.push('\n');
				output.push_str(&format_extension_specs_section(&self.program));
			}
			if !self.program.subqueries.is_empty() {
				output.push('\n');
				output.push_str(&format_subqueries_section(&self.program));
			}
			if !self.program.ddl_defs.is_empty() {
				output.push('\n');
				output.push_str(&format_ddl_defs_section(&self.program));
			}
			if !self.program.dml_targets.is_empty() {
				output.push('\n');
				output.push_str(&format_dml_targets_section(&self.program));
			}
			if !self.program.compiled_exprs.is_empty() || !self.program.compiled_filters.is_empty() {
				output.push('\n');
				output.push_str(&format_compiled_exprs_section(&self.program));
			}
		}

		// Bytecode disassembly
		output.push('\n');
		output.push_str(&self.format_bytecode_section());

		output
	}

	/// Format the program header.
	fn format_header(&self) -> String {
		let mut output = String::new();

		// Top border: ╔═══...═══╗
		output.push('╔');
		output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
		output.push_str("╗\n");

		// Title line
		let title = "COMPILED PROGRAM";
		let padding = (HEADER_CONTENT_WIDTH - title.len()) / 2;
		let title_line = format!(
			"{}{}{}",
			" ".repeat(padding),
			title,
			" ".repeat(HEADER_CONTENT_WIDTH - title.len() - padding)
		);
		output.push_str(&format!("║ {} ║\n", title_line));

		// Middle border
		output.push('╠');
		output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
		output.push_str("╣\n");

		// Entry point line
		let entry = format!("Entry Point: 0x{:04X}", self.program.entry_point);
		output.push_str(&format!("║ {} ║\n", pad_to_width(&entry, HEADER_CONTENT_WIDTH)));

		// Bytecode size line
		let size = format!("Bytecode Size: {} bytes", self.program.bytecode.len());
		output.push_str(&format!("║ {} ║\n", pad_to_width(&size, HEADER_CONTENT_WIDTH)));

		// Stats line
		let stats = format!(
			"Constants: {} | Sources: {} | Expressions: {}",
			self.program.constants.len(),
			self.program.sources.len(),
			self.program.compiled_exprs.len()
		);
		output.push_str(&format!("║ {} ║\n", pad_to_width(&stats, HEADER_CONTENT_WIDTH)));

		// Bottom border
		output.push('╚');
		output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
		output.push_str("╝");

		output
	}

	/// Format the bytecode section.
	fn format_bytecode_section(&self) -> String {
		format_bytecode_section(&self.program, &self.config)
	}
}

/// A decoded bytecode instruction with metadata.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DecodedInstruction {
	offset: usize,
	opcode: Opcode,
	operands: Vec<Operand>,
	raw_bytes: Vec<u8>,
	size: usize,
}

/// Operand representation with type information.
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum Operand {
	U8(u8),
	U16(u16),
	I16(i16),
	U32(u32),
	// Tagged variants for resolution
	ConstantRef(u16),
	SourceRef(u16),
	ColumnListRef(u16),
	SortSpecRef(u16),
	ExtSpecRef(u16),
	DdlDefRef(u16),
	DmlTargetRef(u16),
	ExprRef(u16),
	FilterRef(u16),
	OperatorKind(u8),
	ObjectType(u8),
	JumpOffset(i16, usize),
}

impl Operand {
	/// Format the operand with optional resolution.
	fn format(&self, program: &CompiledProgram, resolve: bool) -> String {
		match self {
			Operand::U8(v) => format!("{}", v),
			Operand::U16(v) => format!("{}", v),
			Operand::I16(v) => format!("{}", v),
			Operand::U32(v) => format!("0x{:08X}", v),
			Operand::ConstantRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_constant(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::SourceRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_source(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::ColumnListRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_column_list(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::SortSpecRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_sort_spec(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::ExtSpecRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_ext_spec(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::DdlDefRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_ddl_def(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::DmlTargetRef(idx) => {
				if resolve {
					format!("{}            → {}", idx, resolve_dml_target(program, *idx))
				} else {
					format!("{}", idx)
				}
			}
			Operand::ExprRef(idx) => {
				format!("{}            → <Expr #{}>", idx, idx)
			}
			Operand::FilterRef(idx) => {
				format!("{}            → <Filter #{}>", idx, idx)
			}
			Operand::OperatorKind(kind) => {
				if let Ok(op) = OperatorKind::try_from(*kind) {
					format!("{:?}", op)
				} else {
					format!("<Invalid:{}>", kind)
				}
			}
			Operand::ObjectType(ot) => {
				if let Ok(obj) = ObjectType::try_from(*ot) {
					format!("{:?}", obj)
				} else {
					format!("<Invalid:{}>", ot)
				}
			}
			Operand::JumpOffset(offset, target) => {
				format!("{:+}           → 0x{:04X}", offset, target)
			}
		}
	}
}

/// Error type for instruction decoding.
#[derive(Debug)]
enum DecodeError {
	InvalidOpcode(u8),
	Truncated,
	EndOfBytecode,
}

impl fmt::Display for DecodeError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			DecodeError::InvalidOpcode(op) => write!(f, "<INVALID:0x{:02X}>", op),
			DecodeError::Truncated => write!(f, "<TRUNCATED>"),
			DecodeError::EndOfBytecode => write!(f, "<END>"),
		}
	}
}

// ============================================================================
// Instruction Decoding
// ============================================================================

/// Decode a single instruction from the bytecode.
fn decode_instruction(
	reader: &mut BytecodeReader,
	_program: &CompiledProgram,
	current_offset: usize,
) -> Result<DecodedInstruction, DecodeError> {
	let offset = reader.position();
	let mut raw_bytes = Vec::new();

	// Read opcode
	let opcode_byte = reader.read_u8().ok_or(DecodeError::EndOfBytecode)?;
	raw_bytes.push(opcode_byte);

	let opcode = Opcode::try_from(opcode_byte).map_err(|_| DecodeError::InvalidOpcode(opcode_byte))?;

	// Read operands based on opcode
	let operands = decode_operands(reader, &mut raw_bytes, opcode, current_offset)?;

	let size = raw_bytes.len();

	Ok(DecodedInstruction {
		offset,
		opcode,
		operands,
		raw_bytes,
		size,
	})
}

/// Decode operands for a specific opcode.
fn decode_operands(
	reader: &mut BytecodeReader,
	raw_bytes: &mut Vec<u8>,
	opcode: Opcode,
	current_offset: usize,
) -> Result<Vec<Operand>, DecodeError> {
	let mut operands = Vec::new();

	match opcode {
		// Stack Operations - u16 operands
		Opcode::PushConst => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::ConstantRef(idx));
		}
		Opcode::PushExpr => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::ExprRef(idx));
		}
		Opcode::PushColRef => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::ConstantRef(idx));
		}
		Opcode::PushColList => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::ColumnListRef(idx));
		}
		Opcode::PushSortSpec => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::SortSpecRef(idx));
		}
		Opcode::PushExtSpec => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::ExtSpecRef(idx));
		}

		// Variable Operations - u32
		Opcode::LoadVar
		| Opcode::StoreVar
		| Opcode::UpdateVar
		| Opcode::LoadPipeline
		| Opcode::StorePipeline => {
			let id = read_u32(reader, raw_bytes)?;
			operands.push(Operand::U32(id));
		}

		// Pipeline Operations
		Opcode::Source | Opcode::FetchBatch => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::SourceRef(idx));
		}
		Opcode::Apply => {
			let kind = read_u8(reader, raw_bytes)?;
			operands.push(Operand::OperatorKind(kind));
		}

		// Control Flow - i16 relative offsets
		Opcode::Jump | Opcode::JumpIf | Opcode::JumpIfNot => {
			let offset = read_i16(reader, raw_bytes)?;
			// Calculate absolute target: current position + offset
			let target = (current_offset as i32 + offset as i32) as usize;
			operands.push(Operand::JumpOffset(offset, target));
		}

		// Function Calls
		Opcode::Call => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::U16(idx));
		}
		Opcode::CallBuiltin => {
			let id = read_u16(reader, raw_bytes)?;
			let argc = read_u8(reader, raw_bytes)?;
			operands.push(Operand::U16(id));
			operands.push(Operand::U8(argc));
		}

		// Frame Operations
		Opcode::GetField => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::ConstantRef(idx));
		}

		// DML Operations - u16
		Opcode::InsertRow | Opcode::UpdateRow | Opcode::DeleteRow => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::DmlTargetRef(idx));
		}

		// DDL Operations - u16
		Opcode::CreateNamespace
		| Opcode::CreateTable
		| Opcode::CreateView
		| Opcode::CreateIndex
		| Opcode::CreateSequence
		| Opcode::CreateRingBuffer
		| Opcode::CreateDictionary => {
			let idx = read_u16(reader, raw_bytes)?;
			operands.push(Operand::DdlDefRef(idx));
		}

		// Drop Object - u16, u8
		Opcode::DropObject => {
			let idx = read_u16(reader, raw_bytes)?;
			let obj_type = read_u8(reader, raw_bytes)?;
			operands.push(Operand::DdlDefRef(idx));
			operands.push(Operand::ObjectType(obj_type));
		}

		// No operands
		Opcode::Inline
		| Opcode::Collect
		| Opcode::Merge
		| Opcode::PopPipeline
		| Opcode::CheckComplete
		| Opcode::Return
		| Opcode::EnterScope
		| Opcode::ExitScope
		| Opcode::FrameLen
		| Opcode::FrameRow
		| Opcode::IntAdd
		| Opcode::IntLt
		| Opcode::IntEq
		| Opcode::IntSub
		| Opcode::IntMul
		| Opcode::IntDiv
		| Opcode::PrintOut
		| Opcode::ColAdd
		| Opcode::ColSub
		| Opcode::ColMul
		| Opcode::ColDiv
		| Opcode::ColLt
		| Opcode::ColLe
		| Opcode::ColGt
		| Opcode::ColGe
		| Opcode::ColEq
		| Opcode::ColNe
		| Opcode::ColAnd
		| Opcode::ColOr
		| Opcode::ColNot
		| Opcode::Nop
		| Opcode::Halt => {
			// No operands
		}
	}

	Ok(operands)
}

// Helper functions to read operands and track raw bytes
fn read_u8(reader: &mut BytecodeReader, raw_bytes: &mut Vec<u8>) -> Result<u8, DecodeError> {
	let val = reader.read_u8().ok_or(DecodeError::Truncated)?;
	raw_bytes.push(val);
	Ok(val)
}

fn read_u16(reader: &mut BytecodeReader, raw_bytes: &mut Vec<u8>) -> Result<u16, DecodeError> {
	let val = reader.read_u16().ok_or(DecodeError::Truncated)?;
	raw_bytes.extend_from_slice(&val.to_le_bytes());
	Ok(val)
}

fn read_i16(reader: &mut BytecodeReader, raw_bytes: &mut Vec<u8>) -> Result<i16, DecodeError> {
	let val = reader.read_i16().ok_or(DecodeError::Truncated)?;
	raw_bytes.extend_from_slice(&(val as u16).to_le_bytes());
	Ok(val)
}

fn read_u32(reader: &mut BytecodeReader, raw_bytes: &mut Vec<u8>) -> Result<u32, DecodeError> {
	let val = reader.read_u32().ok_or(DecodeError::Truncated)?;
	raw_bytes.extend_from_slice(&val.to_le_bytes());
	Ok(val)
}

// ============================================================================
// Metadata Resolution Functions
// ============================================================================

fn resolve_constant(program: &CompiledProgram, idx: u16) -> String {
	program.constants
		.get(idx as usize)
		.map(|c| format_constant(c))
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

fn resolve_source(program: &CompiledProgram, idx: u16) -> String {
	program.sources
		.get(idx as usize)
		.map(|s| format_source(s))
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

fn resolve_column_list(program: &CompiledProgram, idx: u16) -> String {
	program.column_lists
		.get(idx as usize)
		.map(|cols| {
			if cols.len() <= 3 {
				format!("[{}]", cols.join(", "))
			} else {
				format!("[{}, ... ({} total)]", cols[..2].join(", "), cols.len())
			}
		})
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

fn resolve_sort_spec(program: &CompiledProgram, idx: u16) -> String {
	program.sort_specs
		.get(idx as usize)
		.map(|spec| {
			let keys: Vec<_> = spec
				.keys
				.iter()
				.take(2)
				.map(|k| {
					format!(
						"{} {}",
						k.column,
						if k.direction == SortDirection::Asc {
							"ASC"
						} else {
							"DESC"
						}
					)
				})
				.collect();
			if spec.keys.len() > 2 {
				format!("[{}, ...]", keys.join(", "))
			} else {
				format!("[{}]", keys.join(", "))
			}
		})
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

fn resolve_ext_spec(program: &CompiledProgram, idx: u16) -> String {
	program.extension_specs
		.get(idx as usize)
		.map(|spec| {
			if spec.len() <= 2 {
				format!(
					"[{}]",
					spec.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>().join(", ")
				)
			} else {
				format!(
					"[{}, ... ({} total)]",
					spec.iter()
						.take(2)
						.map(|(name, _)| name.as_str())
						.collect::<Vec<_>>()
						.join(", "),
					spec.len()
				)
			}
		})
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

fn resolve_ddl_def(program: &CompiledProgram, idx: u16) -> String {
	program.ddl_defs
		.get(idx as usize)
		.map(|def| match def {
			DdlDef::CreateNamespace(d) => format!("Namespace '{}'", d.name),
			DdlDef::CreateTable(d) => format!("Table '{}'", d.name),
			DdlDef::CreateView(d) => format!("View '{}'", d.name),
			DdlDef::CreateIndex(d) => format!("Index '{}'", d.name),
			DdlDef::CreateSequence(d) => format!("Sequence '{}'", d.name),
			DdlDef::CreateRingBuffer(d) => format!("RingBuffer '{}'", d.name),
			DdlDef::CreateDictionary(d) => format!("Dictionary '{}'", d.name),
			DdlDef::AlterTable(d) => format!("AlterTable '{}'", d.table),
			DdlDef::AlterSequence(d) => format!("AlterSequence '{}'", d.sequence),
			DdlDef::Drop(d) => format!("Drop {:?} '{}'", d.object_type, d.name),
		})
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

fn resolve_dml_target(program: &CompiledProgram, idx: u16) -> String {
	program.dml_targets
		.get(idx as usize)
		.map(|t| format!("{:?} '{}'", t.target_type, t.name))
		.unwrap_or_else(|| format!("<OUT_OF_BOUNDS:{}>", idx))
}

// ============================================================================
// Formatting Helper Functions
// ============================================================================

fn format_constant(c: &Constant) -> String {
	match c {
		Constant::Null => "Null".to_string(),
		Constant::Bool(b) => format!("Bool({})", b),
		Constant::Int(i) => format!("Int({})", i),
		Constant::Float(f) => format!("Float({})", f),
		Constant::String(s) => {
			if s.len() > 30 {
				format!("String(\"{} ... ({} chars)\")", &s[..27], s.len())
			} else {
				format!("String(\"{}\")", s)
			}
		}
		Constant::Bytes(b) => {
			if b.len() > 16 {
				format!("Bytes([{} bytes])", b.len())
			} else {
				format!("Bytes({:?})", b)
			}
		}
	}
}

fn format_source(s: &SourceDef) -> String {
	if let Some(alias) = &s.alias {
		format!("\"{}\" (alias: \"{}\")", s.name, alias)
	} else {
		format!("\"{}\"", s.name)
	}
}

// ============================================================================
// Metadata Section Formatters
// ============================================================================

fn format_constants_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ CONSTANTS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 14));
	output.push_str("╗\n");

	for (i, constant) in program.constants.iter().enumerate() {
		let formatted = format_constant(constant);
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");

	output
}

fn format_sources_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ SOURCES ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 12));
	output.push_str("╗\n");

	for (i, source) in program.sources.iter().enumerate() {
		let formatted = format_source(source);
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");

	output
}

fn format_column_lists_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ COLUMN LISTS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 17));
	output.push_str("╗\n");

	for (i, cols) in program.column_lists.iter().enumerate() {
		let formatted = if cols.len() <= 5 {
			format!("[{}]", cols.join(", "))
		} else {
			format!("[{}, ... ({} total)]", cols[..3].join(", "), cols.len())
		};
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");

	output
}

fn format_sort_specs_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ SORT SPECS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 15));
	output.push_str("╗\n");

	for (i, spec) in program.sort_specs.iter().enumerate() {
		let keys: Vec<_> = spec
			.keys
			.iter()
			.map(|k| {
				format!(
					"{} {}",
					k.column,
					if k.direction == SortDirection::Asc {
						"ASC"
					} else {
						"DESC"
					}
				)
			})
			.collect();
		let formatted = format!("[{}]", keys.join(", "));
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

fn format_extension_specs_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ EXTENSION SPECS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 20));
	output.push_str("╗\n");

	for (i, spec) in program.extension_specs.iter().enumerate() {
		let names: Vec<_> = spec.iter().map(|(name, _)| name.as_str()).collect();
		let formatted = if names.len() <= 5 {
			format!("[{}]", names.join(", "))
		} else {
			format!("[{}, ... ({} total)]", names[..3].join(", "), names.len())
		};
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

fn format_subqueries_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ SUBQUERIES ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 15));
	output.push_str("╗\n");

	for (i, subq) in program.subqueries.iter().enumerate() {
		let formatted = format!("Source: {}", subq.source_name);
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

fn format_ddl_defs_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ DDL DEFINITIONS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 20));
	output.push_str("╗\n");

	for (i, def) in program.ddl_defs.iter().enumerate() {
		let formatted = match def {
			DdlDef::CreateNamespace(d) => format!("CREATE NAMESPACE {}", d.name),
			DdlDef::CreateTable(d) => format!("CREATE TABLE {} ({} cols)", d.name, d.columns.len()),
			DdlDef::CreateView(d) => format!("CREATE VIEW {}", d.name),
			DdlDef::CreateIndex(d) => format!("CREATE INDEX {} ON {}", d.name, d.table),
			DdlDef::CreateSequence(d) => format!("CREATE SEQUENCE {}", d.name),
			DdlDef::CreateRingBuffer(d) => {
				format!("CREATE RINGBUFFER {} ({} cols)", d.name, d.columns.len())
			}
			DdlDef::CreateDictionary(d) => format!("CREATE DICTIONARY {}", d.name),
			DdlDef::AlterTable(d) => format!("ALTER TABLE {}", d.table),
			DdlDef::AlterSequence(d) => format!("ALTER SEQUENCE {}", d.sequence),
			DdlDef::Drop(d) => format!("DROP {:?} {}", d.object_type, d.name),
		};
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

fn format_dml_targets_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ DML TARGETS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 16));
	output.push_str("╗\n");

	for (i, target) in program.dml_targets.iter().enumerate() {
		let formatted = format!("{:?}: {}", target.target_type, target.name);
		let prefix = format!("[{}] ", i);
		let content_width = HEADER_CONTENT_WIDTH - prefix.width();
		output.push_str(&format!("║ {}{} ║\n", prefix, pad_to_width(&formatted, content_width)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

fn format_compiled_exprs_section(program: &CompiledProgram) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ COMPILED EXPRESSIONS ");
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 25));
	output.push_str("╗\n");

	if !program.compiled_exprs.is_empty() {
		let text = format!("Expressions: {} (closures, not introspectable)", program.compiled_exprs.len());
		output.push_str(&format!("║ {} ║\n", pad_to_width(&text, HEADER_CONTENT_WIDTH)));
	}
	if !program.compiled_filters.is_empty() {
		let text = format!("Filters: {} (closures, not introspectable)", program.compiled_filters.len());
		output.push_str(&format!("║ {} ║\n", pad_to_width(&text, HEADER_CONTENT_WIDTH)));
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(HEADER_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

// ============================================================================
// Bytecode Disassembly Section
// ============================================================================

fn format_bytecode_section(program: &CompiledProgram, config: &DisplayConfig) -> String {
	let mut output = String::new();

	// Top border
	output.push('╔');
	output.push_str("═ BYTECODE ");
	output.push_str(&"═".repeat(BYTECODE_BOX_WIDTH - 13));
	output.push_str("╗\n");

	// Column header
	let header = format!(
		"║ {:<8}   {:<16} {:<16} {:<width$} ║\n",
		"OFFSET",
		"BYTES",
		"INSTRUCTION",
		"OPERANDS",
		width = BYTECODE_OPERANDS_WIDTH
	);
	output.push_str(&header);

	// Separator
	output.push('╠');
	output.push_str(&"═".repeat(BYTECODE_BOX_WIDTH - 2));
	output.push_str("╣\n");

	if program.bytecode.is_empty() {
		let empty_msg = pad_to_width("(empty)", BYTECODE_BOX_WIDTH - 4);
		output.push_str(&format!("║ {} ║\n", empty_msg));
		output.push('╚');
		output.push_str(&"═".repeat(BYTECODE_BOX_WIDTH - 2));
		output.push_str("╝");
		return output;
	}

	// Pre-scan for jump targets
	let jump_targets = find_jump_targets(program);

	let mut reader = BytecodeReader::new(&program.bytecode);
	let mut label_counter = 1;

	while !reader.at_end() {
		let offset = reader.position();

		// Add jump label if this is a target
		if jump_targets.contains(&offset) {
			let label_str = format!("╔═ L{:04}:", label_counter);
			let label_width = BYTECODE_BOX_WIDTH - BYTECODE_OFFSET_WIDTH - 7;
			let padded = pad_to_width(&label_str, label_width);
			output.push_str(&format!("║ 0x{:04X}   {} ║\n", offset, padded));
			label_counter += 1;
		}

		// Try to decode the instruction
		match decode_instruction(&mut reader, program, offset) {
			Ok(instr) => {
				// Format hex bytes
				let hex_str = pad_to_width(&format_hex_bytes(&instr.raw_bytes), 16);

				// Format opcode
				let opcode_str = pad_to_width(&format!("{:?}", instr.opcode), 16);

				// Format operands
				let operands_str = instr
					.operands
					.iter()
					.map(|op| op.format(program, config.resolve_refs))
					.collect::<Vec<_>>()
					.join(", ");

				// Build the line
				let offset_str = format!("0x{:04X}", offset);
				let offset_padded = pad_to_width(&offset_str, BYTECODE_OFFSET_WIDTH);
				let line = if jump_targets.contains(&offset) {
					// Jump target: inner ║ separator, operands limited to 69 chars
					let operands_padded = pad_to_width(&operands_str, BYTECODE_OPERANDS_JUMP_WIDTH);
					format!(
						"║ {}   ║ {} {} {} ║\n",
						offset_padded, hex_str, opcode_str, operands_padded
					)
				} else {
					// Normal: no inner separator, operands get 71 chars
					let operands_padded = pad_to_width(&operands_str, BYTECODE_OPERANDS_WIDTH);
					format!(
						"║ {}   {} {} {} ║\n",
						offset_padded, hex_str, opcode_str, operands_padded
					)
				};
				output.push_str(&line);
			}
			Err(err) => {
				// Show error inline
				let offset_str = format!("0x{:04X}", offset);
				let offset_padded = pad_to_width(&offset_str, BYTECODE_OFFSET_WIDTH);
				let err_width = BYTECODE_BOX_WIDTH - BYTECODE_OFFSET_WIDTH - 7;
				let err_str = pad_to_width(&err.to_string(), err_width);
				output.push_str(&format!("║ {}   {} ║\n", offset_padded, err_str));
				break;
			}
		}
	}

	// Bottom border
	output.push('╚');
	output.push_str(&"═".repeat(BYTECODE_BOX_WIDTH - 2));
	output.push_str("╝");
	output
}

/// Find all jump target offsets in the bytecode.
fn find_jump_targets(program: &CompiledProgram) -> HashSet<usize> {
	let mut targets = HashSet::new();
	let mut reader = BytecodeReader::new(&program.bytecode);

	while !reader.at_end() {
		let _offset = reader.position();

		if let Some(opcode_byte) = reader.read_u8() {
			if let Ok(opcode) = Opcode::try_from(opcode_byte) {
				match opcode {
					Opcode::Jump | Opcode::JumpIf | Opcode::JumpIfNot => {
						if let Some(offset_val) = reader.read_i16() {
							let target =
								(reader.position() as i32 + offset_val as i32) as usize;
							targets.insert(target);
						}
					}
					// Skip other operands
					Opcode::PushConst
					| Opcode::PushExpr
					| Opcode::PushColRef
					| Opcode::PushColList
					| Opcode::PushSortSpec
					| Opcode::PushExtSpec
					| Opcode::Source
					| Opcode::FetchBatch
					| Opcode::Call
					| Opcode::GetField
					| Opcode::InsertRow
					| Opcode::UpdateRow
					| Opcode::DeleteRow
					| Opcode::CreateNamespace
					| Opcode::CreateTable
					| Opcode::CreateView
					| Opcode::CreateIndex
					| Opcode::CreateSequence
					| Opcode::CreateRingBuffer
					| Opcode::CreateDictionary => {
						reader.read_u16();
					}
					Opcode::LoadVar
					| Opcode::StoreVar
					| Opcode::UpdateVar
					| Opcode::LoadPipeline
					| Opcode::StorePipeline => {
						reader.read_u32();
					}
					Opcode::Apply => {
						reader.read_u8();
					}
					Opcode::CallBuiltin => {
						reader.read_u16();
						reader.read_u8();
					}
					Opcode::DropObject => {
						reader.read_u16();
						reader.read_u8();
					}
					_ => {}
				}
			}
		}
	}

	targets
}

/// Format hex bytes for display.
fn format_hex_bytes(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ")
}

// ============================================================================
// Display Trait Implementation
// ============================================================================

impl fmt::Display for CompiledProgram {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", ProgramFormatter::new(self).format())
	}
}
