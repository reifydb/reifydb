// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	parse::{
		Opcode, Result, WasmCustom, WasmData, WasmDataMode, WasmElement, WasmElementMode, WasmExport,
		WasmExportDescriptor, WasmFunc, WasmFunctionBody, WasmGlobal, WasmGlobalInit, WasmGlobalType,
		WasmImport, WasmImportDescriptor, WasmInstruction, WasmMemory, WasmMemoryArgument, WasmModule,
		WasmParseError,
		WasmParseError::{
			InvalidExportDescriptor, InvalidMagicNumber, InvalidSectionCode, InvalidValueType,
			UnsupportedVersion,
		},
		WasmResizableLimit, WasmResultType, WasmTable, WasmValueType,
	},
	util::byte_reader::ByteReader,
};

// ---------------------------------------------------------------------------
// SectionCode
// ---------------------------------------------------------------------------

enum SectionCode {
	Custom = 0x00,
	Type = 0x01,
	Import = 0x02,
	Function = 0x03,
	Table = 0x04,
	Memory = 0x05,
	Global = 0x06,
	Export = 0x07,
	Start = 0x08,
	Element = 0x09,
	Code = 0x0A,
	Data = 0x0B,
	DataCount = 0x0C,
}

impl SectionCode {
	fn from_u8(value: u8) -> Result<SectionCode> {
		match value {
			0x00 => Ok(SectionCode::Custom),
			0x01 => Ok(SectionCode::Type),
			0x02 => Ok(SectionCode::Import),
			0x03 => Ok(SectionCode::Function),
			0x04 => Ok(SectionCode::Table),
			0x05 => Ok(SectionCode::Memory),
			0x06 => Ok(SectionCode::Global),
			0x07 => Ok(SectionCode::Export),
			0x08 => Ok(SectionCode::Start),
			0x09 => Ok(SectionCode::Element),
			0x0A => Ok(SectionCode::Code),
			0x0B => Ok(SectionCode::Data),
			0x0C => Ok(SectionCode::DataCount),
			_ => Err(InvalidSectionCode(value)),
		}
	}
}

// ---------------------------------------------------------------------------
// WasmParser - main public API
// ---------------------------------------------------------------------------

/// The `WasmParser` struct is responsible for decoding a WebAssembly (WASM) binary module
/// from a byte stream. It utilizes a `ByteReader` to sequentially read and interpret
/// the bytes that represent the WASM module's structure, such as the magic header and version.
pub struct WasmParser {}

impl WasmParser {
	/// Decodes the WASM module from the byte stream.
	///
	/// This function reads the necessary parts of a WASM module.
	/// It proceeds by interpreting these values and advancing the cursor position accordingly.
	///
	/// # Returns
	///
	/// A `Result` containing either a successfully decoded `WasmModule` or a `WasmParseError`
	/// if any part of the decoding process fails (e.g., due to an unexpected end of file or
	/// invalid data).
	pub fn parse(input: &[u8]) -> Result<WasmModule> {
		let reader = ByteReader::new(input);
		let magic = Self::parse_magic(&reader)?;
		let version = Self::parse_version(&reader)?;

		let mut result = WasmModule {
			magic,
			version,
			customs: Box::default(),
			types: Box::default(),
			imports: Box::default(),
			functions: Box::default(),
			globals: Box::new([]),
			tables: Box::default(),
			memories: Box::default(),
			exports: Box::default(),
			start_function: None,
			elements: Box::default(),
			codes: Box::default(),
			data: Box::default(),
		};

		while !reader.eof() {
			let (code, size) = Self::parse_section_header(&reader)?;
			match code {
				SectionCode::Custom => {
					result.customs = parse_custom_section(size, &reader)?;
				}
				SectionCode::Type => {
					result.types = parse_types_section(size, &reader)?;
				}
				SectionCode::Import => {
					result.imports = parse_import_section(size, &reader)?;
				}
				SectionCode::Function => {
					result.functions = parse_functions_section(size, &reader)?;
				}
				SectionCode::Memory => {
					result.memories = parse_memory_section(size, &reader)?;
				}
				SectionCode::Table => {
					result.tables = parse_table_section(size, &reader)?;
				}
				SectionCode::Global => {
					result.globals = parse_global_section(size, &reader)?;
				}
				SectionCode::Element => {
					result.elements = parse_element_section(size, &reader)?;
				}
				SectionCode::Export => {
					result.exports = parse_export_section(size, &reader)?;
				}
				SectionCode::Start => {
					result.start_function = Some(reader.read_leb128_u32()?);
				}
				SectionCode::Code => {
					result.codes = parse_code_section(size, &reader)?;
				}
				SectionCode::Data => {
					result.data = parse_data_section(size, &reader)?;
				}
				SectionCode::DataCount => {
					// Data count section — just read and discard the count
					let _ = reader.read_leb128_u32()?;
				}
			}
		}

		Ok(result)
	}

	fn parse_magic(reader: &ByteReader) -> Result<Box<[u8]>> {
		let result = reader.read_range(4)?;
		if result.as_ref() != [0x00, 0x61, 0x73, 0x6D] {
			Err(InvalidMagicNumber)
		} else {
			Ok(result)
		}
	}

	fn parse_version(reader: &ByteReader) -> Result<u32> {
		let result = reader.read_u32()?;
		if result != 1 {
			Err(UnsupportedVersion(result))
		} else {
			Ok(result)
		}
	}

	fn parse_section_header(reader: &ByteReader) -> Result<(SectionCode, u32)> {
		let code = SectionCode::from_u8(reader.read_u8()?)?;
		let size = reader.read_leb128_u32()?;
		Ok((code, size))
	}
}

// ---------------------------------------------------------------------------
// Type section
// ---------------------------------------------------------------------------

fn parse_types_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmFunc]>> {
	let mut result: Vec<WasmFunc> = vec![];
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;

	for _ in 0..count {
		let _ = reader.read_u8();
		let mut func = WasmFunc::default();

		let param_count = reader.read_leb128_u32()?;
		func.params = parse_value_types(param_count, reader)?;

		let return_count = reader.read_leb128_u32()?;
		func.results = parse_value_types(return_count, reader)?;

		result.push(func);
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

// ---------------------------------------------------------------------------
// Function section
// ---------------------------------------------------------------------------

fn parse_functions_section(size: u32, reader: &ByteReader) -> Result<Box<[u32]>> {
	let mut result = vec![];
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;

	for _ in 0..count {
		let idx = reader.read_leb128_u32()?;
		result.push(idx);
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

// ---------------------------------------------------------------------------
// Export section
// ---------------------------------------------------------------------------

fn parse_export_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmExport]>> {
	let expected_reader_position = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;
	let mut result = vec![];

	for _ in 0..count {
		let name = parse_name(reader)?;
		let export_kind = reader.read_u8()?;
		let idx = reader.read_leb128_u32()?;
		let desc = match export_kind {
			0x00 => Ok(WasmExportDescriptor::Func(idx)),
			0x01 => Ok(WasmExportDescriptor::Table(idx)),
			0x02 => Ok(WasmExportDescriptor::Memory(idx)),
			0x03 => Ok(WasmExportDescriptor::Global(idx)),
			_ => Err(InvalidExportDescriptor(export_kind)),
		}?;
		result.push(WasmExport {
			name,
			desc,
		});
	}

	debug_assert_eq!(reader.pos(), expected_reader_position);
	Ok(result.into())
}

// ---------------------------------------------------------------------------
// Import section
// ---------------------------------------------------------------------------

fn parse_import_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmImport]>> {
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;

	let mut result = vec![];

	for _ in 0..count {
		let module = parse_name(reader)?;
		let name = parse_name(reader)?;
		let import_kind = reader.read_u8()?;
		let desc = match import_kind {
			0x00 => {
				let idx = reader.read_leb128_u32()?;
				Ok(WasmImportDescriptor::Function(idx))
			}
			0x01 => Ok(WasmImportDescriptor::Table(parse_table(reader)?)),
			0x02 => Ok(WasmImportDescriptor::Memory(parse_memory(reader)?)),
			0x03 => Ok(WasmImportDescriptor::Global(parse_global_type(reader)?)),
			_ => Err(WasmParseError::InvalidImportDescriptor(import_kind)),
		}?;

		result.push(WasmImport {
			module,
			name,
			desc,
		});
	}
	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

// ---------------------------------------------------------------------------
// Memory section
// ---------------------------------------------------------------------------

fn parse_memory_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmMemory]>> {
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;
	let mut result = vec![];

	for _ in 0..count {
		let limits = parse_limits(reader)?;
		result.push(WasmMemory {
			limits,
		});
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

fn parse_memory(reader: &ByteReader) -> Result<WasmMemory> {
	let limits = parse_limits(reader)?;
	Ok(WasmMemory {
		limits,
	})
}

// ---------------------------------------------------------------------------
// Table section
// ---------------------------------------------------------------------------

fn parse_table_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmTable]>> {
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;
	let mut result = vec![];

	for _ in 0..count {
		result.push(parse_table(reader)?);
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

fn parse_table(reader: &ByteReader) -> Result<WasmTable> {
	let ty = parse_value_type(reader)?;
	let limits = parse_limits(reader)?;
	Ok(WasmTable {
		element_types: ty,
		limits,
	})
}

// ---------------------------------------------------------------------------
// Global section
// ---------------------------------------------------------------------------

fn parse_global_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmGlobal]>> {
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;
	let mut result = vec![];

	for _ in 0..count {
		result.push(parse_global(reader)?);
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

fn parse_global_type(reader: &ByteReader) -> Result<WasmGlobalType> {
	let ty = parse_value_type(reader)?;
	let mutable = match reader.read_u8()? {
		0x00 => false,
		0x01 => true,
		v => return Err(WasmParseError::InvalidMutability(v)),
	};

	Ok(WasmGlobalType {
		value_type: ty,
		mutable,
	})
}

fn parse_global(reader: &ByteReader) -> Result<WasmGlobal> {
	let gt = parse_global_type(reader)?;

	Ok(WasmGlobal {
		value_type: gt.value_type,
		mutable: gt.mutable,
		init: parse_global_init(reader)?,
	})
}

fn parse_global_init(reader: &ByteReader) -> Result<WasmGlobalInit> {
	let result = match parse_instruction(reader)? {
		WasmInstruction::F32Const(v) => Ok(WasmGlobalInit::F32(v)),
		WasmInstruction::F64Const(v) => Ok(WasmGlobalInit::F64(v)),
		WasmInstruction::I32Const(v) => Ok(WasmGlobalInit::I32(v)),
		WasmInstruction::I64Const(v) => Ok(WasmGlobalInit::I64(v)),
		WasmInstruction::GlobalGet(idx) => Ok(WasmGlobalInit::Global(idx)),
		WasmInstruction::RefNull(vt) => Ok(WasmGlobalInit::NullRef(vt)),
		WasmInstruction::RefFunc(idx) => Ok(WasmGlobalInit::FuncRef(idx)),
		instruction => Err(WasmParseError::InvalidGlobalInit(instruction)),
	};

	let end_opcode = parse_opcode(reader)?;
	debug_assert_eq!(end_opcode, Opcode::End);

	result
}

// ---------------------------------------------------------------------------
// Element section
// ---------------------------------------------------------------------------

fn parse_element_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmElement]>> {
	let expected_reader_position = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;
	let mut result = vec![];

	for _ in 0..count {
		result.push(parse_element(reader)?);
	}

	debug_assert_eq!(reader.pos(), expected_reader_position);
	Ok(result.into_boxed_slice())
}

fn parse_element(reader: &ByteReader) -> Result<WasmElement> {
	let flags = reader.read_leb128_u32()?;

	match flags {
		// Mode 0: active, table 0, function indices
		0x00 => {
			let offset = parse_instructions(reader)?;
			let func_count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(func_count as usize);
			for _ in 0..func_count {
				init.push(reader.read_leb128_u32()?);
			}
			Ok(WasmElement {
				mode: WasmElementMode::Active {
					table: 0,
					offset,
				},
				init: init.into_boxed_slice(),
			})
		}
		// Mode 1: passive, elem kind byte, function indices
		0x01 => {
			let _elem_kind = reader.read_u8()?; // 0x00 = funcref
			let func_count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(func_count as usize);
			for _ in 0..func_count {
				init.push(reader.read_leb128_u32()?);
			}
			Ok(WasmElement {
				mode: WasmElementMode::Passive,
				init: init.into_boxed_slice(),
			})
		}
		// Mode 2: active, explicit table, elem kind byte, function indices
		0x02 => {
			let table = reader.read_leb128_u32()?;
			let offset = parse_instructions(reader)?;
			let _elem_kind = reader.read_u8()?;
			let func_count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(func_count as usize);
			for _ in 0..func_count {
				init.push(reader.read_leb128_u32()?);
			}
			Ok(WasmElement {
				mode: WasmElementMode::Active {
					table,
					offset,
				},
				init: init.into_boxed_slice(),
			})
		}
		// Mode 3: declarative, elem kind byte, function indices
		0x03 => {
			let _elem_kind = reader.read_u8()?;
			let func_count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(func_count as usize);
			for _ in 0..func_count {
				init.push(reader.read_leb128_u32()?);
			}
			Ok(WasmElement {
				mode: WasmElementMode::Declarative,
				init: init.into_boxed_slice(),
			})
		}
		// Mode 4: active, table 0, init expressions
		0x04 => {
			let offset = parse_instructions(reader)?;
			let count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(count as usize);
			for _ in 0..count {
				let expr = parse_instructions(reader)?;
				// Extract function index from ref.func expression
				init.push(extract_func_idx_from_expr(&expr));
			}
			Ok(WasmElement {
				mode: WasmElementMode::Active {
					table: 0,
					offset,
				},
				init: init.into_boxed_slice(),
			})
		}
		// Mode 5: passive, ref type, init expressions
		0x05 => {
			let _ref_type = reader.read_u8()?; // e.g. 0x70 = funcref
			let count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(count as usize);
			for _ in 0..count {
				let expr = parse_instructions(reader)?;
				init.push(extract_func_idx_from_expr(&expr));
			}
			Ok(WasmElement {
				mode: WasmElementMode::Passive,
				init: init.into_boxed_slice(),
			})
		}
		// Mode 6: active, explicit table, ref type, init expressions
		0x06 => {
			let table = reader.read_leb128_u32()?;
			let offset = parse_instructions(reader)?;
			let _ref_type = reader.read_u8()?;
			let count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(count as usize);
			for _ in 0..count {
				let expr = parse_instructions(reader)?;
				init.push(extract_func_idx_from_expr(&expr));
			}
			Ok(WasmElement {
				mode: WasmElementMode::Active {
					table,
					offset,
				},
				init: init.into_boxed_slice(),
			})
		}
		// Mode 7: declarative, ref type, init expressions
		0x07 => {
			let _ref_type = reader.read_u8()?;
			let count = reader.read_leb128_u32()?;
			let mut init = Vec::with_capacity(count as usize);
			for _ in 0..count {
				let expr = parse_instructions(reader)?;
				init.push(extract_func_idx_from_expr(&expr));
			}
			Ok(WasmElement {
				mode: WasmElementMode::Declarative,
				init: init.into_boxed_slice(),
			})
		}
		m => Err(WasmParseError::InvalidElementMode(m as u8)),
	}
}

/// Extract a function index from an element init expression.
/// Handles ref.func $idx and ref.null patterns.
fn extract_func_idx_from_expr(expr: &[WasmInstruction]) -> u32 {
	for instr in expr {
		match instr {
			WasmInstruction::RefFunc(idx) => return *idx,
			_ => {}
		}
	}
	// ref.null or other expressions — use sentinel value
	u32::MAX
}

// ---------------------------------------------------------------------------
// Code section
// ---------------------------------------------------------------------------

fn parse_code_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmFunctionBody]>> {
	let mut result = vec![];
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;

	for _ in 0..count {
		let size = reader.read_leb128_u32()?;
		let body = parse_function_body(size, reader)?;
		result.push(body);
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

fn parse_function_body(size: u32, reader: &ByteReader) -> Result<WasmFunctionBody> {
	let expected_reader_pos = reader.pos() + size as usize;

	let count = reader.read_leb128_u32()?;
	let mut locals = vec![];

	for _ in 0..count {
		let type_count = reader.read_leb128_u32()?;
		let value_type = parse_value_type(reader)?;
		locals.push((type_count, value_type));
	}

	let code = parse_instructions(reader)?;

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(WasmFunctionBody {
		locals: locals.into(),
		code: code.into(),
	})
}

// ---------------------------------------------------------------------------
// Data section
// ---------------------------------------------------------------------------

fn parse_data_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmData]>> {
	let expected_reader_pos = reader.pos() + size as usize;
	let count = reader.read_leb128_u32()?;

	let mut result = vec![];

	for _ in 0..count {
		let flags = reader.read_leb128_u32()?;
		match flags {
			// Mode 0: active, memory 0, offset expression, data
			0 => {
				let offset = parse_data_expr(reader)?;
				let size = reader.read_leb128_u32()?;
				let data = reader.read_range(size as usize)?;
				result.push(WasmData {
					mode: WasmDataMode::Active {
						index: 0,
						offset,
					},
					data,
				});
			}
			// Mode 1: passive, just data
			1 => {
				let size = reader.read_leb128_u32()?;
				let data = reader.read_range(size as usize)?;
				result.push(WasmData {
					mode: WasmDataMode::Passive,
					data,
				});
			}
			// Mode 2: active, explicit memory index, offset expression, data
			2 => {
				let memory_index = reader.read_leb128_u32()?;
				let offset = parse_data_expr(reader)?;
				let size = reader.read_leb128_u32()?;
				let data = reader.read_range(size as usize)?;
				result.push(WasmData {
					mode: WasmDataMode::Active {
						index: memory_index,
						offset,
					},
					data,
				});
			}
			_ => return Err(WasmParseError::InvalidDataMode(flags as u8)),
		}
	}

	debug_assert_eq!(reader.pos(), expected_reader_pos);
	Ok(result.into())
}

fn parse_data_expr(reader: &ByteReader) -> Result<u32> {
	let opcode = reader.read_u8()?;
	let offset = match opcode {
		0x41 => {
			// i32.const: signed LEB128, reinterpret as u32
			reader.read_leb128_i32()? as u32
		}
		0x23 => {
			// global.get: read global index
			reader.read_leb128_u32()?
			// Note: this returns the global index, not the actual offset.
			// The compiler resolves this during compilation.
		}
		_ => reader.read_leb128_u32()?,
	};
	let end = reader.read_u8()?;
	debug_assert_eq!(end, 0x0B); // end opcode
	Ok(offset)
}

// ---------------------------------------------------------------------------
// Custom section
// ---------------------------------------------------------------------------

fn parse_custom_section(size: u32, reader: &ByteReader) -> Result<Box<[WasmCustom]>> {
	let _ = reader.pos() + size as usize;

	let result = vec![];

	// FIXME: implement custom section parsing
	let _ = reader.read_range(size as usize)?;

	Ok(result.into())
}

// ---------------------------------------------------------------------------
// Instruction parsing
// ---------------------------------------------------------------------------

fn parse_instructions(reader: &ByteReader) -> Result<Box<[WasmInstruction]>> {
	let mut result = vec![];

	loop {
		let opcode = parse_opcode(reader)?;
		if opcode == Opcode::End {
			break;
		}
		let instruction = parse_instruction_of(opcode, reader)?;
		result.push(instruction.clone());
	}

	Ok(result.into_boxed_slice())
}

fn parse_mem_arg(reader: &ByteReader) -> Result<WasmMemoryArgument> {
	let flags = reader.read_leb128_u32()?;
	let offset = reader.read_leb128_u32()?;
	Ok(WasmMemoryArgument {
		align: flags,
		offset,
	})
}

fn parse_opcode(reader: &ByteReader) -> Result<Opcode> {
	let main = reader.read_u8()?;
	let extension = if main == 0xFC || main == 0xFD {
		reader.read_leb128_u32()?
	} else {
		0
	};
	Opcode::from_u8(main, extension)
}

fn parse_instruction(reader: &ByteReader) -> Result<WasmInstruction> {
	let opcode = parse_opcode(reader)?;
	parse_instruction_of(opcode, reader)
}

fn parse_instruction_of(opcode: Opcode, reader: &ByteReader) -> Result<WasmInstruction> {
	match opcode {
		Opcode::I32Add => Ok(WasmInstruction::I32Add),
		Opcode::I64Add => Ok(WasmInstruction::I64Add),
		Opcode::F32Add => Ok(WasmInstruction::F32Add),
		Opcode::F64Add => Ok(WasmInstruction::F64Add),

		Opcode::I32Sub => Ok(WasmInstruction::I32Sub),
		Opcode::I64Sub => Ok(WasmInstruction::I64Sub),
		Opcode::F32Sub => Ok(WasmInstruction::F32Sub),
		Opcode::F64Sub => Ok(WasmInstruction::F64Sub),

		Opcode::I32Mul => Ok(WasmInstruction::I32Mul),
		Opcode::I64Mul => Ok(WasmInstruction::I64Mul),
		Opcode::F32Mul => Ok(WasmInstruction::F32Mul),
		Opcode::F64Mul => Ok(WasmInstruction::F64Mul),

		Opcode::I32DivS => Ok(WasmInstruction::I32DivS),
		Opcode::I32DivU => Ok(WasmInstruction::I32DivU),
		Opcode::I64DivS => Ok(WasmInstruction::I64DivS),
		Opcode::I64DivU => Ok(WasmInstruction::I64DivU),

		Opcode::I32Const => {
			let value = reader.read_leb128_i32()?;
			Ok(WasmInstruction::I32Const(value))
		}
		Opcode::I64Const => {
			let value = reader.read_leb128_i64()?;
			Ok(WasmInstruction::I64Const(value))
		}
		Opcode::F32Const => {
			let value = reader.read_f32()?;
			Ok(WasmInstruction::F32Const(value))
		}
		Opcode::F64Const => {
			let value = reader.read_f64()?;
			Ok(WasmInstruction::F64Const(value))
		}

		Opcode::I32And => Ok(WasmInstruction::I32And),
		Opcode::I64And => Ok(WasmInstruction::I64And),

		Opcode::F32ConvertI32S => Ok(WasmInstruction::F32ConvertI32S),
		Opcode::F32ConvertI32U => Ok(WasmInstruction::F32ConvertI32U),
		Opcode::F32ConvertI64S => Ok(WasmInstruction::F32ConvertI64S),
		Opcode::F32ConvertI64U => Ok(WasmInstruction::F32ConvertI64U),

		Opcode::F64ConvertI32S => Ok(WasmInstruction::F64ConvertI32S),
		Opcode::F64ConvertI32U => Ok(WasmInstruction::F64ConvertI32U),
		Opcode::F64ConvertI64S => Ok(WasmInstruction::F64ConvertI64S),
		Opcode::F64ConvertI64U => Ok(WasmInstruction::F64ConvertI64U),

		Opcode::F64PromoteF32 => Ok(WasmInstruction::F64PromoteF32),
		Opcode::F32DemoteF64 => Ok(WasmInstruction::F32DemoteF64),

		Opcode::I32Extend8S => Ok(WasmInstruction::I32Extend8S),
		Opcode::I32Extend16S => Ok(WasmInstruction::I32Extend16S),
		Opcode::I64Extend8S => Ok(WasmInstruction::I64Extend8S),
		Opcode::I64Extend16S => Ok(WasmInstruction::I64Extend16S),
		Opcode::I64Extend32S => Ok(WasmInstruction::I64Extend32S),
		Opcode::I64ExtendI32S => Ok(WasmInstruction::I64ExtendI32S),
		Opcode::I64ExtendI32U => Ok(WasmInstruction::I64ExtendI32U),

		Opcode::I32Or => Ok(WasmInstruction::I32Or),
		Opcode::I64Or => Ok(WasmInstruction::I64Or),

		Opcode::I32Shl => Ok(WasmInstruction::I32Shl),
		Opcode::I64Shl => Ok(WasmInstruction::I64Shl),

		Opcode::I32ShrS => Ok(WasmInstruction::I32ShrS),
		Opcode::I64ShrS => Ok(WasmInstruction::I64ShrS),
		Opcode::I32ShrU => Ok(WasmInstruction::I32ShrU),
		Opcode::I64ShrU => Ok(WasmInstruction::I64ShrU),

		Opcode::I32Rotl => Ok(WasmInstruction::I32Rotl),
		Opcode::I64Rotl => Ok(WasmInstruction::I64Rotl),
		Opcode::I32Rotr => Ok(WasmInstruction::I32Rotr),
		Opcode::I64Rotr => Ok(WasmInstruction::I64Rotr),

		Opcode::I32Eq => Ok(WasmInstruction::I32Eq),
		Opcode::I64Eq => Ok(WasmInstruction::I64Eq),
		Opcode::F32Eq => Ok(WasmInstruction::F32Eq),
		Opcode::F64Eq => Ok(WasmInstruction::F64Eq),

		Opcode::I32Ne => Ok(WasmInstruction::I32Ne),
		Opcode::I64Ne => Ok(WasmInstruction::I64Ne),
		Opcode::F32Ne => Ok(WasmInstruction::F32Ne),
		Opcode::F64Ne => Ok(WasmInstruction::F64Ne),

		Opcode::I32Eqz => Ok(WasmInstruction::I32Eqz),
		Opcode::I64Eqz => Ok(WasmInstruction::I64Eqz),

		Opcode::I32Clz => Ok(WasmInstruction::I32Clz),
		Opcode::I64Clz => Ok(WasmInstruction::I64Clz),

		Opcode::I32Ctz => Ok(WasmInstruction::I32Ctz),
		Opcode::I64Ctz => Ok(WasmInstruction::I64Ctz),

		Opcode::I32Popcnt => Ok(WasmInstruction::I32Popcnt),
		Opcode::I64Popcnt => Ok(WasmInstruction::I64Popcnt),

		Opcode::F32Neg => Ok(WasmInstruction::F32Neg),
		Opcode::F64Neg => Ok(WasmInstruction::F64Neg),

		Opcode::F32Abs => Ok(WasmInstruction::F32Abs),
		Opcode::F64Abs => Ok(WasmInstruction::F64Abs),

		Opcode::I32ReinterpretF32 => Ok(WasmInstruction::I32ReinterpretF32),
		Opcode::F32ReinterpretI32 => Ok(WasmInstruction::F32ReinterpretI32),
		Opcode::I64ReinterpretF64 => Ok(WasmInstruction::I64ReinterpretF64),
		Opcode::F64ReinterpretI64 => Ok(WasmInstruction::F64ReinterpretI64),

		Opcode::F32Sqrt => Ok(WasmInstruction::F32Sqrt),
		Opcode::F64Sqrt => Ok(WasmInstruction::F64Sqrt),

		Opcode::F32Ceil => Ok(WasmInstruction::F32Ceil),
		Opcode::F64Ceil => Ok(WasmInstruction::F64Ceil),

		Opcode::F32Floor => Ok(WasmInstruction::F32Floor),
		Opcode::F64Floor => Ok(WasmInstruction::F64Floor),

		Opcode::F32Copysign => Ok(WasmInstruction::F32Copysign),
		Opcode::F64Copysign => Ok(WasmInstruction::F64Copysign),

		Opcode::Call => {
			let addr = reader.read_leb128_u32()?;
			Ok(WasmInstruction::Call(addr))
		}
		Opcode::CallIndirect => {
			let function_type_index = reader.read_leb128_u32()?;
			let table_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::CallIndirect(function_type_index, table_index))
		}

		Opcode::Br => {
			let depth = reader.read_leb128_u32()?;
			Ok(WasmInstruction::Br(depth))
		}
		Opcode::BrIf => {
			let depth = reader.read_leb128_u32()?;
			Ok(WasmInstruction::BrIf(depth))
		}
		Opcode::BrTable => {
			let count = reader.read_leb128_u32()?;
			let mut depth = vec![];

			for _ in 0..count {
				depth.push(reader.read_leb128_u32()?);
			}

			let labels = depth.into_boxed_slice();
			let default = reader.read_leb128_u32()?;
			Ok(WasmInstruction::BrTable {
				cases: labels,
				default,
			})
		}

		Opcode::Block => parse_block(reader),
		Opcode::Loop => parse_loop(reader),

		Opcode::Unreachable => Ok(WasmInstruction::Unreachable),

		Opcode::Drop => Ok(WasmInstruction::Drop),

		Opcode::GlobalGet => {
			let global_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::GlobalGet(global_index))
		}
		Opcode::GlobalSet => {
			let global_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::GlobalSet(global_index))
		}

		Opcode::LocalGet => {
			let local_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::LocalGet(local_index))
		}
		Opcode::LocalSet => {
			let local_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::LocalSet(local_index))
		}
		Opcode::LocalTee => {
			let local_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::LocalTee(local_index))
		}

		Opcode::I32Load => Ok(WasmInstruction::I32Load(parse_mem_arg(reader)?)),
		Opcode::I32Load8S => Ok(WasmInstruction::I32Load8S(parse_mem_arg(reader)?)),
		Opcode::I32Load8U => Ok(WasmInstruction::I32Load8U(parse_mem_arg(reader)?)),
		Opcode::I32Load16S => Ok(WasmInstruction::I32Load16S(parse_mem_arg(reader)?)),
		Opcode::I32Load16U => Ok(WasmInstruction::I32Load16U(parse_mem_arg(reader)?)),

		Opcode::I64Load => Ok(WasmInstruction::I64Load(parse_mem_arg(reader)?)),
		Opcode::I64Load8S => Ok(WasmInstruction::I64Load8S(parse_mem_arg(reader)?)),
		Opcode::I64Load8U => Ok(WasmInstruction::I64Load8U(parse_mem_arg(reader)?)),
		Opcode::I64Load16S => Ok(WasmInstruction::I64Load16S(parse_mem_arg(reader)?)),
		Opcode::I64Load16U => Ok(WasmInstruction::I64Load16U(parse_mem_arg(reader)?)),
		Opcode::I64Load32S => Ok(WasmInstruction::I64Load32S(parse_mem_arg(reader)?)),
		Opcode::I64Load32U => Ok(WasmInstruction::I64Load32U(parse_mem_arg(reader)?)),

		Opcode::F32Load => Ok(WasmInstruction::F32Load(parse_mem_arg(reader)?)),
		Opcode::F64Load => Ok(WasmInstruction::F64Load(parse_mem_arg(reader)?)),
		Opcode::I32Store => Ok(WasmInstruction::I32Store(parse_mem_arg(reader)?)),
		Opcode::I32Store8 => Ok(WasmInstruction::I32Store8(parse_mem_arg(reader)?)),
		Opcode::I32Store16 => Ok(WasmInstruction::I32Store16(parse_mem_arg(reader)?)),
		Opcode::I64Store => Ok(WasmInstruction::I64Store(parse_mem_arg(reader)?)),
		Opcode::I64Store8 => Ok(WasmInstruction::I64Store8(parse_mem_arg(reader)?)),
		Opcode::I64Store16 => Ok(WasmInstruction::I64Store16(parse_mem_arg(reader)?)),
		Opcode::I64Store32 => Ok(WasmInstruction::I64Store32(parse_mem_arg(reader)?)),
		Opcode::F32Store => Ok(WasmInstruction::F32Store(parse_mem_arg(reader)?)),
		Opcode::F64Store => Ok(WasmInstruction::F64Store(parse_mem_arg(reader)?)),
		Opcode::MemorySize => {
			let memory_index = reader.read_leb128_u32()?;
			Ok(WasmInstruction::MemorySize(memory_index))
		}
		Opcode::MemoryGrow => {
			let memory_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::MemoryGrow(memory_idx))
		}

		Opcode::I32LtS => Ok(WasmInstruction::I32LtS),
		Opcode::I32LtU => Ok(WasmInstruction::I32LtU),
		Opcode::I32GtS => Ok(WasmInstruction::I32GtS),
		Opcode::I32GtU => Ok(WasmInstruction::I32GtU),
		Opcode::I32LeS => Ok(WasmInstruction::I32LeS),
		Opcode::I32LeU => Ok(WasmInstruction::I32LeU),
		Opcode::I32GeS => Ok(WasmInstruction::I32GeS),
		Opcode::I32GeU => Ok(WasmInstruction::I32GeU),
		Opcode::I64LtS => Ok(WasmInstruction::I64LtS),
		Opcode::I64LtU => Ok(WasmInstruction::I64LtU),
		Opcode::I64GtS => Ok(WasmInstruction::I64GtS),
		Opcode::I64GtU => Ok(WasmInstruction::I64GtU),
		Opcode::I64LeS => Ok(WasmInstruction::I64LeS),
		Opcode::I64LeU => Ok(WasmInstruction::I64LeU),
		Opcode::I64GeS => Ok(WasmInstruction::I64GeS),
		Opcode::I64GeU => Ok(WasmInstruction::I64GeU),
		Opcode::F32Lt => Ok(WasmInstruction::F32Lt),
		Opcode::F32Gt => Ok(WasmInstruction::F32Gt),
		Opcode::F32Le => Ok(WasmInstruction::F32Le),
		Opcode::F32Ge => Ok(WasmInstruction::F32Ge),
		Opcode::F64Lt => Ok(WasmInstruction::F64Lt),
		Opcode::F64Gt => Ok(WasmInstruction::F64Gt),
		Opcode::F64Le => Ok(WasmInstruction::F64Le),
		Opcode::F64Ge => Ok(WasmInstruction::F64Ge),
		Opcode::I32RemS => Ok(WasmInstruction::I32RemS),
		Opcode::I32RemU => Ok(WasmInstruction::I32RemU),
		Opcode::I64RemS => Ok(WasmInstruction::I64RemS),
		Opcode::I64RemU => Ok(WasmInstruction::I64RemU),
		Opcode::I32Xor => Ok(WasmInstruction::I32Xor),
		Opcode::I64Xor => Ok(WasmInstruction::I64Xor),

		Opcode::F32Nearest => Ok(WasmInstruction::F32Nearest),
		Opcode::F32Div => Ok(WasmInstruction::F32Div),
		Opcode::F32Min => Ok(WasmInstruction::F32Min),
		Opcode::F32Max => Ok(WasmInstruction::F32Max),

		Opcode::F32Trunc => Ok(WasmInstruction::F32Trunc),
		Opcode::F64Trunc => Ok(WasmInstruction::F64Trunc),

		Opcode::I32TruncF32S => Ok(WasmInstruction::I32TruncF32S),
		Opcode::I32TruncSatF32S => Ok(WasmInstruction::I32TruncSatF32S),
		Opcode::I32TruncF32U => Ok(WasmInstruction::I32TruncF32U),
		Opcode::I32TruncSatF32U => Ok(WasmInstruction::I32TruncSatF32U),
		Opcode::I32TruncF64S => Ok(WasmInstruction::I32TruncF64S),
		Opcode::I32TruncSatF64S => Ok(WasmInstruction::I32TruncSatF64S),
		Opcode::I32TruncF64U => Ok(WasmInstruction::I32TruncF64U),
		Opcode::I32TruncSatF64U => Ok(WasmInstruction::I32TruncSatF64U),

		Opcode::I64TruncF32S => Ok(WasmInstruction::I64TruncF32S),
		Opcode::I64TruncSatF32S => Ok(WasmInstruction::I64TruncSatF32S),
		Opcode::I64TruncF32U => Ok(WasmInstruction::I64TruncF32U),
		Opcode::I64TruncSatF32U => Ok(WasmInstruction::I64TruncSatF32U),
		Opcode::I64TruncF64S => Ok(WasmInstruction::I64TruncF64S),
		Opcode::I64TruncSatF64S => Ok(WasmInstruction::I64TruncSatF64S),
		Opcode::I64TruncF64U => Ok(WasmInstruction::I64TruncF64U),
		Opcode::I64TruncSatF64U => Ok(WasmInstruction::I64TruncSatF64U),

		Opcode::F64Nearest => Ok(WasmInstruction::F64Nearest),
		Opcode::F64Div => Ok(WasmInstruction::F64Div),
		Opcode::F64Min => Ok(WasmInstruction::F64Min),
		Opcode::F64Max => Ok(WasmInstruction::F64Max),

		Opcode::Nop => Ok(WasmInstruction::Nop),

		Opcode::If => {
			let result_type = parse_result_type(reader)?;
			let mut then = vec![];

			let has_else = loop {
				let opcode = parse_opcode(reader)?;
				if opcode == Opcode::End {
					break false;
				}

				if opcode == Opcode::Else {
					break true;
				}
				let instruction = parse_instruction_of(opcode, reader)?;
				then.push(instruction.clone());
			};

			let then = then.into_boxed_slice();

			let otherwise = if has_else {
				parse_instructions(reader)?
			} else {
				Box::new([])
			};

			Ok(WasmInstruction::If {
				result_type,
				then,
				otherwise,
			})
		}

		Opcode::Return => Ok(WasmInstruction::Return),
		Opcode::Select => Ok(WasmInstruction::Select),
		Opcode::SelectT => {
			// Typed select — read and discard the type vector (we treat it like untyped select)
			let count = reader.read_leb128_u32()?;
			for _ in 0..count {
				let _ = parse_value_type(reader)?;
			}
			Ok(WasmInstruction::Select)
		}
		Opcode::I32WrapI64 => Ok(WasmInstruction::I32WrapI64),

		Opcode::MemoryCopy => {
			let _src = reader.read_leb128_u32()?;
			let _dst = reader.read_leb128_u32()?;
			Ok(WasmInstruction::MemoryCopy)
		}
		Opcode::MemoryFill => {
			let _mem = reader.read_leb128_u32()?;
			Ok(WasmInstruction::MemoryFill)
		}
		Opcode::MemoryInit => {
			let data_idx = reader.read_leb128_u32()?;
			let _mem = reader.read_leb128_u32()?;
			Ok(WasmInstruction::MemoryInit(data_idx))
		}
		Opcode::DataDrop => {
			let data_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::DataDrop(data_idx))
		}

		Opcode::TableInit => {
			let elem_idx = reader.read_leb128_u32()?;
			let table_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableInit(table_idx, elem_idx))
		}
		Opcode::ElemDrop => {
			let elem_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::ElemDrop(elem_idx))
		}
		Opcode::TableCopy => {
			let dst = reader.read_leb128_u32()?;
			let src = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableCopy(dst, src))
		}
		Opcode::TableGrow => {
			let table_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableGrow(table_idx))
		}
		Opcode::TableSize => {
			let table_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableSize(table_idx))
		}
		Opcode::TableFill => {
			let table_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableFill(table_idx))
		}

		Opcode::TableGet => {
			let table_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableGet(table_idx))
		}
		Opcode::TableSet => {
			let table_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::TableSet(table_idx))
		}

		Opcode::NullRef => {
			let type_byte = reader.read_u8()?;
			let vt = value_type_from_u8(type_byte)?;
			Ok(WasmInstruction::RefNull(vt))
		}
		Opcode::RefIsNull => Ok(WasmInstruction::RefIsNull),
		Opcode::FuncRef => {
			let func_idx = reader.read_leb128_u32()?;
			Ok(WasmInstruction::RefFunc(func_idx))
		}

		_ => Err(WasmParseError::UnsupportedOpcode(opcode)),
	}
}

// ---------------------------------------------------------------------------
// Block / Loop parsing
// ---------------------------------------------------------------------------

fn parse_block(reader: &ByteReader) -> Result<WasmInstruction> {
	parse_block_or_loop(Opcode::Block, reader)
}

fn parse_loop(reader: &ByteReader) -> Result<WasmInstruction> {
	parse_block_or_loop(Opcode::Loop, reader)
}

fn parse_block_or_loop(op: Opcode, reader: &ByteReader) -> Result<WasmInstruction> {
	if op != Opcode::Block && op != Opcode::Loop {
		return Err(WasmParseError::InvalidOpcode(op as u8));
	}

	let result_type = parse_result_type(reader)?;
	let body = parse_instructions(reader)?;

	if op == Opcode::Block {
		Ok(WasmInstruction::Block {
			result_type,
			body,
		})
	} else {
		Ok(WasmInstruction::Loop {
			result_type,
			body,
		})
	}
}

fn parse_result_type(reader: &ByteReader) -> Result<WasmResultType> {
	let result_type = reader.read_u8()?;
	Ok(match result_type {
		0x40 => WasmResultType::None,
		0x7F => WasmResultType::FromValue(WasmValueType::try_from(0x7F).map_err(|v| InvalidValueType(v))?),
		0x7E => WasmResultType::FromValue(WasmValueType::try_from(0x7E).map_err(|v| InvalidValueType(v))?),
		0x7D => WasmResultType::FromValue(WasmValueType::try_from(0x7D).map_err(|v| InvalidValueType(v))?),
		0x7C => WasmResultType::FromValue(WasmValueType::try_from(0x7C).map_err(|v| InvalidValueType(v))?),
		0x70 => WasmResultType::FromValue(WasmValueType::try_from(0x70).map_err(|v| InvalidValueType(v))?),
		0x6F => WasmResultType::FromValue(WasmValueType::try_from(0x6F).map_err(|v| InvalidValueType(v))?),
		index => WasmResultType::FromType(index as u32),
	})
}

// ---------------------------------------------------------------------------
// Value type helpers
// ---------------------------------------------------------------------------

fn parse_value_types(size: u32, reader: &ByteReader) -> Result<Box<[WasmValueType]>> {
	let mut result = vec![];
	for _ in 0..size {
		result.push(parse_value_type(reader)?);
	}
	Ok(result.into())
}

fn parse_value_type(reader: &ByteReader) -> Result<WasmValueType> {
	let value_type = reader.read_u8()?;
	value_type_from_u8(value_type)
}

fn value_type_from_u8(value: u8) -> Result<WasmValueType> {
	value.try_into().map_err(|value| InvalidValueType(value))
}

// ---------------------------------------------------------------------------
// Limit helpers
// ---------------------------------------------------------------------------

fn parse_limits(reader: &ByteReader) -> Result<WasmResizableLimit> {
	let flags = reader.read_leb128_u32()?;
	let min = reader.read_leb128_u32()?;

	let max = if flags == 0 {
		None
	} else {
		let max = reader.read_leb128_u32()?;
		Some(max)
	};

	Ok(WasmResizableLimit {
		min,
		max,
	})
}

// ---------------------------------------------------------------------------
// Name helpers
// ---------------------------------------------------------------------------

fn parse_name(reader: &ByteReader) -> Result<Box<[u8]>> {
	let size = reader.read_leb128_u32()?;
	let name = reader.read_range(size as usize)?;
	Ok(name)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
	use crate::{
		parse::{
			WasmParseError::{InvalidMagicNumber, UnexpectedEndOfFile, UnsupportedVersion},
			binary::WasmParser,
		},
		util::byte_reader::ByteReader,
	};

	#[test]
	fn nothing_to_decode() {
		let err = WasmParser::parse([0u8, 0].as_ref()).err().unwrap();
		assert_eq!(err, UnexpectedEndOfFile);
	}

	#[test]
	fn invalid_magic_number() {
		let err = WasmParser::parse(&[0x00, 0x6D, 0x73, 0x61]).err().unwrap();
		assert_eq!(err, InvalidMagicNumber);
	}

	#[test]
	fn invalid_version() {
		let given_bytes = &2_i32.to_le_bytes();
		let reader = ByteReader::new(given_bytes.as_ref());
		let err = WasmParser::parse_version(&reader).err().unwrap();
		assert_eq!(err, UnsupportedVersion(2));
	}
}
