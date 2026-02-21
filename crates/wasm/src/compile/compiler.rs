// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use crate::{
	HostMemory, HostTable,
	module::{
		DataSegment, ElementSegment, Export, ExportData, Function, FunctionExternal, FunctionIndex,
		FunctionType, Global, GlobalIndex, Instruction, Memory, MemoryArgument, MemoryIndex, Module, ModuleId,
		PAGE_SIZE, Table, TableLimit, Value, ValueType,
	},
	parse::{
		WasmDataMode, WasmElementMode, WasmExportDescriptor, WasmFunc, WasmGlobalInit, WasmGlobalType,
		WasmImportDescriptor, WasmInstruction, WasmMemoryArgument, WasmModule, WasmResultType, WasmValueType,
	},
};

pub struct Compiler {}

impl Default for Compiler {
	fn default() -> Self {
		Self {}
	}
}

pub enum CompilationError {
	PlaceHolder,
	OutOfBoundsMemoryAccess,
	OutOfBoundsTableAccess,
}

impl std::fmt::Display for CompilationError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			CompilationError::PlaceHolder => todo!(),
			CompilationError::OutOfBoundsMemoryAccess => write!(f, "out of bounds memory access"),
			CompilationError::OutOfBoundsTableAccess => write!(f, "out of bounds table access"),
		}
	}
}

fn convert_value_type(wt: &WasmValueType) -> ValueType {
	match wt {
		WasmValueType::I32 => ValueType::I32,
		WasmValueType::I64 => ValueType::I64,
		WasmValueType::F32 => ValueType::F32,
		WasmValueType::F64 => ValueType::F64,
		WasmValueType::FuncRef => ValueType::RefFunc,
		WasmValueType::ExternRef => ValueType::RefExtern,
	}
}

impl Compiler {
	pub fn new() -> Self {
		Self {}
	}

	pub fn compile(&self, id: ModuleId, wasm: WasmModule) -> Result<Module, CompilationError> {
		self.compile_with_imports(id, wasm, &[], &[], &[])
	}

	pub fn compile_with_imports(
		&self,
		id: ModuleId,
		wasm: WasmModule,
		host_globals: &[(String, String, Value)],
		host_memories: &[(String, String, HostMemory)],
		host_tables: &[(String, String, HostTable)],
	) -> Result<Module, CompilationError> {
		let func_type_addrs = match wasm.functions {
			ref addr => addr.clone(),
			_ => Box::default(),
		};

		let mut functions: Vec<Arc<Function>> = vec![];
		let mut memories: Vec<Memory> = vec![];
		let mut tables: Vec<Table> = vec![];
		let mut function_types: Vec<FunctionType> = vec![];
		let mut globals: Vec<Global> = vec![];

		for function_type in &wasm.types {
			function_types.push(FunctionType {
				params: function_type.params.iter().map(|p| convert_value_type(p)).collect(),
				results: function_type.results.iter().map(|p| convert_value_type(p)).collect(),
			})
		}

		if let ref import_section = wasm.imports {
			for import in import_section {
				let module_name = import.module.clone();
				let field = import.name.clone();
				match &import.desc {
					WasmImportDescriptor::Function(type_idx) => {
						let ref func_types = wasm.types else {
							panic!("not found type_section")
						};

						let Some(func_type) = func_types.get(*type_idx as usize) else {
							panic!("not found func types in type_section")
						};

						let func = Function::External(FunctionExternal {
							module: std::str::from_utf8(&*module_name).unwrap().to_string(),
							function_name: std::str::from_utf8(&*field)
								.unwrap()
								.to_string(),
							function_type: FunctionType {
								params: func_type
									.params
									.iter()
									.map(|p| convert_value_type(p))
									.collect(),
								results: func_type
									.results
									.iter()
									.map(|p| convert_value_type(p))
									.collect(),
							},
						});
						functions.push(Arc::new(func));
					}
					WasmImportDescriptor::Table(table) => {
						let mod_name = std::str::from_utf8(&*module_name).unwrap_or("");
						let field_name = std::str::from_utf8(&*field).unwrap_or("");
						let min = host_tables
							.iter()
							.find(|(m, n, _)| m == mod_name && n == field_name)
							.map(|(_, _, t)| t.min.max(table.limits.min))
							.unwrap_or(table.limits.min);
						let max = host_tables
							.iter()
							.find(|(m, n, _)| m == mod_name && n == field_name)
							.and_then(|(_, _, t)| t.max)
							.or(table.limits.max);
						tables.push(Table {
							elements: vec![None; min as usize],
							limit: TableLimit {
								min,
								max,
							},
						});
					}
					WasmImportDescriptor::Memory(mem) => {
						let mod_name = std::str::from_utf8(&*module_name).unwrap_or("");
						let field_name = std::str::from_utf8(&*field).unwrap_or("");
						let min_pages = host_memories
							.iter()
							.find(|(m, n, _)| m == mod_name && n == field_name)
							.map(|(_, _, m)| m.min_pages.max(mem.limits.min))
							.unwrap_or(mem.limits.min);
						let max = host_memories
							.iter()
							.find(|(m, n, _)| m == mod_name && n == field_name)
							.and_then(|(_, _, m)| m.max_pages)
							.or(mem.limits.max);
						let min = min_pages * PAGE_SIZE;
						memories.push(Memory {
							data: vec![0; min as usize],
							max,
						});
					}
					WasmImportDescriptor::Global(global) => {
						let mod_name = std::str::from_utf8(&*module_name).unwrap_or("");
						let field_name = std::str::from_utf8(&*field).unwrap_or("");
						let value = host_globals
							.iter()
							.find(|(m, n, _)| m == mod_name && n == field_name)
							.map(|(_, _, v)| v.clone())
							.unwrap_or_else(|| match &global.value_type {
								WasmValueType::I32 => Value::I32(0),
								WasmValueType::I64 => Value::I64(0),
								WasmValueType::F32 => Value::F32(0.0),
								WasmValueType::F64 => Value::F64(0.0),
								WasmValueType::FuncRef => {
									Value::RefNull(ValueType::RefFunc)
								}
								WasmValueType::ExternRef => {
									Value::RefNull(ValueType::RefExtern)
								}
							});
						globals.push(Global {
							mutable: global.mutable,
							value,
						});
					}
				};
			}
		}

		if let ref code_section = wasm.codes {
			for (func_body, type_idx) in code_section.iter().zip(func_type_addrs.into_iter()) {
				let ref func_types = wasm.types else {
					panic!("not found type_section")
				};

				let Some(func_type) = func_types.get(type_idx as usize) else {
					panic!("not found func types in type_section")
				};

				let mut locals: Vec<ValueType> = Vec::with_capacity(func_body.locals.len());
				for local in func_body.locals.iter() {
					for _ in 0..local.0 {
						locals.push(convert_value_type(&local.1));
					}
				}

				functions.push(Arc::new(Function::local(
					FunctionType::new(
						func_type
							.params
							.iter()
							.map(|p| convert_value_type(p))
							.collect::<Vec<_>>()
							.into(),
						func_type
							.results
							.iter()
							.map(|r| convert_value_type(r))
							.collect::<Vec<_>>()
							.into(),
					),
					locals.into(),
					func_body
						.code
						.iter()
						.map(|instruction| convert_instruction(instruction, &wasm))
						.collect(),
				)))
			}
		}

		let mut exports = Vec::with_capacity(wasm.exports.len());
		if let ref sections = wasm.exports {
			for export in sections {
				let name = std::str::from_utf8(&*export.name).unwrap().to_string();
				let export_inst = Export {
					name: name.clone(),
					data: match export.desc {
						WasmExportDescriptor::Func(v) => {
							ExportData::Function(v as FunctionIndex)
						}
						WasmExportDescriptor::Table(idx) => ExportData::Table(idx as usize),
						WasmExportDescriptor::Memory(idx) => {
							ExportData::Memory(idx as MemoryIndex)
						}
						WasmExportDescriptor::Global(idx) => {
							ExportData::Global(idx as GlobalIndex)
						}
					},
				};
				exports.push(export_inst);
			}
		};

		if let ref sections = wasm.memories {
			for memory in sections {
				let min = memory.limits.min * PAGE_SIZE;
				let memory = Memory {
					data: vec![0; min as usize],
					max: memory.limits.max,
				};
				memories.push(memory);
			}
		}

		let mut data_segments: Vec<DataSegment> = Vec::with_capacity(wasm.data.len());

		for data in &wasm.data {
			match &data.mode {
				WasmDataMode::Active {
					index,
					offset,
				} => {
					let mem = memories.get_mut(*index as usize).unwrap();
					let offset = *offset as usize;
					let init = &data.data;
					if offset.checked_add(init.len()).map_or(true, |end| end > mem.data.len()) {
						return Err(CompilationError::OutOfBoundsMemoryAccess);
					}
					mem.data[offset..offset + init.len()].copy_from_slice(init);
					// Keep the data available for memory.init
					data_segments.push(DataSegment {
						data: Some(init.clone()),
					});
				}
				WasmDataMode::Passive => {
					// Passive data segments are not copied into memory at init time.
					// They are only used by memory.init.
					data_segments.push(DataSegment {
						data: Some(data.data.clone()),
					});
				}
			}
		}

		for table in &wasm.tables {
			tables.push(Table {
				elements: vec![None; table.limits.min as usize],
				limit: TableLimit {
					min: table.limits.min,
					max: table.limits.max,
				},
			});
		}

		let mut element_segments: Vec<ElementSegment> = Vec::with_capacity(wasm.elements.len());

		for element in &wasm.elements {
			match &element.mode {
				WasmElementMode::Active {
					table: table_idx,
					offset,
				} => {
					let table = tables.get_mut(*table_idx as usize).unwrap();

					let offset = match offset[offset.len() - 1] {
						WasmInstruction::I32Const(i) => i as usize,
						WasmInstruction::GlobalGet(idx) => match globals.get(idx as usize) {
							Some(g) => match g.value {
								Value::I32(v) => v as usize,
								_ => return Err(
									CompilationError::OutOfBoundsTableAccess,
								),
							},
							None => return Err(CompilationError::OutOfBoundsTableAccess),
						},
						_ => return Err(CompilationError::OutOfBoundsTableAccess),
					};

					if offset
						.checked_add(element.init.len())
						.map_or(true, |end| end > table.elements.len())
					{
						return Err(CompilationError::OutOfBoundsTableAccess);
					}

					for i in 0..element.init.len() {
						table.elements[offset + i] =
							Some(Value::RefFunc(element.init[i] as FunctionIndex));
					}
					// Active segments are dropped after initialization per spec
					element_segments.push(ElementSegment {
						elements: None,
					});
				}
				WasmElementMode::Passive => {
					let elems: Box<[Option<usize>]> =
						element.init.iter().map(|idx| Some(*idx as usize)).collect();
					element_segments.push(ElementSegment {
						elements: Some(elems),
					});
				}
				WasmElementMode::Declarative => {
					element_segments.push(ElementSegment {
						elements: None,
					});
				}
			}
		}

		for global in &wasm.globals {
			let value = match &global.init {
				WasmGlobalInit::I32(v) => Value::I32(v.clone()),
				WasmGlobalInit::I64(v) => Value::I64(v.clone()),
				WasmGlobalInit::F32(v) => Value::F32(v.clone()),
				WasmGlobalInit::F64(v) => Value::F64(v.clone()),
				WasmGlobalInit::Global(idx) => globals
					.get(*idx as usize)
					.expect("global reference out of bounds")
					.value
					.clone(),
				WasmGlobalInit::NullRef(value_type) => Value::RefNull(convert_value_type(value_type)),
				WasmGlobalInit::FuncRef(idx) => Value::RefFunc(idx.clone() as usize),
			};

			globals.push(Global {
				mutable: global.mutable.clone(),
				value,
			})
		}

		Ok(Module::with_segments(
			id,
			exports.into(),
			functions.into(),
			function_types.into(),
			globals.into(),
			memories.into(),
			tables.into(),
			data_segments.into(),
			element_segments.into(),
			wasm.start_function.map(|idx| idx as FunctionIndex),
		))
	}
}

fn convert_instructions(module: &WasmModule, instructions: &Box<[WasmInstruction]>) -> Box<[Instruction]> {
	let mut result = Vec::with_capacity(instructions.len());

	instructions.iter().for_each(|i| result.push(convert_instruction(i, module)));

	result.into_boxed_slice()
}

fn convert_result_types(module: &WasmModule, result_type: &WasmResultType) -> Box<[ValueType]> {
	match result_type {
		WasmResultType::None => Box::new([]),
		WasmResultType::FromValue(vt) => Box::new([convert_value_type(vt)]),
		WasmResultType::FromType(type_idx) => {
			let func: &WasmFunc = module.types.get(*type_idx as usize).unwrap();
			func.results.iter().map(|vt| convert_value_type(vt)).collect()
		}
	}
}

fn convert_param_types(module: &WasmModule, result_type: &WasmResultType) -> Box<[ValueType]> {
	match result_type {
		WasmResultType::None | WasmResultType::FromValue(_) => Box::new([]),
		WasmResultType::FromType(type_idx) => {
			let func: &WasmFunc = module.types.get(*type_idx as usize).unwrap();
			func.params.iter().map(|vt| convert_value_type(vt)).collect()
		}
	}
}

fn convert_memory_arg(arg: &WasmMemoryArgument) -> MemoryArgument {
	MemoryArgument {
		align: arg.align,
		offset: arg.offset,
	}
}

pub fn convert_instruction(wasm_instruction: &WasmInstruction, module: &WasmModule) -> Instruction {
	match wasm_instruction {
		WasmInstruction::Unreachable => Instruction::Unreachable,
		WasmInstruction::Nop => Instruction::Nop,
		WasmInstruction::Block {
			result_type,
			body,
		} => Instruction::Block {
			result_types: convert_result_types(module, result_type),
			body: body.iter().map(|instr| convert_instruction(instr, module)).collect(),
		},
		WasmInstruction::Loop {
			result_type,
			body,
		} => Instruction::Loop {
			param_types: convert_param_types(module, result_type),
			result_types: convert_result_types(module, result_type),
			body: body.iter().map(|instr| convert_instruction(instr, module)).collect(),
		},
		WasmInstruction::If {
			result_type,
			then,
			otherwise,
		} => Instruction::If {
			result_types: convert_result_types(module, result_type),
			then: then.iter().map(|instr| convert_instruction(instr, module)).collect(),
			otherwise: otherwise.iter().map(|instr| convert_instruction(instr, module)).collect(),
		},
		WasmInstruction::Else => Instruction::Else,
		WasmInstruction::Br(depth) => Instruction::Br(*depth as usize),
		WasmInstruction::BrIf(depth) => Instruction::BrIf(*depth as usize),
		WasmInstruction::BrTable {
			cases,
			default,
		} => Instruction::BrTable {
			cases: cases.iter().map(|c| c.clone() as usize).collect::<Vec<_>>().into(),
			default: *default as usize,
		},
		WasmInstruction::Return => Instruction::Return,
		WasmInstruction::Call(index) => Instruction::Call(*index as usize),
		WasmInstruction::CallIndirect(type_index, table_index) => {
			Instruction::CallIndirect(*type_index as usize, *table_index as usize)
		}
		WasmInstruction::Drop => Instruction::Drop,
		WasmInstruction::Select => Instruction::Select,
		WasmInstruction::LocalGet(index) => Instruction::LocalGet(*index as usize),
		WasmInstruction::LocalSet(index) => Instruction::LocalSet(*index as usize),
		WasmInstruction::LocalTee(index) => Instruction::LocalTee(*index as usize),
		WasmInstruction::GlobalGet(index) => Instruction::GlobalGet(*index as usize),
		WasmInstruction::GlobalSet(index) => Instruction::GlobalSet(*index as usize),
		WasmInstruction::I32Load(arg) => Instruction::I32Load(convert_memory_arg(arg)),
		WasmInstruction::I64Load(arg) => Instruction::I64Load(convert_memory_arg(arg)),
		WasmInstruction::F32Load(arg) => Instruction::F32Load(convert_memory_arg(arg)),
		WasmInstruction::F64Load(arg) => Instruction::F64Load(convert_memory_arg(arg)),
		WasmInstruction::I32Load8S(arg) => Instruction::I32Load8S(convert_memory_arg(arg)),
		WasmInstruction::I32Load8U(arg) => Instruction::I32Load8U(convert_memory_arg(arg)),
		WasmInstruction::I32Load16S(arg) => Instruction::I32Load16S(convert_memory_arg(arg)),
		WasmInstruction::I32Load16U(arg) => Instruction::I32Load16U(convert_memory_arg(arg)),
		WasmInstruction::I64Load8S(arg) => Instruction::I64Load8S(convert_memory_arg(arg)),
		WasmInstruction::I64Load8U(arg) => Instruction::I64Load8U(convert_memory_arg(arg)),
		WasmInstruction::I64Load16S(arg) => Instruction::I64Load16S(convert_memory_arg(arg)),
		WasmInstruction::I64Load16U(arg) => Instruction::I64Load16U(convert_memory_arg(arg)),
		WasmInstruction::I64Load32S(arg) => Instruction::I64Load32S(convert_memory_arg(arg)),
		WasmInstruction::I64Load32U(arg) => Instruction::I64Load32U(convert_memory_arg(arg)),
		WasmInstruction::I32Store(arg) => Instruction::I32Store(convert_memory_arg(arg)),
		WasmInstruction::I64Store(arg) => Instruction::I64Store(convert_memory_arg(arg)),
		WasmInstruction::F32Store(arg) => Instruction::F32Store(convert_memory_arg(arg)),
		WasmInstruction::F64Store(arg) => Instruction::F64Store(convert_memory_arg(arg)),
		WasmInstruction::I32Store8(arg) => Instruction::I32Store8(convert_memory_arg(arg)),
		WasmInstruction::I32Store16(arg) => Instruction::I32Store16(convert_memory_arg(arg)),
		WasmInstruction::I64Store8(arg) => Instruction::I64Store8(convert_memory_arg(arg)),
		WasmInstruction::I64Store16(arg) => Instruction::I64Store16(convert_memory_arg(arg)),
		WasmInstruction::I64Store32(arg) => Instruction::I64Store32(convert_memory_arg(arg)),
		WasmInstruction::MemorySize(index) => Instruction::MemorySize(*index as usize),
		WasmInstruction::MemoryGrow(index) => Instruction::MemoryGrow(*index as usize),
		WasmInstruction::I32Const(value) => Instruction::I32Const(*value),
		WasmInstruction::I64Const(value) => Instruction::I64Const(*value),
		WasmInstruction::F32Const(value) => Instruction::F32Const(*value),
		WasmInstruction::F64Const(value) => Instruction::F64Const(*value),
		WasmInstruction::I32Eqz => Instruction::I32Eqz,
		WasmInstruction::I32Eq => Instruction::I32Eq,
		WasmInstruction::I32Ne => Instruction::I32Ne,
		WasmInstruction::I32LtS => Instruction::I32LtS,
		WasmInstruction::I32LtU => Instruction::I32LtU,
		WasmInstruction::I32GtS => Instruction::I32GtS,
		WasmInstruction::I32GtU => Instruction::I32GtU,
		WasmInstruction::I32LeS => Instruction::I32LeS,
		WasmInstruction::I32LeU => Instruction::I32LeU,
		WasmInstruction::I32GeS => Instruction::I32GeS,
		WasmInstruction::I32GeU => Instruction::I32GeU,
		WasmInstruction::I64Eqz => Instruction::I64Eqz,
		WasmInstruction::I64Eq => Instruction::I64Eq,
		WasmInstruction::I64Ne => Instruction::I64Ne,
		WasmInstruction::I64LtS => Instruction::I64LtS,
		WasmInstruction::I64LtU => Instruction::I64LtU,
		WasmInstruction::I64GtS => Instruction::I64GtS,
		WasmInstruction::I64GtU => Instruction::I64GtU,
		WasmInstruction::I64LeS => Instruction::I64LeS,
		WasmInstruction::I64LeU => Instruction::I64LeU,
		WasmInstruction::I64GeS => Instruction::I64GeS,
		WasmInstruction::I64GeU => Instruction::I64GeU,
		WasmInstruction::F32Eq => Instruction::F32Eq,
		WasmInstruction::F32Ne => Instruction::F32Ne,
		WasmInstruction::F32Lt => Instruction::F32Lt,
		WasmInstruction::F32Gt => Instruction::F32Gt,
		WasmInstruction::F32Le => Instruction::F32Le,
		WasmInstruction::F32Ge => Instruction::F32Ge,
		WasmInstruction::F64Eq => Instruction::F64Eq,
		WasmInstruction::F64Ne => Instruction::F64Ne,
		WasmInstruction::F64Lt => Instruction::F64Lt,
		WasmInstruction::F64Gt => Instruction::F64Gt,
		WasmInstruction::F64Le => Instruction::F64Le,
		WasmInstruction::F64Ge => Instruction::F64Ge,
		WasmInstruction::I32Clz => Instruction::I32Clz,
		WasmInstruction::I32Ctz => Instruction::I32Ctz,
		WasmInstruction::I32Popcnt => Instruction::I32Popcnt,
		WasmInstruction::I32Add => Instruction::I32Add,
		WasmInstruction::I32Sub => Instruction::I32Sub,
		WasmInstruction::I32Mul => Instruction::I32Mul,
		WasmInstruction::I32DivS => Instruction::I32DivS,
		WasmInstruction::I32DivU => Instruction::I32DivU,
		WasmInstruction::I32RemS => Instruction::I32RemS,
		WasmInstruction::I32RemU => Instruction::I32RemU,
		WasmInstruction::I32And => Instruction::I32And,
		WasmInstruction::I32Or => Instruction::I32Or,
		WasmInstruction::I32Xor => Instruction::I32Xor,
		WasmInstruction::I32Shl => Instruction::I32Shl,
		WasmInstruction::I32ShrS => Instruction::I32ShrS,
		WasmInstruction::I32ShrU => Instruction::I32ShrU,
		WasmInstruction::I32Rotl => Instruction::I32Rotl,
		WasmInstruction::I32Rotr => Instruction::I32Rotr,
		WasmInstruction::I64Clz => Instruction::I64Clz,
		WasmInstruction::I64Ctz => Instruction::I64Ctz,
		WasmInstruction::I64Popcnt => Instruction::I64Popcnt,
		WasmInstruction::I64Add => Instruction::I64Add,
		WasmInstruction::I64Sub => Instruction::I64Sub,
		WasmInstruction::I64Mul => Instruction::I64Mul,
		WasmInstruction::I64DivS => Instruction::I64DivS,
		WasmInstruction::I64DivU => Instruction::I64DivU,
		WasmInstruction::I64RemS => Instruction::I64RemS,
		WasmInstruction::I64RemU => Instruction::I64RemU,
		WasmInstruction::I64And => Instruction::I64And,
		WasmInstruction::I64Or => Instruction::I64Or,
		WasmInstruction::I64Xor => Instruction::I64Xor,
		WasmInstruction::I64Shl => Instruction::I64Shl,
		WasmInstruction::I64ShrS => Instruction::I64ShrS,
		WasmInstruction::I64ShrU => Instruction::I64ShrU,
		WasmInstruction::I64Rotl => Instruction::I64Rotl,
		WasmInstruction::I64Rotr => Instruction::I64Rotr,
		WasmInstruction::F32Abs => Instruction::F32Abs,
		WasmInstruction::F32Neg => Instruction::F32Neg,
		WasmInstruction::F32Ceil => Instruction::F32Ceil,
		WasmInstruction::F32Floor => Instruction::F32Floor,
		WasmInstruction::F32Trunc => Instruction::F32Trunc,
		WasmInstruction::F32Nearest => Instruction::F32Nearest,
		WasmInstruction::F32Sqrt => Instruction::F32Sqrt,
		WasmInstruction::F32Add => Instruction::F32Add,
		WasmInstruction::F32Sub => Instruction::F32Sub,
		WasmInstruction::F32Mul => Instruction::F32Mul,
		WasmInstruction::F32Div => Instruction::F32Div,
		WasmInstruction::F32Min => Instruction::F32Min,
		WasmInstruction::F32Max => Instruction::F32Max,
		WasmInstruction::F32Copysign => Instruction::F32Copysign,
		WasmInstruction::F64Abs => Instruction::F64Abs,
		WasmInstruction::F64Neg => Instruction::F64Neg,
		WasmInstruction::F64Ceil => Instruction::F64Ceil,
		WasmInstruction::F64Floor => Instruction::F64Floor,
		WasmInstruction::F64Trunc => Instruction::F64Trunc,
		WasmInstruction::F64Nearest => Instruction::F64Nearest,
		WasmInstruction::F64Sqrt => Instruction::F64Sqrt,
		WasmInstruction::F64Add => Instruction::F64Add,
		WasmInstruction::F64Sub => Instruction::F64Sub,
		WasmInstruction::F64Mul => Instruction::F64Mul,
		WasmInstruction::F64Div => Instruction::F64Div,
		WasmInstruction::F64Min => Instruction::F64Min,
		WasmInstruction::F64Max => Instruction::F64Max,
		WasmInstruction::F64Copysign => Instruction::F64Copysign,
		WasmInstruction::I32WrapI64 => Instruction::I32WrapI64,
		WasmInstruction::I32TruncF32S => Instruction::I32TruncF32S,
		WasmInstruction::I32TruncSatF32S => Instruction::I32TruncSatF32S,
		WasmInstruction::I32TruncF32U => Instruction::I32TruncF32U,
		WasmInstruction::I32TruncSatF32U => Instruction::I32TruncSatF32U,
		WasmInstruction::I32TruncF64S => Instruction::I32TruncF64S,
		WasmInstruction::I32TruncSatF64S => Instruction::I32TruncSatF64S,
		WasmInstruction::I32TruncF64U => Instruction::I32TruncF64U,
		WasmInstruction::I32TruncSatF64U => Instruction::I32TruncSatF64U,
		WasmInstruction::I64ExtendI32S => Instruction::I64ExtendI32S,
		WasmInstruction::I64ExtendI32U => Instruction::I64ExtendI32U,
		WasmInstruction::I64TruncF32S => Instruction::I64TruncF32S,
		WasmInstruction::I64TruncSatF32S => Instruction::I64TruncSatF32S,
		WasmInstruction::I64TruncF32U => Instruction::I64TruncF32U,
		WasmInstruction::I64TruncSatF32U => Instruction::I64TruncSatF32U,
		WasmInstruction::I64TruncF64S => Instruction::I64TruncF64S,
		WasmInstruction::I64TruncSatF64S => Instruction::I64TruncSatF64S,
		WasmInstruction::I64TruncF64U => Instruction::I64TruncF64U,
		WasmInstruction::I64TruncSatF64U => Instruction::I64TruncSatF64U,
		WasmInstruction::F32ConvertI32S => Instruction::F32ConvertI32S,
		WasmInstruction::F32ConvertI32U => Instruction::F32ConvertI32U,
		WasmInstruction::F32ConvertI64S => Instruction::F32ConvertI64S,
		WasmInstruction::F32ConvertI64U => Instruction::F32ConvertI64U,
		WasmInstruction::F32DemoteF64 => Instruction::F32DemoteF64,
		WasmInstruction::F64ConvertI32S => Instruction::F64ConvertI32S,
		WasmInstruction::F64ConvertI32U => Instruction::F64ConvertI32U,
		WasmInstruction::F64ConvertI64S => Instruction::F64ConvertI64S,
		WasmInstruction::F64ConvertI64U => Instruction::F64ConvertI64U,
		WasmInstruction::F64PromoteF32 => Instruction::F64PromoteF32,
		WasmInstruction::I32ReinterpretF32 => Instruction::I32ReinterpretF32,
		WasmInstruction::I64ReinterpretF64 => Instruction::I64ReinterpretF64,
		WasmInstruction::F32ReinterpretI32 => Instruction::F32ReinterpretI32,
		WasmInstruction::F64ReinterpretI64 => Instruction::F64ReinterpretI64,
		WasmInstruction::I32Extend8S => Instruction::I32Extend8S,
		WasmInstruction::I32Extend16S => Instruction::I32Extend16S,
		WasmInstruction::I64Extend8S => Instruction::I64Extend8S,
		WasmInstruction::I64Extend16S => Instruction::I64Extend16S,
		WasmInstruction::I64Extend32S => Instruction::I64Extend32S,

		WasmInstruction::MemoryCopy => Instruction::MemoryCopy,
		WasmInstruction::MemoryFill => Instruction::MemoryFill,
		WasmInstruction::MemoryInit(data_idx) => Instruction::MemoryInit(*data_idx),
		WasmInstruction::DataDrop(data_idx) => Instruction::DataDrop(*data_idx),

		WasmInstruction::TableGet(idx) => Instruction::TableGet(*idx as usize),
		WasmInstruction::TableSet(idx) => Instruction::TableSet(*idx as usize),
		WasmInstruction::TableGrow(idx) => Instruction::TableGrow(*idx as usize),
		WasmInstruction::TableSize(idx) => Instruction::TableSize(*idx as usize),
		WasmInstruction::TableFill(idx) => Instruction::TableFill(*idx as usize),
		WasmInstruction::TableCopy(dst, src) => Instruction::TableCopy(*dst as usize, *src as usize),
		WasmInstruction::TableInit(table_idx, elem_idx) => {
			Instruction::TableInit(*table_idx as usize, *elem_idx)
		}
		WasmInstruction::ElemDrop(idx) => Instruction::ElemDrop(*idx),

		WasmInstruction::RefNull(vt) => Instruction::RefNull(convert_value_type(vt)),
		WasmInstruction::RefIsNull => Instruction::RefIsNull,
		WasmInstruction::RefFunc(idx) => Instruction::RefFunc(*idx as usize),
	}
}
