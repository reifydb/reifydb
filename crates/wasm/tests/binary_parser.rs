// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_wasm::parse::{
	WasmData, WasmDataMode, WasmElement, WasmElementMode, WasmExport, WasmExportDescriptor, WasmFunc,
	WasmFunctionBody, WasmImport, WasmImportDescriptor, WasmInstruction, WasmMemory, WasmMemoryArgument,
	WasmParser, WasmResizableLimit, WasmResultType, WasmTable, WasmValueType,
};

fn wasm(content: &str) -> Box<[u8]> {
	wat::parse_str(content).unwrap().into_boxed_slice()
}

#[test]
fn parse_empty_module() {
	let wasm = wasm(r#"(module)"#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(result.types.as_ref(), []);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), []);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function.as_ref(), None);
	assert_eq!(result.codes.as_ref(), []);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_empty_function() {
	let wasm = wasm("(module (func))");
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(result.types.as_ref(), [WasmFunc::default()]);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::default(),
			code: Box::new([]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_func_with_params() {
	let wasm = wasm("(module (func (param i32 i64)))");
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(
		result.types.as_ref(),
		[WasmFunc {
			params: Box::new([WasmValueType::I32, WasmValueType::I64]),
			results: Box::default()
		}]
	);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::default(),
			code: Box::new([]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_func_with_locals() {
	let wasm = wasm(r#"
        (module
          (func
            (local i32)
            (local i64 i64)
          )
        )
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(result.types.as_ref(), [WasmFunc::default()]);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::new([(1, WasmValueType::I32), (2, WasmValueType::I64)]),
			code: Box::new([]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_add() {
	let wasm = wasm(r#"
        (module
          (func (export "add") (param i32 i32) (result i32)
            (local.get 0)
            (local.get 1)
            i32.add
          )
        )
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(
		result.types.as_ref(),
		[WasmFunc {
			params: Box::new([WasmValueType::I32, WasmValueType::I32]),
			results: Box::new([WasmValueType::I32])
		}]
	);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(
		result.exports.as_ref(),
		[WasmExport {
			name: Box::from("add".as_bytes()),
			desc: WasmExportDescriptor::Func(0),
		}]
	);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::default(),
			code: Box::new([
				WasmInstruction::LocalGet(0),
				WasmInstruction::LocalGet(1),
				WasmInstruction::I32Add
			]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_func_call() {
	let wasm = wasm(r#"
        (module
          (func (export "call_doubler") (param i32) (result i32)
            (local.get 0)
            (call $double)
          )
          (func $double (param i32) (result i32)
            (local.get 0)
            (local.get 0)
            i32.add
          )
        )
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(
		result.types.as_ref(),
		[WasmFunc {
			params: Box::new([WasmValueType::I32]),
			results: Box::new([WasmValueType::I32])
		}]
	);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0, 0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(
		result.exports.as_ref(),
		[WasmExport {
			name: Box::from("call_doubler".as_bytes()),
			desc: WasmExportDescriptor::Func(0),
		}]
	);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[
			WasmFunctionBody {
				locals: Box::default(),
				code: Box::new([WasmInstruction::LocalGet(0), WasmInstruction::Call(1)]),
			},
			WasmFunctionBody {
				locals: Box::default(),
				code: Box::new([
					WasmInstruction::LocalGet(0),
					WasmInstruction::LocalGet(0),
					WasmInstruction::I32Add
				]),
			}
		]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_import() {
	let wasm = wasm(r#"
        (module
          (func $add (import "env" "add") (param i32) (result i32))
          (func (export "call_add") (param i32) (result i32)
            (local.get 0)
            (call $add)
          )
        )
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(
		result.types.as_ref(),
		[WasmFunc {
			params: Box::new([WasmValueType::I32]),
			results: Box::new([WasmValueType::I32])
		}]
	);
	assert_eq!(
		result.imports.as_ref(),
		[WasmImport {
			module: Box::from("env".as_bytes()),
			name: Box::from("add".as_bytes()),
			desc: WasmImportDescriptor::Function(0),
		}]
	);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(
		result.exports.as_ref(),
		[WasmExport {
			name: Box::from("call_add".as_bytes()),
			desc: WasmExportDescriptor::Func(1),
		}]
	);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::default(),
			code: Box::new([WasmInstruction::LocalGet(0), WasmInstruction::Call(0)]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_block() {
	let wasm = wasm(r#"
        (module
          (func (export "empty")
            (block)
            (block $l)
          )
        )
        "#);

	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(
		result.types.as_ref(),
		[WasmFunc {
			params: Box::default(),
			results: Box::default()
		}]
	);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(
		result.exports.as_ref(),
		[WasmExport {
			name: Box::from("empty".as_bytes()),
			desc: WasmExportDescriptor::Func(0),
		}]
	);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::default(),
			code: Box::new([
				WasmInstruction::Block {
					result_type: WasmResultType::None,
					body: Box::new([])
				},
				WasmInstruction::Block {
					result_type: WasmResultType::None,
					body: Box::new([])
				},
			]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_store() {
	let wasm = wasm(r#"
        (module
          (memory 1)
          (func $i32_store
            (i32.const 0)
            (i32.const 42)
            (i32.store)
          )
          (export "i32_store" (func $i32_store))
        )
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(
		result.types.as_ref(),
		[WasmFunc {
			params: Box::default(),
			results: Box::default()
		}]
	);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), [0]);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(
		result.memories.as_ref(),
		[WasmMemory {
			limits: WasmResizableLimit {
				min: 1,
				max: None
			}
		}]
	);
	assert_eq!(
		result.exports.as_ref(),
		[WasmExport {
			name: Box::from("i32_store".as_bytes()),
			desc: WasmExportDescriptor::Func(0),
		}]
	);
	assert_eq!(result.start_function, None);
	assert_eq!(
		result.codes.as_ref(),
		[WasmFunctionBody {
			locals: Box::default(),
			code: Box::new([
				WasmInstruction::I32Const(0),
				WasmInstruction::I32Const(42),
				WasmInstruction::I32Store(WasmMemoryArgument {
					align: 2,
					offset: 0,
				})
			]),
		}]
	);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_data() {
	let wasm = wasm(r#"
        (module (memory 1) (data (i32.const 0) "hello") (data (i32.const 5) "world"))
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(result.types.as_ref(), []);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), []);
	assert_eq!(result.tables.as_ref(), []);
	assert_eq!(
		result.memories.as_ref(),
		[WasmMemory {
			limits: WasmResizableLimit {
				min: 1,
				max: None
			}
		}]
	);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function, None);
	assert_eq!(result.codes.as_ref(), []);
	assert_eq!(
		result.data.as_ref(),
		[
			WasmData {
				mode: WasmDataMode::Active {
					index: 0,
					offset: 0
				},
				data: Box::from("hello".as_bytes()),
			},
			WasmData {
				mode: WasmDataMode::Active {
					index: 0,
					offset: 5
				},
				data: Box::from("world".as_bytes()),
			},
		]
	);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_table() {
	let wasm = wasm(r#"
        (module (table 0 0 funcref))
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(result.types.as_ref(), []);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), []);
	assert_eq!(
		result.tables.as_ref(),
		[WasmTable {
			element_types: WasmValueType::FuncRef,
			limits: WasmResizableLimit {
				min: 0,
				max: Some(0)
			}
		}]
	);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function, None);
	assert_eq!(result.codes.as_ref(), []);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(result.elements.as_ref(), []);
}

#[test]
fn parse_element() {
	let wasm = wasm(r#"
        (module
          (table 10 funcref)
          (elem (i32.const 23) func 1 2 3)
        )
        "#);
	let result = WasmParser::parse(&wasm).unwrap();
	assert_eq!(result.magic.as_ref(), "\0asm".as_bytes());
	assert_eq!(result.version, 1);
	assert_eq!(result.customs.as_ref(), []);
	assert_eq!(result.types.as_ref(), []);
	assert_eq!(result.imports.as_ref(), []);
	assert_eq!(result.functions.as_ref(), []);
	assert_eq!(
		result.tables.as_ref(),
		[WasmTable {
			element_types: WasmValueType::FuncRef,
			limits: WasmResizableLimit {
				min: 10,
				max: None
			}
		}]
	);
	assert_eq!(result.memories.as_ref(), []);
	assert_eq!(result.exports.as_ref(), []);
	assert_eq!(result.start_function, None);
	assert_eq!(result.codes.as_ref(), []);
	assert_eq!(result.data.as_ref(), []);
	assert_eq!(
		result.elements.as_ref(),
		[WasmElement {
			mode: WasmElementMode::Active {
				table: 0,
				offset: Box::new([WasmInstruction::I32Const(23)])
			},
			init: Box::new([1, 2, 3]),
		}]
	);
}
