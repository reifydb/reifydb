// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_wasm::{Engine, SpawnBinary, module::value::Value, source};

fn add_module() -> Vec<u8> {
	wat::parse_str(
		r#"
		(module
		  (func (export "add") (param i32 i32) (result i32)
		    local.get 0
		    local.get 1
		    i32.add)
		)
		"#,
	)
	.expect("failed to parse WAT")
}

fn alloc_module() -> Vec<u8> {
	wat::parse_str(
		r#"
		(module
		  (memory (export "memory") 1)
		  (global $heap (mut i32) (i32.const 1024))

		  (func $alloc (export "alloc") (param $size i32) (result i32)
		    (local $ptr i32)
		    (local.set $ptr (global.get $heap))
		    (global.set $heap (i32.add (global.get $heap) (local.get $size)))
		    (local.get $ptr))

		  (func (export "dealloc") (param i32) (param i32))

		  (func (export "identity") (param $ptr i32) (param $len i32) (result i32)
		    (local $out i32)
		    (local.set $out (call $alloc (i32.add (local.get $len) (i32.const 4))))
		    (i32.store (local.get $out) (local.get $len))
		    (memory.copy
		      (i32.add (local.get $out) (i32.const 4))
		      (local.get $ptr)
		      (local.get $len))
		    (local.get $out))
		)
		"#,
	)
	.expect("failed to parse WAT")
}

#[test]
fn invoke_add() {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(add_module())).expect("spawn failed");

	let result = engine.invoke("add", &[Value::I32(2), Value::I32(3)]).unwrap();
	assert_eq!(result.as_ref(), &[Value::I32(5)]);
}

#[test]
fn invoke_missing_export() {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(add_module())).expect("spawn failed");

	let err = engine.invoke("nonexistent", &[]).unwrap_err();
	let msg = format!("{}", err);
	assert!(msg.contains("nonexistent"), "error should mention function name: {}", msg);
}

#[test]
fn write_read_memory() {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(alloc_module())).expect("spawn failed");

	let data = b"hello world";
	engine.write_memory(0, data).expect("write failed");

	let result = engine.read_memory(0, data.len()).expect("read failed");
	assert_eq!(result, data);
}

#[test]
fn alloc_write_invoke_read_pattern() {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(alloc_module())).expect("spawn failed");

	let input = b"test data";

	let alloc_result = engine.invoke("alloc", &[Value::I32(input.len() as i32)]).unwrap();
	let ptr = match alloc_result.first() {
		Some(Value::I32(v)) => *v,
		other => panic!("alloc returned unexpected: {:?}", other),
	};

	engine.write_memory(ptr as usize, input).expect("write failed");

	let result = engine.invoke("identity", &[Value::I32(ptr), Value::I32(input.len() as i32)]).unwrap();
	let out_ptr = match result.first() {
		Some(Value::I32(v)) => *v as usize,
		other => panic!("identity returned unexpected: {:?}", other),
	};

	let len_bytes = engine.read_memory(out_ptr, 4).expect("read len failed");
	let out_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
	assert_eq!(out_len, input.len());

	let output = engine.read_memory(out_ptr + 4, out_len).expect("read data failed");
	assert_eq!(output, input);
}

#[test]
fn invalid_wasm_bytes() {
	let mut engine = Engine::default();
	let result = engine.spawn(source::binary::bytes(b"not wasm"));
	assert!(result.is_err());
}

#[test]
fn out_of_bounds_memory() {
	let mut engine = Engine::default();
	engine.spawn(source::binary::bytes(alloc_module())).expect("spawn failed");

	let result = engine.read_memory(65536 * 2, 1);
	assert!(result.is_err());
}
