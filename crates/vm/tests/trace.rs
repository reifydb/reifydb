// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// //! Integration tests for VM execution tracing.
//
// #![cfg(feature = "trace")]
//
// use std::sync::Arc;
//
// use reifydb_core::value::column::{Column, ColumnData, Columns};
// use reifydb_type::Fragment;
// use reifydb_vm::{InMemorySourceRegistry, VmContext, VmState, VmTracer, compile_script};
//
// /// Create test data
// fn create_registry() -> InMemorySourceRegistry {
// 	let mut registry = InMemorySourceRegistry::new();
//
// 	let columns = Columns::new(vec![
// 		Column::new(Fragment::from("id"), ColumnData::int8(vec![1, 2, 3])),
// 		Column::new(
// 			Fragment::from("name"),
// 			ColumnData::utf8(vec![String::from("Alice"), String::from("Bob"), String::from("Charlie")]),
// 		),
// 		Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 17, 35])),
// 	]);
//
// 	registry.register("users", columns);
// 	registry
// }
//
// #[tokio::test]
// fn test_basic_trace() {
// 	let registry = create_registry();
//
// 	// Simple script: push a constant, store it, halt
// 	let script = r#"
//         let $x = 42
//     "#;
//
// 	let program = compile_script(script).expect("compile failed");
// 	let context = Arc::new(VmContext::new(Arc::new(registry)));
//
// 	// Create VM with tracer
// 	let tracer = VmTracer::new();
// 	let mut vm = VmState::new(Arc::new(program), context).with_tracer(tracer);
//
// 	// Execute
// 	let result = vm.execute_memory();
// 	assert!(result.is_ok(), "Execution should succeed");
//
// 	// Get trace
// 	let trace = vm.take_trace().expect("Should have trace");
//
// 	println!("\n=== TRACE OUTPUT ===");
// 	for entry in &trace {
// 		println!("{}", entry);
// 	}
// 	println!("=== END TRACE ===\n");
//
// 	// Verify trace entries
// 	assert!(!trace.is_empty(), "Trace should have entries");
//
// 	// First entry should be PushConst
// 	let first = &trace[0];
// 	assert_eq!(first.step, 0);
// 	println!("First instruction: {}", first.instruction);
// }
//
// #[tokio::test]
// fn test_trace_with_pipeline() {
// 	let registry = create_registry();
//
// 	let script = r#"
//         let $adults = scan users | filter age > 20
//         $adults
//     "#;
//
// 	let program = compile_script(script).expect("compile failed");
// 	let context = Arc::new(VmContext::new(Arc::new(registry)));
//
// 	// Create VM with tracer
// 	let tracer = VmTracer::new();
// 	let mut vm = VmState::new(Arc::new(program), context).with_tracer(tracer);
//
// 	// Execute
// 	let result = vm.execute_memory();
// 	assert!(result.is_ok(), "Execution should succeed");
//
// 	// Get and print trace
// 	let trace = vm.take_trace().expect("Should have trace");
//
// 	println!("\n=== PIPELINE TRACE ===");
// 	for entry in &trace {
// 		println!("{}", entry);
// 	}
// 	println!("=== END TRACE ===\n");
//
// 	// Verify we have entries
// 	assert!(!trace.is_empty(), "Trace should have entries");
//
// 	// Check that we recorded state changes
// 	let total_changes: usize = trace.iter().map(|e| e.changes.len()).sum();
// 	println!("Total state changes: {}", total_changes);
// 	assert!(total_changes > 0, "Should have recorded state changes");
// }
//
// #[tokio::test]
// fn test_trace_format() {
// 	let registry = create_registry();
//
// 	let script = r#"
//         let $count = 0
//         $count = $count + 1
//     "#;
//
// 	let program = compile_script(script).expect("compile failed");
// 	let context = Arc::new(VmContext::new(Arc::new(registry)));
//
// 	let tracer = VmTracer::new();
// 	let mut vm = VmState::new(Arc::new(program), context).with_tracer(tracer);
//
// 	let result = vm.execute_memory();
// 	assert!(result.is_ok(), "Execution should succeed");
//
// 	// Get tracer back and format
// 	if let Some(tracer_back) = vm.tracer.take() {
// 		let formatted = tracer_back.format();
// 		println!("\n=== FORMATTED TRACE ===");
// 		println!("{}", formatted);
// 		println!("=== END FORMATTED ===\n");
//
// 		assert!(!formatted.is_empty(), "Formatted trace should not be empty");
// 		assert!(formatted.contains("Step"), "Should contain step markers");
// 		assert!(formatted.contains("Changes:"), "Should contain changes section");
// 	}
// }
