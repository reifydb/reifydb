// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// //! Integration tests for VM with real database storage.
// //!
// //! These tests demonstrate that the VM pipeline works correctly with
// //! actual storage and transactions, testing VmState::execute() directly.
//
// use std::sync::Arc;
//
// use futures_util::TryStreamExt;
// use reifydb_catalog::MaterializedCatalog;
// use reifydb_core::{
// 	event::EventBus,
// 	interceptor::StandardInterceptorFactory,
// 	interface::{Engine, Identity},
// 	ioc::IocContainer,
// };
// use reifydb_engine::{StandardEngine, Transaction};
// use reifydb_store_transaction::TransactionStore;
// use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMulti, single::TransactionSingle};
// use reifydb_vm::{
// 	collect,
// 	dsl::compile_script,
// 	source::InMemorySourceRegistry,
// 	vmcore::{VmContext, VmState},
// };
//
// /// Create a test engine with in-memory storage.
// fn create_test_engine() -> StandardEngine {
// 	let store = TransactionStore::testing_memory();
// 	let eventbus = EventBus::new();
// 	let single = TransactionSingle::svl(store.clone(), eventbus.clone());
// 	let cdc = TransactionCdc::new(store.clone());
// 	let multi = TransactionMulti::new(store, single.clone(), eventbus.clone()).unwrap();
//
// 	StandardEngine::new(
// 		multi,
// 		single,
// 		cdc,
// 		eventbus,
// 		Box::new(StandardInterceptorFactory::default()),
// 		MaterializedCatalog::new(),
// 		None,
// 		IocContainer::new(),
// 	)
//
// }
//
// /// Test identity for commands.
// fn test_identity() -> Identity {
// 	Identity::root()
// }
//
// /// Create a namespace via RQL command.
// fn create_namespace(engine: &StandardEngine, name: &str) {
// 	let identity = test_identity();
// 	engine.command_as(&identity, &format!("CREATE NAMESPACE {name}"), Default::default())
// 		.try_collect::<Vec<_>>()
//
// 		.unwrap();
// }
//
// /// Create a table via RQL command.
// fn create_table(engine: &StandardEngine, namespace: &str, table: &str, columns: &str) {
// 	let identity = test_identity();
// 	engine.command_as(&identity, &format!("CREATE TABLE {namespace}.{table} {{ {columns} }}"), Default::default())
// 		.try_collect::<Vec<_>>()
//
// 		.unwrap();
// }
//
// /// Insert data via RQL command.
// fn insert_data(engine: &StandardEngine, rql: &str) {
// 	let identity = test_identity();
// 	engine.command_as(&identity, rql, Default::default()).try_collect::<Vec<_>>().unwrap();
// }
//
// #[tokio::test]
// fn test_vm_scan_real_table() {
// 	let engine = create_test_engine();
//
// 	// Setup: create namespace, table, and insert data
// 	create_namespace(&engine, "test");
// 	create_table(&engine, "test", "users", "id: int4, name: utf8, age: int4");
// 	insert_data(
// 		&engine,
// 		r#"from [
// 			{id: 1, name: "Alice", age: 30},
// 			{id: 2, name: "Bob", age: 25},
// 			{id: 3, name: "Charlie", age: 35}
// 		] insert test.users"#,
// 	)
// 	;
//
// 	// Get a query transaction
// 	let mut query_txn = engine.begin_query().unwrap();
// 	let mut tx: Transaction = (&mut query_txn).into();
//
// 	// Compile script and create VM
// 	let sources = Arc::new(InMemorySourceRegistry::new());
// 	let program = compile_script("scan test.users").unwrap();
//
// 	// Debug: print bytecode info
// 	println!("\n=== INTEGRATION TEST: SCAN REAL TABLE ===");
// 	println!("Constants: {:?}", program.constants);
// 	println!("Sources: {:?}", program.sources);
// 	println!("Expressions: {:?}", program.expressions);
// 	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
// 	println!("==========================================\n");
//
// 	let program = Arc::new(program);
// 	let context = Arc::new(VmContext::new(sources));
// 	let mut vm = VmState::new(program, context);
//
// 	// Execute VM directly with transaction
// 	let result = vm.execute(&mut tx).unwrap();
//
// 	// Verify results
// 	assert!(result.is_some(), "Expected a pipeline result");
// 	let pipeline = result.unwrap();
// 	let columns = collect(pipeline).unwrap();
// 	println!("Result: {} rows", columns.row_count());
// 	assert_eq!(columns.row_count(), 3, "Expected 3 rows");
// }
//
// #[tokio::test]
// fn test_vm_filter_real_table() {
// 	let engine = create_test_engine();
//
// 	// Setup - use int8 to match VM literal type
// 	create_namespace(&engine, "test");
// 	create_table(&engine, "test", "products", "id: int8, name: utf8, price: int8");
// 	insert_data(
// 		&engine,
// 		r#"from [
// 			{id: 1, name: "Apple", price: 100},
// 			{id: 2, name: "Banana", price: 50},
// 			{id: 3, name: "Cherry", price: 200},
// 			{id: 4, name: "Date", price: 150}
// 		] insert test.products"#,
// 	)
// 	;
//
// 	// Get transaction
// 	let mut query_txn = engine.begin_query().unwrap();
// 	let mut tx: Transaction = (&mut query_txn).into();
//
// 	// Compile and execute: filter price > 100
// 	let sources = Arc::new(InMemorySourceRegistry::new());
// 	let program = compile_script("scan test.products | filter price > 100").unwrap();
//
// 	// Debug: print bytecode info
// 	println!("\n=== INTEGRATION TEST: FILTER REAL TABLE ===");
// 	println!("Constants: {:?}", program.constants);
// 	println!("Sources: {:?}", program.sources);
// 	println!("Expressions: {:?}", program.expressions);
// 	println!("Compiled filters: {:?}", program.compiled_filters);
// 	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
// 	println!("============================================\n");
//
// 	let program = Arc::new(program);
// 	let context = Arc::new(VmContext::new(sources));
// 	let mut vm = VmState::new(program, context);
//
// 	let result = vm.execute(&mut tx).unwrap();
//
// 	// Verify: should get Cherry (200) and Date (150)
// 	assert!(result.is_some());
// 	let columns = collect(result.unwrap()).unwrap();
// 	println!("Result: {:?} rows", columns);
// 	assert_eq!(columns.row_count(), 2, "Expected 2 rows with price > 100");
// }
//
// #[tokio::test]
// fn test_vm_sort_and_take() {
// 	let engine = create_test_engine();
//
// 	// Setup - use int8 to match VM literal type
// 	create_namespace(&engine, "test");
// 	create_table(&engine, "test", "scores", "player: utf8, score: int8");
// 	insert_data(
// 		&engine,
// 		r#"from [
// 			{player: "Alice", score: 100},
// 			{player: "Bob", score: 250},
// 			{player: "Charlie", score: 175},
// 			{player: "Diana", score: 300},
// 			{player: "Eve", score: 125}
// 		] insert test.scores"#,
// 	)
// 	;
//
// 	// Get transaction
// 	let mut query_txn = engine.begin_query().unwrap();
// 	let mut tx: Transaction = (&mut query_txn).into();
//
// 	// Compile and execute: top 3 scores (sort descending then take 3)
// 	let sources = Arc::new(InMemorySourceRegistry::new());
// 	let program = compile_script("scan test.scores | sort score desc | take 3").unwrap();
//
// 	// Debug: print bytecode info
// 	println!("\n=== INTEGRATION TEST: SORT AND TAKE ===");
// 	println!("Constants: {:?}", program.constants);
// 	println!("Sources: {:?}", program.sources);
// 	println!("Sort specs: {:?}", program.sort_specs);
// 	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
// 	println!("========================================\n");
//
// 	let program = Arc::new(program);
// 	let context = Arc::new(VmContext::new(sources));
// 	let mut vm = VmState::new(program, context);
//
// 	let result = vm.execute(&mut tx).unwrap();
//
// 	// Verify: should get 3 rows (Diana 300, Bob 250, Charlie 175)
// 	assert!(result.is_some());
// 	let columns = collect(result.unwrap()).unwrap();
// 	println!("Result: {} rows", columns.row_count());
// 	assert_eq!(columns.row_count(), 3, "Expected top 3 scores");
// }
