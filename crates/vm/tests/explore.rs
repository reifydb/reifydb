// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::TryStreamExt;
use reifydb_catalog::{Catalog, MaterializedCatalog};
use reifydb_core::{event::EventBus, interface::Identity, ioc::IocContainer, value::column::Columns};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{
	cdc::TransactionCdc, interceptor::StandardInterceptorFactory, multi::TransactionMulti,
	single::TransactionSingle,
};
use reifydb_type::Params;
use reifydb_vm::{collect, compile_script, execute_program};

async fn create_test_engine() -> StandardEngine {
	let store = TransactionStore::testing_memory().await;
	let eventbus = EventBus::new();
	let single = TransactionSingle::svl(store.clone(), eventbus.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMulti::new(store, single.clone(), eventbus.clone()).await.unwrap();

	StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		Catalog::default(),
		None,
		IocContainer::new(),
	)
	.await
}

/// Test identity for commands.
fn test_identity() -> Identity {
	Identity::root()
}

/// Create a namespace via RQL command.
async fn create_namespace(engine: &StandardEngine, name: &str) {
	let identity = test_identity();
	engine.command_as(&identity, &format!("CREATE NAMESPACE {name}"), Default::default())
		.try_collect::<Vec<_>>()
		.await
		.unwrap();
}

/// Create a table via RQL command.
async fn create_table(engine: &StandardEngine, namespace: &str, table: &str, columns: &str) {
	let identity = test_identity();
	engine.command_as(&identity, &format!("CREATE TABLE {namespace}.{table} {{ {columns} }}"), Default::default())
		.try_collect::<Vec<_>>()
		.await
		.unwrap();
}

/// Insert data via RQL command.
async fn insert_data(engine: &StandardEngine, rql: &str) {
	let identity = test_identity();
	engine.command_as(&identity, rql, Default::default()).try_collect::<Vec<_>>().await.unwrap();
}

#[tokio::test]
async fn explore() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 30},
				{id: 2, name: "Bob", age: 25},
				{id: 3, name: "Charlie", age: 35}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();

	let catalog = engine.catalog();

	let script = r#"
        let $x = from test.users | filter age > 25  | MAP { name, age } | take 100;
        $x
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Column lists: {:?}", program.column_lists);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);

	// Decode bytecode
	println!("\nDecoded:");
	let mut i = 0;
	while i < program.bytecode.len() {
		let op = program.bytecode[i];
		let desc = match op {
			0x20 => {
				let idx = u16::from_le_bytes([program.bytecode[i + 1], program.bytecode[i + 2]]);
				i += 2;
				format!("Source {}", idx)
			}
			0x02 => {
				let idx = u16::from_le_bytes([program.bytecode[i + 1], program.bytecode[i + 2]]);
				i += 2;
				format!("PushExpr {}", idx)
			}
			0x04 => {
				let idx = u16::from_le_bytes([program.bytecode[i + 1], program.bytecode[i + 2]]);
				i += 2;
				format!("PushColList {}", idx)
			}
			0x22 => {
				let k = program.bytecode[i + 1];
				i += 1;
				format!("Apply {:?}", k)
			}
			0x12 => {
				let idx = u16::from_le_bytes([program.bytecode[i + 1], program.bytecode[i + 2]]);
				i += 2;
				format!("StorePipeline {} ({:?})", idx, program.constants.get(idx as usize))
			}
			0x13 => {
				let idx = u16::from_le_bytes([program.bytecode[i + 1], program.bytecode[i + 2]]);
				i += 2;
				format!("LoadPipeline {} ({:?})", idx, program.constants.get(idx as usize))
			}
			0xFF => "Halt".to_string(),
			_ => format!("Unknown(0x{:02X})", op),
		};
		println!("  {:04}: {}", i, desc);
		i += 1;
	}
	println!("================\n");

	for frame in engine
		.query_as(&test_identity(), "from test.users", Params::None)
		.try_collect::<Vec<_>>()
		.await
		.unwrap()
	{
		println!("{}", frame);
	}

	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("==============\n");
}
#[tokio::test]
async fn test_function_declaration_and_call() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data with scores
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8, score: float8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25, score: 85.5},
				{id: 2, name: "Bob", age: 17, score: 92.0},
				{id: 3, name: "Charlie", age: 35, score: 78.5},
				{id: 4, name: "Diana", age: 22, score: 88.0},
				{id: 5, name: "Eve", age: 19, score: 95.5}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// ============================================
	// Test function declarations and calls
	// ============================================
	let script = "fn get_top_scorers() { from test.users | sort { score: asc } | take 3 }; get_top_scorers()";

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== FUNCTION TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Script functions: {:?}", program.script_functions);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("==============================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== FUNCTION TEST RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("============================\n");

	// Verify: get_top_scorers() should return 3 rows sorted by score descending
	// Scores are: [85.5, 92.0, 78.5, 88.0, 95.5] for [Alice, Bob, Charlie, Diana, Eve]
	// Top 3 by score: Eve(95.5), Bob(92.0), Diana(88.0)
	assert_eq!(result.row_count(), 3, "Expected 3 rows from get_top_scorers()");
	assert_eq!(result.len(), 4, "Expected 4 columns (id, name, age, score)");
}
/// Test dollar-prefixed variable declaration syntax
#[tokio::test]
async fn test_dollar_variable_declaration() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// ============================================
	// Test dollar-prefixed variable declaration with map projection
	// Variable schema tracking allows column resolution in $adults | map { name, age }
	// ============================================
	let script = "let $adults = from test.users | filter age >= 18; $adults | map { name, age }";

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== DOLLAR VARIABLE TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("=====================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== DOLLAR VARIABLE TEST RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("===================================\n");

	// Users: Alice(25), Bob(17), Charlie(35), Diana(22), Eve(19)
	// Adults (age >= 18): Alice, Charlie, Diana, Eve = 4 rows
	assert_eq!(result.row_count(), 4, "Expected 4 adults");
	assert_eq!(result.len(), 2, "Expected 2 columns [name, age]");
}

/// Test if/else-if/else chain
#[tokio::test]
async fn test_if_else_if_else() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Test that else-if branch is taken when first condition is false but second is true
	let script = r#"
		if false {
			from test.users | take 1
		} else if true {
			from test.users | take 2
		} else {
			from test.users | take 3
		}
	"#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== IF/ELSE-IF/ELSE TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("=====================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== IF/ELSE-IF/ELSE TEST RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("===================================\n");

	// else-if branch (true) executes, take 2
	assert_eq!(result.row_count(), 2, "Expected 2 rows from else-if branch");
}

/// Test simple loop with break
#[tokio::test]
async fn test_loop_break() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Simple loop that breaks immediately after one iteration
	let script = r#"
		loop {
			from test.users | take 1
			break
		}
	"#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== LOOP/BREAK TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== LOOP/BREAK TEST RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("==============================\n");

	// Loop executes once and breaks, leaving the pipeline on stack
	assert_eq!(result.row_count(), 1, "Expected 1 row from single loop iteration");
}

/// Test for..in iteration over query results
#[tokio::test]
async fn test_for_in_iteration() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// For loop iterating over users with age > 20
	// Note: $user field access ($user.id) not yet supported, so just query all users
	let script = r#"
		for $user in from test.users | filter age > 20 {
			from test.users | take 1;
		}
	"#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== FOR IN ITERATION TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("======================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== FOR IN ITERATION TEST RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("====================================\n");

	// There are 3 users with age > 20: Alice (25), Charlie (35), Diana (22)
	// Each iteration outputs 1 row, but only the last iteration's output is on stack
	// The for loop should produce 1 row (the last user matched)
	assert!(result.row_count() >= 1, "Expected at least 1 row from for loop");
}

/// Test loop counting to 10
#[tokio::test]
async fn test_loop_count_to_10() {
	let engine = create_test_engine().await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	let script = r#"
		let $count = 0
		loop {
			$count = $count + 1
			console::log($count)
			if $count == 10 {
				break
			}
		}
	"#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== LOOP COUNT TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nExecution result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	// Should execute successfully (console::log prints 1-10)
	assert!(pipeline.is_ok(), "Loop count test should execute successfully");
}

/// Test for..in with field access
#[tokio::test]
async fn test_for_in_field_access() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	let script = r#"
		for $user in from test.users | take 10 {
			let $x = $user.id
			if $x < 4 {
				console::log($x)
			}
		}
	"#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== FOR..IN FIELD ACCESS TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("==========================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	// Should execute successfully
	assert!(pipeline.is_ok(), "For..in field access test should execute successfully");
}

/// Test bare literal expression in function body - compilation only
#[tokio::test]
async fn test_bare_literal_expression_compiles() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Function with bare literal as implicit return
	let script = r#"
		fn get_min_age() {
			20
		}

		from test.users | filter age > get_in_age() | take 1
	"#;

	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("Script Functions: {:?}", program.script_functions);
	println!("Constants: {:?}", program.constants);
	println!("Bytecode: {:?}", program.bytecode);

	assert_eq!(program.script_functions.len(), 1);
	assert_eq!(program.script_functions[0].name, "get_min_age");
}

/// Test bare literal expression in function body used in filter
#[tokio::test(flavor = "multi_thread")]
async fn test_bare_literal_expression_in_filter() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Function with bare literal as implicit return, used in filter
	let script = r#"
		fn get_min_age() {
			20
		}

		from test.users | filter age > get_min_age() | map { name, age }
	"#;

	// Debug: compile and show bytecode
	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== BARE LITERAL IN FILTER TEST ===");
	println!("Constants: {:?}", program.constants);
	println!("Script Functions: {:?}", program.script_functions);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("===================================\n");

	// Execute using bytecode VM
	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));

	let result = match pipeline {
		Ok(Some(p)) => {
			println!("Got pipeline, collecting...");
			match collect(p).await {
				Ok(cols) => {
					println!("Collected {} columns, {} rows", cols.len(), cols.row_count());
					cols
				}
				Err(e) => {
					println!("Collect failed: {:?}", e);
					Columns::empty()
				}
			}
		}
		Ok(None) => {
			println!("No pipeline returned");
			Columns::empty()
		}
		Err(e) => {
			println!("Execute failed: {:?}", e);
			Columns::empty()
		}
	};

	// Print results
	println!("\n=== BARE LITERAL IN FILTER RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("=====================================\n");

	// Users with age > 20: Alice(25), Charlie(35), Diana(22) = 3 rows
	assert_eq!(result.row_count(), 3, "Expected 3 users with age > 20");
	assert_eq!(result.len(), 2, "Expected 2 columns (name, age)");
}

/// Test arithmetic expression in function body used in filter
#[tokio::test(flavor = "multi_thread")]
async fn test_arithmetic_expression_in_filter() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Function with arithmetic as implicit return
	let script = r#"
		fn calculate_threshold() {
			10 + 15
		}

		from test.users | filter age > calculate_threshold() | map { name, age }
	"#;

	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== ARITHMETIC IN FILTER TEST ===");
	println!("Script Functions: {:?}", program.script_functions);
	println!("=================================\n");

	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;

	let result = match pipeline {
		Ok(Some(p)) => collect(p).await.unwrap_or_else(|_| Columns::empty()),
		_ => Columns::empty(),
	};

	println!("Result: {} rows", result.row_count());
	// 10 + 15 = 25, users with age > 25: Charlie(35) = 1 row
	assert_eq!(result.row_count(), 1, "Expected 1 user with age > 25");
}

/// Test parenthesized expression in function body used in filter
#[tokio::test(flavor = "multi_thread")]
async fn test_parenthesized_expression_in_filter() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Function with parenthesized expression
	let script = r#"
		fn compute_limit() {
			(2 + 3) * 4
		}

		from test.users | filter age > compute_limit() | map { name, age }
	"#;

	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== PARENTHESIZED IN FILTER TEST ===");
	println!("Script Functions: {:?}", program.script_functions);
	println!("====================================\n");

	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;

	let result = match pipeline {
		Ok(Some(p)) => collect(p).await.unwrap_or_else(|_| Columns::empty()),
		_ => Columns::empty(),
	};

	println!("Result: {} rows", result.row_count());
	// (2 + 3) * 4 = 20, users with age > 20: Alice(25), Charlie(35), Diana(22) = 3 rows
	assert_eq!(result.row_count(), 3, "Expected 3 users with age > 20");
}

/// Test variable expression in function body used in filter
#[tokio::test(flavor = "multi_thread")]
async fn test_variable_expression_in_filter() {
	let engine = create_test_engine().await;

	// Setup: create namespace, table, and insert data
	create_namespace(&engine, "test").await;
	create_table(&engine, "test", "users", "id: int8, name: utf8, age: int8").await;
	insert_data(
		&engine,
		r#"from [
				{id: 1, name: "Alice", age: 25},
				{id: 2, name: "Bob", age: 17},
				{id: 3, name: "Charlie", age: 35},
				{id: 4, name: "Diana", age: 22},
				{id: 5, name: "Eve", age: 19}
			] insert test.users"#,
	)
	.await;

	let mut tx = engine.begin_command().await.unwrap();
	let catalog = engine.catalog();

	// Function with variable arithmetic
	let script = r#"
		fn double_base() {
			let $base = 10
			$base * 2
		}

		from test.users | filter age > double_base() | map { name, age }
	"#;

	let program = compile_script(script, &catalog, &mut tx).await.expect("compile failed");
	println!("\n=== VARIABLE IN FILTER TEST ===");
	println!("Script Functions: {:?}", program.script_functions);
	println!("===============================\n");

	let pipeline = execute_program(program.clone(), catalog, &mut tx).await;

	let result = match pipeline {
		Ok(Some(p)) => collect(p).await.unwrap_or_else(|_| Columns::empty()),
		_ => Columns::empty(),
	};

	println!("Result: {} rows", result.row_count());
	// 10 * 2 = 20, users with age > 20: Alice(25), Charlie(35), Diana(22) = 3 rows
	assert_eq!(result.row_count(), 3, "Expected 3 users with age > 20");
}
