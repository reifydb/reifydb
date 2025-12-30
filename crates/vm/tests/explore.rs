// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::value::column::{Column, ColumnData, Columns};
use reifydb_type::Fragment;
use reifydb_vm::{InMemorySourceRegistry, collect, compile_script, execute_script};

/// Create test data - modify this to set up your exploration data
fn create_registry() -> InMemorySourceRegistry {
	let mut registry = InMemorySourceRegistry::new();

	let columns = Columns::new(vec![
		Column::new(Fragment::from("id"), ColumnData::int8(vec![1, 2, 3, 4, 5])),
		Column::new(
			Fragment::from("name"),
			ColumnData::utf8(vec![
				String::from("Alice"),
				String::from("Bob"),
				String::from("Charlie"),
				String::from("Diana"),
				String::from("Eve"),
			]),
		),
		Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 17, 35, 22, 19])),
		Column::new(Fragment::from("score"), ColumnData::float8(vec![85.5, 92.0, 78.5, 88.0, 95.5])),
	]);

	registry.register("users", columns);
	registry
}

#[tokio::test]
async fn explore() {
	let registry = create_registry();

	// ============================================
	// MODIFY THIS SCRIPT TO EXPLORE THE VM
	// ============================================
	let script = r#"
        let $user = scan users | filter age > 20 | select [name, age]
        $user
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Expressions: {:?}", program.expressions);
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

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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
	let registry = create_registry();

	// ============================================
	// Test function declarations and calls
	// ============================================
	let script = r#"
        fn get_adults() {
            scan users | filter age >= 18
        }

        fn get_top_scorers() {
            scan users | sort score desc | take 3
        }

        get_top_scorers()
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== FUNCTION TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Functions: {:?}", program.functions);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("==============================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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
	let registry = create_registry();
	let script = r#"
        let $adults = scan users | filter age >= 18
        $adults | select [name, age]
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== DOLLAR VARIABLE TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("=====================================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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
	let registry = create_registry();
	// Test that else-if branch is taken when first condition is false but second is true
	let script = r#"
        if false {
            scan users | take 1
        } else if true {
            scan users | take 2
        } else {
            scan users | take 3
        }
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== IF/ELSE-IF/ELSE TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("=====================================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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
	let registry = create_registry();
	// Simple loop that breaks immediately after one iteration
	let script = r#"
        loop {
            scan users | take 1
            break
        }
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== LOOP/BREAK TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("================================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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

/// Test for..in iteration
#[tokio::test]
async fn test_for_in_iteration() {
	let registry = create_registry();
	// Iterate over adults (age > 20) and count them
	// Users: Alice(25), Bob(17), Charlie(35), Diana(22), Eve(19)
	// Adults (age > 20): Alice(25), Charlie(35), Diana(22) = 3
	let script = r#"
        for $user in scan users | filter age > 20 {
            scan users filter id == $user.id | take 1
        }
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== FOR..IN TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("=============================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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
	println!("\n=== FOR..IN TEST RESULT ===");
	println!("Columns: {}", result.len());
	println!("Rows: {}", result.row_count());
	for col in result.iter() {
		println!("\n  Column: {}", col.name().text());
		println!("  Data: {:?}", col.data());
	}
	println!("===========================\n");

	// The for loop iterates 3 times (3 adults), last pipeline on stack has 1 row
	assert_eq!(result.row_count(), 1, "Expected 1 row from last iteration");
}

/// Test loop counting to 10
#[tokio::test]
async fn test_loop_count_to_10() {
	let registry = create_registry();

	let script = r#"
        let $count = 0
        loop {
            $count = $count + 1
            console::log($count)
            if $count == 10 {
                console::log($count)
                break
            }
        }
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== LOOP COUNT TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("================================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let result = execute_script(script, registry).await;
	println!("\nExecution result: {:?}", result.as_ref().map(|o| o.is_some()));

	// Should print "10" at the end
	assert!(result.is_ok(), "Loop count test should execute successfully");
}

/// Test for..in with field access
#[tokio::test]
async fn test_for_in_field_access() {
	let registry = create_registry();

	let script = r#"
        for $user in scan users | take 10 {
        	let $x = $user.id
        	if $x < 4 {
            	console::log($x)
            }
        }
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== FOR..IN FIELD ACCESS TEST BYTECODE ===");
	println!("Constants: {:?}", program.constants);
	println!("Sources: {:?}", program.sources);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("==========================================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
	println!("\nPipeline result: {:?}", pipeline.as_ref().map(|o| o.is_some()));
}

/// Test bare literal expression in function body - compilation only
#[test]
fn test_bare_literal_expression_compiles() {
	// Function with bare literal as implicit return
	let script = r#"
        fn get_min_age() {
            20
        }

        scan users | take 1
    "#;

	let program = compile_script(script).expect("compile failed");
	println!("Functions: {:?}", program.functions);
	println!("Constants: {:?}", program.constants);
	println!("Bytecode: {:?}", program.bytecode);

	// Decode bytecode manually
	let func = &program.functions[0];
	println!("\nFunction bytecode (offset {}, len {}):", func.bytecode_offset, func.bytecode_len);
	for i in func.bytecode_offset..(func.bytecode_offset + func.bytecode_len) {
		println!("  {}: 0x{:02x}", i, program.bytecode[i]);
	}

	assert_eq!(program.functions.len(), 1);
	assert_eq!(program.functions[0].name, "get_min_age");
}

/// Test bare literal expression in function body used in filter
#[tokio::test]
async fn test_bare_literal_expression_in_filter() {
	let registry = create_registry();

	// Function with bare literal as implicit return, used in filter
	let script = r#"
        fn get_min_age() {
            20
        }

        scan users | filter age > get_min_age() | select [name, age]
    "#;

	// Debug: compile and show bytecode
	let program = compile_script(script).expect("compile failed");
	println!("\n=== BARE LITERAL IN FILTER TEST ===");
	println!("Constants: {:?}", program.constants);
	println!("Functions: {:?}", program.functions);
	println!("Expressions: {:?}", program.expressions);
	println!("Bytecode ({} bytes): {:?}", program.bytecode.len(), program.bytecode);
	println!("===================================\n");

	// Execute using bytecode VM
	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;
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
#[tokio::test]
async fn test_arithmetic_expression_in_filter() {
	let registry = create_registry();

	// Function with arithmetic as implicit return
	let script = r#"
        fn calculate_threshold() {
            10 + 15
        }

        scan users | filter age > calculate_threshold() | select [name, age]
    "#;

	let program = compile_script(script).expect("compile failed");
	println!("\n=== ARITHMETIC IN FILTER TEST ===");
	println!("Functions: {:?}", program.functions);
	println!("=================================\n");

	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;

	let result = match pipeline {
		Ok(Some(p)) => collect(p).await.unwrap_or_else(|_| Columns::empty()),
		_ => Columns::empty(),
	};

	println!("Result: {} rows", result.row_count());
	// 10 + 15 = 25, users with age > 25: Charlie(35) = 1 row
	assert_eq!(result.row_count(), 1, "Expected 1 user with age > 25");
}

/// Test parenthesized expression in function body used in filter
#[tokio::test]
async fn test_parenthesized_expression_in_filter() {
	let registry = create_registry();

	// Function with parenthesized expression
	let script = r#"
        fn compute_limit() {
            (2 + 3) * 4
        }

        scan users | filter age > compute_limit() | select [name, age]
    "#;

	let program = compile_script(script).expect("compile failed");
	println!("\n=== PARENTHESIZED IN FILTER TEST ===");
	println!("Functions: {:?}", program.functions);
	println!("====================================\n");

	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;

	let result = match pipeline {
		Ok(Some(p)) => collect(p).await.unwrap_or_else(|_| Columns::empty()),
		_ => Columns::empty(),
	};

	println!("Result: {} rows", result.row_count());
	// (2 + 3) * 4 = 20, users with age > 20: Alice(25), Charlie(35), Diana(22) = 3 rows
	assert_eq!(result.row_count(), 3, "Expected 3 users with age > 20");
}

/// Test variable expression in function body used in filter
#[tokio::test]
async fn test_variable_expression_in_filter() {
	let registry = create_registry();

	// Function with variable arithmetic
	let script = r#"
        fn double_base() {
            let $base = 10
            $base * 2
        }

        scan users | filter age > double_base() | select [name, age]
    "#;

	let program = compile_script(script).expect("compile failed");
	println!("\n=== VARIABLE IN FILTER TEST ===");
	println!("Functions: {:?}", program.functions);
	println!("===============================\n");

	let registry = Arc::new(registry);
	let pipeline = execute_script(script, registry).await;

	let result = match pipeline {
		Ok(Some(p)) => collect(p).await.unwrap_or_else(|_| Columns::empty()),
		_ => Columns::empty(),
	};

	println!("Result: {} rows", result.row_count());
	// 10 * 2 = 20, users with age > 20: Alice(25), Charlie(35), Diana(22) = 3 rows
	assert_eq!(result.row_count(), 3, "Expected 3 users with age > 20");
}
