// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later, see license.md file
//
// //! Tests for subquery support in the VM DSL.
// //!
// //! This module tests the following subquery types:
// //! - Scalar subqueries: `filter amount > (scan thresholds | take 1 | select [max_amount])`
// //! - EXISTS / NOT EXISTS: `filter exists(scan orders | filter status == "pending")`
// //! - IN / NOT IN with subqueries: `filter user_id in (scan active_users | select [id])`
// //! - IN / NOT IN with inline lists: `filter status in ("active", "pending")`
//
// use std::sync::Arc;
//
// use reifydb_core::value::column::{Column, ColumnData, Columns};
// use reifydb_type::Fragment;
// use reifydb_vm::{InMemorySourceRegistry, collect, compile_script, execute_script_memory};
//
// /// Create test data with users, orders, and thresholds tables
// fn create_registry() -> InMemorySourceRegistry {
// 	let mut registry = InMemorySourceRegistry::new();
//
// 	// Users table
// 	let users = Columns::new(vec![
// 		Column::new(Fragment::from("id"), ColumnData::int8(vec![1, 2, 3, 4, 5])),
// 		Column::new(
// 			Fragment::from("name"),
// 			ColumnData::utf8(vec![
// 				String::from("Alice"),
// 				String::from("Bob"),
// 				String::from("Charlie"),
// 				String::from("Diana"),
// 				String::from("Eve"),
// 			]),
// 		),
// 		Column::new(Fragment::from("age"), ColumnData::int8(vec![25, 17, 35, 22, 19])),
// 		Column::new(
// 			Fragment::from("status"),
// 			ColumnData::utf8(vec![
// 				String::from("active"),
// 				String::from("inactive"),
// 				String::from("active"),
// 				String::from("pending"),
// 				String::from("active"),
// 			]),
// 		),
// 	]);
// 	registry.register("users", users);
//
// 	// Orders table
// 	let orders = Columns::new(vec![
// 		Column::new(Fragment::from("order_id"), ColumnData::int8(vec![101, 102, 103, 104])),
// 		Column::new(Fragment::from("user_id"), ColumnData::int8(vec![1, 2, 1, 3])),
// 		Column::new(Fragment::from("amount"), ColumnData::float8(vec![150.0, 75.0, 200.0, 50.0])),
// 		Column::new(
// 			Fragment::from("status"),
// 			ColumnData::utf8(vec![
// 				String::from("completed"),
// 				String::from("pending"),
// 				String::from("completed"),
// 				String::from("pending"),
// 			]),
// 		),
// 	]);
// 	registry.register("orders", orders);
//
// 	// Thresholds table (for scalar subquery tests)
// 	let thresholds = Columns::new(vec![
// 		Column::new(
// 			Fragment::from("name"),
// 			ColumnData::utf8(vec![String::from("max_order"), String::from("min_age")]),
// 		),
// 		Column::new(Fragment::from("value"), ColumnData::float8(vec![100.0, 18.0])),
// 	]);
// 	registry.register("thresholds", thresholds);
//
// 	// Active user IDs (for IN subquery tests)
// 	let active_users = Columns::new(vec![Column::new(Fragment::from("id"), ColumnData::int8(vec![1, 3, 5]))]);
// 	registry.register("active_users", active_users);
//
// 	registry
// }
//
// // ============================================================================
// // SCALAR SUBQUERY TESTS
// // ============================================================================
//
// /// Test scalar subquery in filter: filter amount > (scan thresholds | ...)
// #[tokio::test]
// async fn test_scalar_subquery_in_filter() {
// 	let registry = create_registry();
//
// 	// Filter orders where amount > max_order threshold (100.0)
// 	// Orders: [150.0, 75.0, 200.0, 50.0]
// 	// Expected: orders with amount > 100.0 = [150.0, 200.0] = 2 rows
// 	let script = r#"
//         scan orders
//         | filter amount > (scan thresholds | filter name == "max_order" | take 1 | select [value])
//     "#;
//
// 	let program = compile_script(script).expect("compile failed");
// 	println!("Subqueries in program: {:?}", program.subqueries.len());
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== SCALAR SUBQUERY RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 2, "Expected 2 orders with amount > 100.0");
// }
//
// /// Test scalar subquery that returns empty result (should be NULL/undefined)
// #[tokio::test]
// async fn test_scalar_subquery_empty_result() {
// 	let registry = create_registry();
//
// 	// Filter with subquery that returns no rows - comparison with NULL should filter all
// 	let script = r#"
//         scan users
//         | filter age > (scan thresholds | filter name == "nonexistent" | take 1 | select [value])
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	// Comparison with NULL should yield NULL, which filters out all rows
// 	assert_eq!(result.row_count(), 0, "Expected 0 rows when comparing with NULL subquery result");
// }
//
// // ============================================================================
// // EXISTS / NOT EXISTS TESTS
// // ============================================================================
//
// /// Test EXISTS subquery: filter exists(scan orders | filter ...)
// #[tokio::test]
// async fn test_exists_subquery() {
// 	let registry = create_registry();
//
// 	// Get users who have at least one pending order
// 	// Orders: user_id 2 and 3 have pending orders
// 	// Users: Bob(id=2), Charlie(id=3) should match
// 	let script = r#"
//         scan users
//         | filter exists(scan orders | filter user_id == id and status == "pending")
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== EXISTS RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 2, "Expected 2 users with pending orders (Bob, Charlie)");
// }
//
// /// Test NOT EXISTS subquery: filter not exists(...)
// #[tokio::test]
// async fn test_not_exists_subquery() {
// 	let registry = create_registry();
//
// 	// Get users who have NO orders at all
// 	// Orders have user_ids: [1, 2, 1, 3]
// 	// Users without orders: Diana(id=4), Eve(id=5)
// 	let script = r#"
//         scan users
//         | filter not exists(scan orders | filter user_id == id)
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== NOT EXISTS RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 2, "Expected 2 users without orders (Diana, Eve)");
// }
//
// /// Test EXISTS with empty subquery result
// #[tokio::test]
// async fn test_exists_empty_subquery() {
// 	let registry = create_registry();
//
// 	// EXISTS on subquery that returns no rows should be false for all
// 	let script = r#"
//         scan users
//         | filter exists(scan orders | filter amount > 10000)
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	assert_eq!(result.row_count(), 0, "Expected 0 rows when EXISTS subquery is empty");
// }
//
// // ============================================================================
// // IN / NOT IN WITH SUBQUERY TESTS
// // ============================================================================
//
// /// Test IN subquery: filter id in (scan active_users | select [id])
// #[tokio::test]
// async fn test_in_subquery() {
// 	let registry = create_registry();
//
// 	// Get users whose id is in the active_users table
// 	// active_users has ids: [1, 3, 5]
// 	// Should return: Alice(1), Charlie(3), Eve(5)
// 	let script = r#"
//         scan users
//         | filter id in (scan active_users | select [id])
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== IN SUBQUERY RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 3, "Expected 3 users in active_users (Alice, Charlie, Eve)");
// }
//
// /// Test NOT IN subquery: filter id not in (scan active_users | select [id])
// #[tokio::test]
// async fn test_not_in_subquery() {
// 	let registry = create_registry();
//
// 	// Get users whose id is NOT in the active_users table
// 	// active_users has ids: [1, 3, 5]
// 	// Should return: Bob(2), Diana(4)
// 	let script = r#"
//         scan users
//         | filter id not in (scan active_users | select [id])
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== NOT IN SUBQUERY RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 2, "Expected 2 users not in active_users (Bob, Diana)");
// }
//
// /// Test IN with empty subquery result
// #[tokio::test]
// async fn test_in_empty_subquery() {
// 	let registry = create_registry();
//
// 	// IN with empty subquery should match nothing
// 	let script = r#"
//         scan users
//         | filter id in (scan active_users | filter id > 100 | select [id])
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	assert_eq!(result.row_count(), 0, "Expected 0 rows when IN subquery is empty");
// }
//
// // ============================================================================
// // IN / NOT IN WITH INLINE LIST TESTS
// // ============================================================================
//
// /// Test IN with inline list: filter status in ("active", "pending")
// #[tokio::test]
// async fn test_in_inline_list_strings() {
// 	let registry = create_registry();
//
// 	// Users with status "active" or "pending"
// 	// Statuses: ["active", "inactive", "active", "pending", "active"]
// 	// Should return: Alice, Charlie, Diana, Eve (4 users)
// 	let script = r#"
//         scan users
//         | filter status in ("active", "pending")
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== IN INLINE LIST (STRINGS) RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 4, "Expected 4 users with active or pending status");
// }
//
// /// Test NOT IN with inline list: filter id not in (2, 4)
// #[tokio::test]
// async fn test_not_in_inline_list_integers() {
// 	let registry = create_registry();
//
// 	// Users with id NOT in [2, 4]
// 	// IDs: [1, 2, 3, 4, 5]
// 	// Should return: Alice(1), Charlie(3), Eve(5) = 3 users
// 	let script = r#"
//         scan users
//         | filter id not in (2, 4)
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	println!("=== NOT IN INLINE LIST (INTEGERS) RESULT ===");
// 	println!("Rows: {}", result.row_count());
// 	for col in result.iter() {
// 		println!("  {}: {:?}", col.name().text(), col.data());
// 	}
//
// 	assert_eq!(result.row_count(), 3, "Expected 3 users with id not in [2, 4]");
// }
//
// /// Test IN with single-element list
// #[tokio::test]
// async fn test_in_single_element_list() {
// 	let registry = create_registry();
//
// 	// Should behave like equality check
// 	let script = r#"
//         scan users
//         | filter status in ("inactive")
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	assert_eq!(result.row_count(), 1, "Expected 1 user with inactive status (Bob)");
// }
//
// // ============================================================================
// // SUBQUERY CACHING TESTS
// // ============================================================================
//
// /// Test that the same subquery used multiple times is only executed once
// #[tokio::test]
// async fn test_subquery_caching() {
// 	let registry = create_registry();
//
// 	// Use the same subquery in multiple places - should be cached
// 	let script = r#"
//         scan orders
//         | filter amount > (scan thresholds | filter name == "max_order" | take 1 | select [value])
//         | filter amount < (scan thresholds | filter name == "max_order" | take 1 | select [value]) * 2
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	// Orders with 100 < amount < 200: [150.0] = 1 row
// 	assert_eq!(result.row_count(), 1, "Expected 1 order with 100 < amount < 200");
// }
//
// // ============================================================================
// // COMBINED/COMPLEX SUBQUERY TESTS
// // ============================================================================
//
// /// Test combining EXISTS and IN in the same query
// #[tokio::test]
// async fn test_combined_exists_and_in() {
// 	let registry = create_registry();
//
// 	// Get active users who have at least one order
// 	let script = r#"
//         scan users
//         | filter id in (scan active_users | select [id])
//         | filter exists(scan orders | filter user_id == id)
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	// active_users: [1, 3, 5]
// 	// Users with orders: [1, 2, 3]
// 	// Intersection: [1, 3] = Alice, Charlie
// 	assert_eq!(result.row_count(), 2, "Expected 2 active users with orders");
// }
//
// /// Test nested scenario with scalar subquery in extend
// #[tokio::test]
// #[ignore = "subqueries in extend not yet supported"]
// async fn test_scalar_subquery_in_extend() {
// 	let registry = create_registry();
//
// 	// Add a computed column showing if amount exceeds threshold
// 	let script = r#"
//         scan orders
//         | extend { exceeds_threshold: amount > (scan thresholds | filter name == "max_order" | take 1 | select [value]) }
//     "#;
//
// 	let registry = Arc::new(registry);
// 	let pipeline = execute_script_memory(script, registry).await.expect("execute failed");
// 	let result = collect(pipeline.unwrap()).await.expect("collect failed");
//
// 	assert_eq!(result.row_count(), 4, "Expected all 4 orders");
// 	assert!(result.iter().any(|c| c.name().text() == "exceeds_threshold"), "Expected exceeds_threshold column");
// }
