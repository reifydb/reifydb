// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Test sorting on system virtual tables

use futures_util::TryStreamExt;
use reifydb_core::{Frame, interface::Identity};
use reifydb_engine::test_utils::create_test_engine;

fn test_identity() -> Identity {
	Identity::root()
}

#[tokio::test]
async fn test_sort_system_namespaces() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	// Create some namespaces to have predictable data
	engine.command_as(&identity, "CREATE NAMESPACE zoo", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE NAMESPACE alpha", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE NAMESPACE beta", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();

	// Query system.namespaces with sort
	let frames: Vec<Frame> = engine
		.query_as(&identity, "FROM system.namespaces SORT name", Default::default())
		.try_collect()
		.await
		.unwrap();

	// Extract namespace names from results
	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	// Print the names to see what we got
	println!("Namespace names from query: {:?}", names);

	// Check if sorted (should include alpha, beta, system, zoo at minimum)
	// Note: The default sort direction is DESC, so we expect reverse alphabetical order
	let mut sorted_names = names.clone();
	sorted_names.sort();
	sorted_names.reverse(); // DESC order

	// Check if the names are in descending order
	for i in 1..names.len() {
		assert!(
			names[i - 1] >= names[i],
			"Names should be sorted in descending order, but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[tokio::test]
async fn test_sort_system_namespaces_asc() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	// Create some namespaces to have predictable data
	engine.command_as(&identity, "CREATE NAMESPACE zoo", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE NAMESPACE alpha", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE NAMESPACE beta", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();

	// Query system.namespaces with explicit ASC sort
	let frames: Vec<Frame> = engine
		.query_as(&identity, "FROM system.namespaces SORT name ASC", Default::default())
		.try_collect()
		.await
		.unwrap();

	// Extract namespace names from results
	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	// Print the names to see what we got
	println!("Namespace names from query (ASC): {:?}", names);

	// Check if sorted in ascending order
	for i in 1..names.len() {
		assert!(
			names[i - 1] <= names[i],
			"Names should be sorted in ascending order, but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[tokio::test]
async fn test_sort_system_tables() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	// Create some tables to have predictable data
	engine.command_as(&identity, "CREATE NAMESPACE test", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.zebra { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.apple { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.banana { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();

	// Query system.tables with sort
	let frames: Vec<Frame> = engine
		.query_as(&identity, "FROM system.tables SORT name ASC", Default::default())
		.try_collect()
		.await
		.unwrap();

	// Extract table names from results
	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	// Print the names to see what we got
	println!("Table names from query (ASC): {:?}", names);

	// Check if sorted in ascending order
	for i in 1..names.len() {
		assert!(
			names[i - 1] <= names[i],
			"Names should be sorted in ascending order, but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[tokio::test]
async fn test_sort_system_tables_with_pipe_syntax() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	// Create some tables to have predictable data
	engine.command_as(&identity, "CREATE NAMESPACE test", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.zebra { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.apple { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.banana { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();

	// Query system.tables with pipe syntax
	let frames: Vec<Frame> = engine
		.query_as(&identity, "from system.tables | sort name", Default::default())
		.try_collect()
		.await
		.unwrap();

	// Extract table names from results
	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	// Print the names to see what we got
	println!("Table names from pipe syntax query (DESC default): {:?}", names);

	// Check if sorted in descending order (default)
	for i in 1..names.len() {
		assert!(
			names[i - 1] >= names[i],
			"Names should be sorted in descending order (default), but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[tokio::test]
async fn test_sort_table_storage_stats_by_total_bytes() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	// Create multiple tables and insert data of varying sizes to ensure different storage sizes
	engine.command_as(&identity, "CREATE NAMESPACE test", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.tiny { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.small { id: int4, name: text }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.medium { id: int4, name: text }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(
		&identity,
		"CREATE TABLE test.large { id: int4, name: text, description: text }",
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	// Insert varying amounts of data to create clear size differences

	// Tiny: 1 row, minimal data
	engine.command_as(
		&identity,
		r#"
		FROM [{ id: 1 }]
		INSERT test.tiny
	"#,
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	// Small: 1 row with small text
	engine.command_as(
		&identity,
		r#"
		FROM [{ id: 1, name: "a" }]
		INSERT test.small
	"#,
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	// Medium: 3 rows with moderate text
	engine.command_as(
		&identity,
		r#"
		FROM [
			{ id: 1, name: "abc" },
			{ id: 2, name: "def" },
			{ id: 3, name: "ghi" }
		]
		INSERT test.medium
	"#,
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	// Large: 5 rows with long text
	engine.command_as(
		&identity,
		r#"
		FROM [
			{ id: 1, name: "abcdefghij", description: "This is a longer description with more text" },
			{ id: 2, name: "opqrstuvwx", description: "Fifth and final row with more text data" },
			{ id: 3, name: "klmnopqrst", description: "Another long description with lots of data" },
			{ id: 4, name: "uvwxyzabcd", description: "Yet another description to increase size" },
			{ id: 5, name: "efghijklmn", description: "Fourth row with substantial content here" }
		]
		INSERT test.large
	"#,
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	println!("\n=== Testing system.table_storage_stats Sorting ===\n");

	// First, query WITHOUT sorting to show natural order
	let frames_unsorted: Vec<Frame> = engine
		.query_as(&identity, "FROM system.table_storage_stats", Default::default())
		.try_collect()
		.await
		.unwrap();

	let frame_unsorted = frames_unsorted.first().expect("Expected at least one frame");
	let id_col_unsorted = frame_unsorted.columns.iter().find(|c| c.name == "id").unwrap();
	let bytes_col_unsorted = frame_unsorted.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut unsorted_data: Vec<(u64, u64)> = Vec::new();
	for i in 0..id_col_unsorted.data.len() {
		let id = id_col_unsorted.data.as_string(i).parse::<u64>().unwrap_or(0);
		let bytes = bytes_col_unsorted.data.as_string(i).parse::<u64>().unwrap_or(0);
		unsorted_data.push((id, bytes));
	}

	println!("UNSORTED (natural order):");
	for (id, bytes) in &unsorted_data {
		println!("  Table ID: {}, Total Bytes: {}", id, bytes);
	}

	// Now query WITH sorting (ascending)
	let frames_asc: Vec<Frame> = engine
		.query_as(&identity, "FROM system.table_storage_stats SORT total_bytes ASC", Default::default())
		.try_collect()
		.await
		.unwrap();

	let frame_asc = frames_asc.first().expect("Expected at least one frame");
	let id_col_asc = frame_asc.columns.iter().find(|c| c.name == "id").unwrap();
	let bytes_col_asc = frame_asc.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut sorted_asc_data: Vec<(u64, u64)> = Vec::new();
	for i in 0..id_col_asc.data.len() {
		let id = id_col_asc.data.as_string(i).parse::<u64>().unwrap_or(0);
		let bytes = bytes_col_asc.data.as_string(i).parse::<u64>().unwrap_or(0);
		sorted_asc_data.push((id, bytes));
	}

	println!("\nSORTED ASCENDING by total_bytes:");
	for (id, bytes) in &sorted_asc_data {
		println!("  Table ID: {}, Total Bytes: {}", id, bytes);
	}

	// Verify ascending sort is correct
	let byte_values_asc: Vec<u64> = sorted_asc_data.iter().map(|(_, bytes)| *bytes).collect();
	for i in 1..byte_values_asc.len() {
		assert!(
			byte_values_asc[i - 1] <= byte_values_asc[i],
			"ASC: Byte counts should be sorted in ascending order, but {} comes before {}",
			byte_values_asc[i - 1],
			byte_values_asc[i]
		);
	}

	// Now query WITH sorting (descending)
	let frames_desc: Vec<Frame> = engine
		.query_as(&identity, "FROM system.table_storage_stats SORT total_bytes DESC", Default::default())
		.try_collect()
		.await
		.unwrap();

	let frame_desc = frames_desc.first().expect("Expected at least one frame");
	let id_col_desc = frame_desc.columns.iter().find(|c| c.name == "id").unwrap();
	let bytes_col_desc = frame_desc.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut sorted_desc_data: Vec<(u64, u64)> = Vec::new();
	for i in 0..id_col_desc.data.len() {
		let id = id_col_desc.data.as_string(i).parse::<u64>().unwrap_or(0);
		let bytes = bytes_col_desc.data.as_string(i).parse::<u64>().unwrap_or(0);
		sorted_desc_data.push((id, bytes));
	}

	println!("\nSORTED DESCENDING by total_bytes:");
	for (id, bytes) in &sorted_desc_data {
		println!("  Table ID: {}, Total Bytes: {}", id, bytes);
	}

	// Verify descending sort is correct
	let byte_values_desc: Vec<u64> = sorted_desc_data.iter().map(|(_, bytes)| *bytes).collect();
	for i in 1..byte_values_desc.len() {
		assert!(
			byte_values_desc[i - 1] >= byte_values_desc[i],
			"DESC: Byte counts should be sorted in descending order, but {} comes before {}",
			byte_values_desc[i - 1],
			byte_values_desc[i]
		);
	}

	// Verify that sorting actually changed the order (not already sorted)
	let unsorted_bytes: Vec<u64> = unsorted_data.iter().map(|(_, bytes)| *bytes).collect();
	let is_already_sorted_asc = unsorted_bytes.windows(2).all(|w| w[0] <= w[1]);

	if is_already_sorted_asc {
		println!("\nNOTE: Data was already in ascending order naturally");
	} else {
		println!("\nVERIFIED: Sort operator changed the order (data was not naturally sorted)");
	}

	// Final verification: DESC should be reverse of ASC
	let mut asc_reversed = byte_values_asc.clone();
	asc_reversed.reverse();
	assert_eq!(byte_values_desc, asc_reversed, "DESC sort should be reverse of ASC sort");
}

#[tokio::test]
async fn test_sort_table_storage_stats_multiline_syntax() {
	let engine = create_test_engine().await;
	let identity = test_identity();

	// Create multiple tables with different sizes
	engine.command_as(&identity, "CREATE NAMESPACE test", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.tiny { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(
		&identity,
		"CREATE TABLE test.large { id: int4, name: text, description: text }",
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	// Insert minimal data
	engine.command_as(&identity, r#"FROM [{ id: 1 }] INSERT test.tiny"#, Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();

	// Insert lots of data
	engine.command_as(
		&identity,
		r#"
		FROM [
			{ id: 1, name: "abcdefghij", description: "This is a longer description with more text" },
			{ id: 2, name: "klmnopqrst", description: "Another long description with lots of data" },
			{ id: 3, name: "uvwxyzabcd", description: "Yet another description to increase size" }
		]
		INSERT test.large
	"#,
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	println!("\n=== Testing Multi-line Syntax ===\n");

	// Test with MULTI-LINE syntax (newline between from and sort) - EXACT USER SYNTAX
	let multiline_query = "from system.table_storage_stats
sort total_bytes asc";

	println!("Query:\n{}\n", multiline_query);

	let frames: Vec<Frame> =
		engine.query_as(&identity, multiline_query, Default::default()).try_collect().await.unwrap();

	let frame = frames.first().expect("Expected at least one frame");
	let id_col = frame.columns.iter().find(|c| c.name == "id").unwrap();
	let bytes_col = frame.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut data: Vec<(u64, u64)> = Vec::new();
	for i in 0..id_col.data.len() {
		let id = id_col.data.as_string(i).parse::<u64>().unwrap_or(0);
		let bytes = bytes_col.data.as_string(i).parse::<u64>().unwrap_or(0);
		data.push((id, bytes));
	}

	println!("Results from multi-line query:");
	for (i, (id, bytes)) in data.iter().enumerate() {
		println!("  [{}] Table ID: {}, Total Bytes: {}", i, id, bytes);
	}

	let byte_values: Vec<u64> = data.iter().map(|(_, bytes)| *bytes).collect();
	println!("\nByte values in order: {:?}", byte_values);

	// Check if first value is the smallest (correct for ASC)
	let min_bytes = *byte_values.iter().min().unwrap();
	let max_bytes = *byte_values.iter().max().unwrap();

	println!("\nExpected first (ASC): {} (smallest)", min_bytes);
	println!("Expected last (ASC):  {} (largest)", max_bytes);
	println!("Actual first:         {}", byte_values[0]);
	println!("Actual last:          {}", byte_values[byte_values.len() - 1]);

	// Verify ascending sort is correct
	for i in 1..byte_values.len() {
		assert!(
			byte_values[i - 1] <= byte_values[i],
			"Multi-line ASC: Byte counts should be sorted in ascending order, but {} comes before {}",
			byte_values[i - 1],
			byte_values[i]
		);
	}

	assert_eq!(
		byte_values[0], min_bytes,
		"First value should be smallest for ASC sort, but got {} instead of {}",
		byte_values[0], min_bytes
	);

	println!("\n✅ Multi-line syntax test passed!");
}

#[tokio::test]
async fn test_asc_is_not_desc() {
	// This test specifically checks that ASC doesn't behave like DESC
	let engine = create_test_engine().await;
	let identity = test_identity();

	engine.command_as(&identity, "CREATE NAMESPACE test", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.a { id: int4 }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(&identity, "CREATE TABLE test.b { id: int4, data: text }", Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();

	// Insert different amounts to create size difference
	engine.command_as(&identity, r#"FROM [{ id: 1 }] INSERT test.a"#, Default::default())
		.try_collect::<Vec<Frame>>()
		.await
		.unwrap();
	engine.command_as(
		&identity,
		r#"
		FROM [
			{ id: 1, data: "lots of data here to make this bigger" },
			{ id: 2, data: "even more data to increase size further" },
			{ id: 3, data: "yet more data to make this the largest" }
		]
		INSERT test.b
	"#,
		Default::default(),
	)
	.try_collect::<Vec<Frame>>()
	.await
	.unwrap();

	// Get results with ASC
	let frames_asc: Vec<Frame> = engine
		.query_as(&identity, "from system.table_storage_stats\nsort total_bytes asc", Default::default())
		.try_collect()
		.await
		.unwrap();

	// Get results with DESC
	let frames_desc: Vec<Frame> = engine
		.query_as(&identity, "from system.table_storage_stats\nsort total_bytes desc", Default::default())
		.try_collect()
		.await
		.unwrap();

	// Extract first total_bytes from each
	let frame_asc = frames_asc.first().unwrap();
	let bytes_col_asc = frame_asc.columns.iter().find(|c| c.name == "total_bytes").unwrap();
	let first_asc = bytes_col_asc.data.as_string(0).parse::<u64>().unwrap();

	let frame_desc = frames_desc.first().unwrap();
	let bytes_col_desc = frame_desc.columns.iter().find(|c| c.name == "total_bytes").unwrap();
	let first_desc = bytes_col_desc.data.as_string(0).parse::<u64>().unwrap();

	println!("\nFirst value with ASC:  {}", first_asc);
	println!("First value with DESC: {}", first_desc);

	// If ASC were behaving like DESC, these would be equal
	// They should be DIFFERENT (smallest vs largest)
	assert_ne!(
		first_asc, first_desc,
		"ASC and DESC should return different first values, but both returned {}. ASC may be behaving like DESC!",
		first_asc
	);

	// ASC should give us the SMALLER value first
	assert!(
		first_asc < first_desc,
		"ASC first value ({}) should be LESS than DESC first value ({}), but it's not! ASC is behaving like DESC.",
		first_asc,
		first_desc
	);

	println!("✅ Confirmed: ASC gives smallest first ({}), DESC gives largest first ({})", first_asc, first_desc);
}
