//! # SORT Operator Example
//!
//! Demonstrates the SORT operator in ReifyDB's RQL:
//! - Sorting by single column
//! - Sorting by multiple columns
//! - Ascending and descending order
//! - Sorting different data types
//!
//! Run with: `make rql-sort` or `cargo run --bin rql-sort`

use reifydb::{log_info, sync, Params, SessionSync};

/// Helper function to log queries with formatting
/// The query text is displayed in bold for better readability
fn log_query(query: &str) {
	log_info!("Query:");
	// Split the query into lines and format each line with bold
	let formatted_query = query
		.lines()
		.map(|line| format!("\x1b[1m{}\x1b[0m", line))
		.collect::<Vec<_>>()
		.join("\n");
	log_info!("{}", formatted_query);
}

fn main() {
	// Create and start an in-memory database
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Set up sample data
	db.command_as_root("create schema store", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table store.products {
			id: int4,
			name: utf8,
			category: utf8,
			price: float8,
			stock: int4,
			rating: float4
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, name: "Laptop", category: "Electronics", price: 999.99, stock: 15, rating: 4.5 },
			{ id: 2, name: "Mouse", category: "Electronics", price: 25.99, stock: 50, rating: 4.2 },
			{ id: 3, name: "Desk", category: "Furniture", price: 299.99, stock: 8, rating: 4.7 },
			{ id: 4, name: "Chair", category: "Furniture", price: 149.99, stock: 20, rating: 4.3 },
			{ id: 5, name: "Keyboard", category: "Electronics", price: 79.99, stock: 30, rating: 4.6 },
			{ id: 6, name: "Monitor", category: "Electronics", price: 349.99, stock: 12, rating: 4.8 },
			{ id: 7, name: "Lamp", category: "Furniture", price: 45.99, stock: 25, rating: 4.1 },
			{ id: 8, name: "Webcam", category: "Electronics", price: 89.99, stock: 18, rating: 4.4 }
		]
		insert store.products
		"#,
		Params::None,
	)
	.unwrap();

	// Example 1: Sort by single column (ascending - default)
	log_info!("Example 1: Sort by price (ascending - default)");
	log_query(
		r#"from store.products
sort price"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			sort price
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 2: Sort by single column (ascending - explicit)
	log_info!("\nExample 2: Sort by name (ascending - explicit)");
	log_query(
		r#"from store.products
sort name asc"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			sort name asc
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 3: Sort by single column (descending)
	log_info!("\nExample 3: Sort by rating (descending)");
	log_query(
		r#"from store.products
sort rating desc"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			sort rating desc
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 4: Sort by multiple columns
	log_info!("\nExample 4: Sort by category, then by price");
	log_query(
		r#"from store.products
sort { category asc, price asc }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			sort { category asc, price asc }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 5: Sort with filter
	log_info!("\nExample 5: Filter Electronics, then sort by stock descending");
	log_query(
		r#"from store.products
filter category == "Electronics"
sort stock desc"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			filter category == "Electronics"
			sort stock desc
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 6: Sort inline data
	log_info!("\nExample 6: Sort inline data by score");
	log_query(
		r#"from [
  { name: "Alice", score: 85 },
  { name: "Bob", score: 92 },
  { name: "Carol", score: 78 },
  { name: "Dave", score: 95 }
]
sort score desc"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from [
				{ name: "Alice", score: 85 },
				{ name: "Bob", score: 92 },
				{ name: "Carol", score: 78 },
				{ name: "Dave", score: 95 }
			]
			sort score desc
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 7: Sort with map (projection)
	log_info!("\nExample 7: Project specific columns, then sort");
	log_query(
		r#"from store.products
map { name, price, rating }
sort rating desc"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			map { name, price, rating }
			sort rating desc
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 8: Complex sort with mixed directions
	log_info!("\nExample 8: Sort by category ascending, then rating descending");
	log_query(
		r#"from store.products
sort { category asc, rating desc }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			sort { category asc, rating desc }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 9: Sort numeric data
	log_info!("\nExample 9: Sort by id to show original insertion order");
	log_query(
		r#"from store.products
sort id asc"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from store.products
			sort id asc
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}
}
