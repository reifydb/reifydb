//! # AGGREGATE Operator Example
//!
//! Demonstrates the AGGREGATE operator in ReifyDB's RQL:
//! - Basic aggregation functions (avg, sum, count, min, max)
//! - Group by clauses
//! - Multiple aggregations
//! - Aggregation with filtering
//!
//! Run with: `make rql-aggregate` or `cargo run --bin rql-aggregate`

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

	// Set up sample sales data
	db.command_as_root("create schema sales", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table sales.transactions {
			id: int4,
			product: utf8,
			category: utf8,
			quantity: int4,
			price: float8,
			region: utf8,
			month: int2
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, product: "Laptop", category: "Electronics", quantity: 2, price: 999.99, region: "North", month: 1 },
			{ id: 2, product: "Mouse", category: "Electronics", quantity: 5, price: 25.99, region: "North", month: 1 },
			{ id: 3, product: "Desk", category: "Furniture", quantity: 1, price: 299.99, region: "South", month: 1 },
			{ id: 4, product: "Chair", category: "Furniture", quantity: 3, price: 149.99, region: "North", month: 2 },
			{ id: 5, product: "Laptop", category: "Electronics", quantity: 1, price: 999.99, region: "South", month: 2 },
			{ id: 6, product: "Monitor", category: "Electronics", quantity: 2, price: 349.99, region: "East", month: 2 },
			{ id: 7, product: "Desk", category: "Furniture", quantity: 2, price: 299.99, region: "East", month: 3 },
			{ id: 8, product: "Mouse", category: "Electronics", quantity: 10, price: 25.99, region: "West", month: 3 },
			{ id: 9, product: "Chair", category: "Furniture", quantity: 4, price: 149.99, region: "West", month: 3 },
			{ id: 10, product: "Laptop", category: "Electronics", quantity: 3, price: 999.99, region: "North", month: 3 }
		]
		insert sales.transactions
		"#,
		Params::None,
	)
	.unwrap();

	// Example 1: Simple average
	log_info!("Example 1: Calculate average price by product");
	log_query(
		r#"from sales.transactions
aggregate avg(price) by product"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			aggregate avg(price) by product
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 2: Group by with average
	log_info!("\nExample 2: Average price by category");
	log_query(
		r#"from sales.transactions
aggregate avg(price) by category
sort category"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			aggregate avg(price) by category
			sort category
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 3: Multiple aggregations
	log_info!("\nExample 3: Multiple aggregations by region");
	log_query(
		r#"from sales.transactions
aggregate { avg(price), sum(quantity), count(id) } by region
sort region"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			aggregate { avg(price), sum(quantity), count(id) } by region
			sort region
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 4: Aggregate with filter
	log_info!("\nExample 4: Average price for Electronics only");
	log_query(
		r#"from sales.transactions
filter category == "Electronics"
aggregate { avg(price), sum(quantity) } by product"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			filter category == "Electronics"
			aggregate { avg(price), sum(quantity) } by product
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 5: Count aggregation
	log_info!("\nExample 5: Count transactions by month");
	log_query(
		r#"from sales.transactions
aggregate count(id) by month
sort month"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			aggregate count(id) by month
			sort month
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 6: Sum aggregation
	log_info!("\nExample 6: Total revenue (price * quantity) by category");
	log_query(
		r#"from sales.transactions
map { category, price * quantity as revenue }
aggregate sum(revenue) by category"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			map { category, price * quantity as revenue }
			aggregate sum(revenue) by category
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 7: Group by multiple columns
	log_info!("\nExample 7: Aggregate by category and region");
	log_query(
		r#"from sales.transactions
aggregate { sum(quantity), avg(price) } by { category, region }
sort { category, region }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from sales.transactions
			aggregate { sum(quantity), avg(price) } by { category, region }
			sort { category, region }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}
}
