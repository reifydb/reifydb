//! # Logical Operators Example
//!
//! Demonstrates logical operators in ReifyDB's RQL:
//! - AND operator
//! - OR operator
//! - NOT operator
//! - XOR operator
//! - Complex logical expressions
//! - Operator precedence with parentheses
//!
//! Run with: `make rql-logical` or `cargo run --bin rql-logical`

use reifydb::{sync, Params, SessionSync};
use reifydb::log_info;

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

	// Example 1: Basic logical operations
	log_info!("Example 1: Basic logical operations");
	log_query(r#"map {
  true and true as and_true,
  true and false as and_false,
  true or false as or_true,
  false or false as or_false,
  not true as not_true,
  not false as not_false,
  true xor false as xor_true,
  true xor true as xor_false
}"#);
	for frame in db
		.query_as_root(
			r#"
			map {
				true and true as and_true,
				true and false as and_false,
				true or false as or_true,
				false or false as or_false,
				not true as not_true,
				not false as not_false,
				true xor false as xor_true,
				true xor true as xor_false
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Set up sample data
	db.command_as_root("create schema inventory", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table inventory.products {
			id: int4,
			name: utf8,
			category: utf8,
			price: float8,
			in_stock: bool,
			on_sale: bool,
			featured: bool,
			min_age: int2
		}
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, name: "Toy Car", category: "Toys", price: 15.99, in_stock: true, on_sale: true, featured: false, min_age: 3 },
			{ id: 2, name: "Laptop", category: "Electronics", price: 999.99, in_stock: true, on_sale: false, featured: true, min_age: 0 },
			{ id: 3, name: "Book", category: "Books", price: 12.99, in_stock: false, on_sale: false, featured: false, min_age: 0 },
			{ id: 4, name: "Headphones", category: "Electronics", price: 79.99, in_stock: true, on_sale: true, featured: true, min_age: 0 },
			{ id: 5, name: "Board Game", category: "Toys", price: 35.99, in_stock: true, on_sale: false, featured: false, min_age: 8 },
			{ id: 6, name: "T-Shirt", category: "Clothing", price: 19.99, in_stock: false, on_sale: true, featured: false, min_age: 0 },
			{ id: 7, name: "Smartphone", category: "Electronics", price: 699.99, in_stock: true, on_sale: false, featured: true, min_age: 0 },
			{ id: 8, name: "Puzzle", category: "Toys", price: 24.99, in_stock: true, on_sale: true, featured: false, min_age: 5 }
		]
		insert inventory.products
		"#,
		Params::None,
	)
	.unwrap();

	// Example 2: AND operator in filters
	log_info!("\nExample 2: AND operator - products in stock AND on sale");
	log_query(r#"from inventory.products
filter in_stock == true and on_sale == true"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter in_stock == true and on_sale == true
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 3: OR operator in filters
	log_info!("\nExample 3: OR operator - featured OR on sale");
	log_query(r#"from inventory.products
filter featured == true or on_sale == true"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter featured == true or on_sale == true
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 4: NOT operator
	log_info!("\nExample 4: NOT operator - products NOT in stock");
	log_query(r#"from inventory.products
filter not in_stock"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter not in_stock
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 5: Complex logical expression with parentheses
	log_info!("\nExample 5: Complex expression - (Electronics OR Toys) AND on_sale");
	log_query(r#"from inventory.products
filter (category == "Electronics" or category == "Toys")
   and on_sale == true"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter (category == "Electronics" or category == "Toys") 
			   and on_sale == true
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 6: XOR operator
	log_info!("\nExample 6: XOR operator - either featured OR on_sale (but not both)");
	log_query(r#"from inventory.products
filter featured xor on_sale"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter featured xor on_sale
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 7: Multiple AND conditions
	log_info!("\nExample 7: Multiple AND conditions");
	log_query(r#"from inventory.products
filter category == "Toys" and in_stock == true and price < 30"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter category == "Toys" and in_stock == true and price < 30
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 8: Operator precedence (AND before OR)
	log_info!("\nExample 8: Operator precedence demonstration");
	log_info!("Without parentheses (AND has higher precedence):");
	log_query(r#"from inventory.products
filter on_sale == true or featured == true and category == "Electronics""#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter on_sale == true or featured == true and category == "Electronics"
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	log_info!("\nWith parentheses (changing precedence):");
	log_query(r#"from inventory.products
filter (on_sale == true or featured == true) and category == "Electronics""#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter (on_sale == true or featured == true) and category == "Electronics"
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 9: Logical operators in computed fields
	log_info!("\nExample 9: Logical operators in computed fields");
	log_query(r#"from inventory.products
map {
  name,
  price,
  in_stock and on_sale as available_deal,
  featured or (price > 500) as premium_item,
  not in_stock or not on_sale as limited_offer
}"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			map {
				name,
				price,
				in_stock and on_sale as available_deal,
				featured or (price > 500) as premium_item,
				not in_stock or not on_sale as limited_offer
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 10: Complex nested logical expressions
	log_info!("\nExample 10: Complex nested logical expression");
	log_query(r#"from inventory.products
filter ((category == "Toys" and min_age >= 5) or
        (category == "Electronics" and price < 100)) and
        in_stock == true"#);
	for frame in db
		.query_as_root(
			r#"
			from inventory.products
			filter ((category == "Toys" and min_age >= 5) or
			        (category == "Electronics" and price < 100)) and
			        in_stock == true
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

}