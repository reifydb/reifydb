//! # MAP Operator Example
//!
//! Demonstrates the MAP operator in ReifyDB's RQL:
//! - Projecting specific columns
//! - Computing new fields with expressions
//! - Renaming fields
//! - Creating derived values
//!
//! Run with: `make rql-map` or `cargo run --bin rql-map`

use reifydb::{log_info, sync, Params, SessionSync};
use reifydb_examples::log_query;

fn main() {
	// Create and start an in-memory database
	let mut db = sync::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Example 1: MAP with constants
	log_info!("Example 1: MAP with constants");
	log_query(r#"map { 42 as answer, "hello" as greeting }"#);
	for frame in db
		.query_as_root(
			r#"map { 42 as answer, "hello" as greeting }"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +----------+------------+
		// |  answer  |  greeting  |
		// +----------+------------+
		// |    42    |   hello    |
		// +----------+------------+
	}

	// Example 2: MAP with arithmetic expressions
	log_info!("\nExample 2: MAP with arithmetic expressions");
	log_query(
		r#"map { 10 + 5 as sum, 10 * 5 as product, 10 / 5 as quotient }"#,
	);
	for frame in db
		.query_as_root(
			r#"map { 10 + 5 as sum, 10 * 5 as product, 10 / 5 as quotient }"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +-------+-----------+------------+
		// |  sum  |  product  |  quotient  |
		// +-------+-----------+------------+
		// |  15   |    50     |     2      |
		// +-------+-----------+------------+
	}

	// Example 3: MAP to project columns from inline data
	log_info!("\nExample 3: MAP to project specific columns");
	log_query(
		r#"from [{ id: 1, name: "Alice", age: 30, city: "NYC" }]
map { name, age }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from [{ id: 1, name: "Alice", age: 30, city: "NYC" }]
			map { name, age }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +--------+-------+
		// |  name  |  age  |
		// +--------+-------+
		// | Alice  |  30   |
		// +--------+-------+
	}

	// Example 4: MAP with field renaming
	log_info!("\nExample 4: MAP with field renaming");
	log_query(
		r#"from [{ first_name: "Bob", years: 25 }]
map { first_name as name, years as age }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from [{ first_name: "Bob", years: 25 }]
			map { first_name as name, years as age }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +--------+-------+
		// |  name  |  age  |
		// +--------+-------+
		// |  Bob   |  25   |
		// +--------+-------+
	}

	// Example 5: MAP with computed fields
	log_info!("\nExample 5: MAP with computed fields");

	// Create sample data with prices
	log_info!("Setting up product data...");
	log_query(
		r#"from [
  { product: "Widget", price: 100, quantity: 5 },
  { product: "Gadget", price: 200, quantity: 3 },
  { product: "Tool", price: 50, quantity: 10 }
]
map {
  product,
  price,
  quantity,
  price * quantity as total,
  price * 0.1 as tax
}"#,
	);

	for frame in db
		.query_as_root(
			r#"
			from [
				{ product: "Widget", price: 100, quantity: 5 },
				{ product: "Gadget", price: 200, quantity: 3 },
				{ product: "Tool", price: 50, quantity: 10 }
			]
			map {
				product,
				price,
				quantity,
				price * quantity as total,
				price * 0.1 as tax
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +-----------+---------+------------+---------+-------+
		// |  product  |  price  |  quantity  |  total  |  tax  |
		// +-----------+---------+------------+---------+-------+
		// |  Widget   |   100   |     5      |   500   |  10   |
		// |  Gadget   |   200   |     3      |   600   |  20   |
		// |   Tool    |   50    |     10     |   500   |   5   |
		// +-----------+---------+------------+---------+-------+
	}

	// Example 6: MAP with complex expressions
	log_info!("\nExample 6: MAP with complex expressions");

	// Create a table for more realistic example
	db.command_as_root("create schema sales", Params::None).unwrap();
	db.command_as_root(
		r#"
		create table sales.orders {
			id: int4,
			customer: utf8,
			subtotal: float8,
			discount_percent: float8
		}
		"#,
		Params::None,
	)
	.unwrap();

	// Insert sample data
	db.command_as_root(
		r#"
		from [
			{ id: 1, customer: "Alice", subtotal: 100.0, discount_percent: 10.0 },
			{ id: 2, customer: "Bob", subtotal: 200.0, discount_percent: 15.0 },
			{ id: 3, customer: "Carol", subtotal: 150.0, discount_percent: 5.0 }
		]
		insert sales.orders
		"#,
		Params::None,
	)
	.unwrap();

	log_query(
		r#"from sales.orders
map {
  customer,
  subtotal,
  subtotal * (discount_percent / 100) as discount_amount,
  subtotal - (subtotal * (discount_percent / 100)) as final_total
}"#,
	);

	for frame in db
		.query_as_root(
			r#"
			from sales.orders
			map {
				customer,
				subtotal,
				subtotal * (discount_percent / 100) as discount_amount,
				subtotal - (subtotal * (discount_percent / 100)) as final_total
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +------------+-----------+-------------------+---------------+
		// |  customer  |  subtotal |  discount_amount  |  final_total  |
		// +------------+-----------+-------------------+---------------+
		// |   Carol    |   150.0   |       7.5         |     142.5     |
		// |    Bob     |   200.0   |      30.0         |     170.0     |
		// |   Alice    |   100.0   |      10.0         |      90.0     |
		// +------------+-----------+-------------------+---------------+
	}

	// Example 7: MAP with boolean expressions
	log_info!("\nExample 7: MAP with boolean expressions");
	log_query(
		r#"from [{ value: 10 }, { value: 20 }, { value: 5 }]
map { value, value > 15 as is_high, value <= 10 as is_low }"#,
	);

	for frame in db
		.query_as_root(
			r#"
			from [{ value: 10 }, { value: 20 }, { value: 5 }]
			map { value, value > 15 as is_high, value <= 10 as is_low }
			"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
		// Output:
		// +---------+-----------+----------+
		// |  value  |  is_high  |  is_low  |
		// +---------+-----------+----------+
		// |   10    |   false   |   true   |
		// |   20    |   true    |   false  |
		// |    5    |   false   |   true   |
		// +---------+-----------+----------+
	}
}
