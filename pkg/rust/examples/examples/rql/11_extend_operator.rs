//! # EXTEND Operator Example
//!
//! Demonstrates the EXTEND operator in ReifyDB's RQL:
//! - Adding new columns while preserving existing ones
//! - Computing derived columns from existing data
//! - Comparing EXTEND vs MAP behavior
//!
//! Run with: `make rql-extend` or `cargo run --bin rql-extend`

use reifydb::{embedded, log_info, Params, Session};
use reifydb_examples::log_query;

fn main() {
	// Create and start an in-memory database
	let mut db = embedded::memory_optimistic().build().unwrap();
	db.start().unwrap();

	// Example 1: Standalone EXTEND with constants (creates a single-row
	// frame)
	log_info!("Example 1: Standalone EXTEND with constants");
	log_query(r#"extend { total: 42, tax: 3.14 }"#);

	for frame in db.query_as_root(r#"extend { total: 42, tax: 3.14 }"#, Params::None).unwrap() {
		log_info!("{}", frame);
	}

	// Example 2: Standalone EXTEND with single expression
	log_info!("\nExample 2: Standalone EXTEND with computed value");
	log_query(r#"extend result: 100 + 23"#);

	for frame in db.query_as_root(r#"extend result: 100 + 23"#, Params::None).unwrap() {
		log_info!("{}", frame);
	}

	// Example 3: EXTEND with inline data to add computed columns
	log_info!("\nExample 3: EXTEND with existing frame (inline data)");
	log_query(
		r#"from [{ name: "Alice", price: 100 }]
extend { total: price * 1.1, tax: price * 0.1 }"#,
	);

	for frame in db
		.query_as_root(
			r#"
		from [{ name: "Alice", price: 100 }]
		extend { total: price * 1.1, tax: price * 0.1 }
		"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 4: EXTEND with multiple rows
	log_info!("\nExample 4: EXTEND with multiple rows");
	log_query(
		r#"from [{ value: 10 }, { value: 20 }]
extend result: value * 2"#,
	);

	for frame in db
		.query_as_root(
			r#"
		from [{ value: 10 }, { value: 20 }]
		extend result: value * 2
		"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}

	// Example 5: Compare EXTEND vs MAP behavior
	log_info!("\nExample 5: EXTEND vs MAP - column preservation");
	log_query(
		r#"from [{ id: 1, name: "Bob", age: 25 }]
extend category: age > 30"#,
	);

	for frame in db
		.query_as_root(
			r#"
		from [{ id: 1, name: "Bob", age: 25 }]
		extend category: age > 30
		"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("EXTEND result (preserves all original columns):");
		log_info!("{}", frame);
	}

	log_query(
		r#"from [{ id: 1, name: "Bob", age: 25 }]
map { name, category: age > 30 }"#,
	);

	for frame in db
		.query_as_root(
			r#"
		from [{ id: 1, name: "Bob", age: 25 }]
		map { name, category: age > 30 }
		"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("MAP result (only selected columns):");
		log_info!("{}", frame);
	}

	// Example 6: EXTEND with complex calculations
	log_info!("\nExample 6: EXTEND with complex calculations");
	log_query(
		r#"from [
  { product: "Widget", price: 100, quantity: 5 },
  { product: "Gadget", price: 200, quantity: 3 }
]
extend {
  subtotal: price * quantity,
  tax: price * quantity * 0.1,
  total: price * quantity * 1.1
}"#,
	);

	for frame in db
		.query_as_root(
			r#"
		from [
		  { product: "Widget", price: 100, quantity: 5 },
		  { product: "Gadget", price: 200, quantity: 3 }
		]
		extend {
		  subtotal: price * quantity,
		  tax: price * quantity * 0.1,
		  total: price * quantity * 1.1
		}
		"#,
			Params::None,
		)
		.unwrap()
	{
		log_info!("{}", frame);
	}
}
