//! # Arithmetic Expressions Example
//!
//! Demonstrates arithmetic operations in ReifyDB's RQL:
//! - Basic arithmetic operators (+, -, *, /)
//! - Modulo operator (%)
//! - Parentheses for operation precedence
//! - Arithmetic in different contexts
//!
//! Run with: `make rql-arithmetic` or `cargo run --bin rql-arithmetic`

use reifydb::{Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

#[tokio::main]
async fn main() {
	let mut db = embedded::memory().await.unwrap().build().await.unwrap();
	db.start().await.unwrap();

	// Example 1: Basic arithmetic operations
	info!("Example 1: Basic arithmetic operations");
	log_query(
		r#"map {
  10 + 5 as addition,
  10 - 5 as subtraction,
  10 * 5 as multiplication,
  10 / 5 as division,
  10 % 3 as modulo
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				10 + 5 as addition,
				10 - 5 as subtraction,
				10 * 5 as multiplication,
				10 / 5 as division,
				10 % 3 as modulo
			}
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 2: Operator precedence
	info!("\nExample 2: Operator precedence (multiplication before addition)");
	log_query(
		r#"map {
  2 + 3 * 4 as without_parens,
  (2 + 3) * 4 as with_parens
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				2 + 3 * 4 as without_parens,
				(2 + 3) * 4 as with_parens
			}
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 3: Arithmetic with floating point
	info!("\nExample 3: Floating point arithmetic");
	log_query(
		r#"map {
  3.14 * 2.0 as pi_times_two,
  10.5 / 2.5 as decimal_division,
  1.1 + 2.2 as decimal_addition
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				3.14 * 2.0 as pi_times_two,
				10.5 / 2.5 as decimal_division,
				1.1 + 2.2 as decimal_addition
			}
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Set up sample data for more comptokenize examples
	db.command_as_root("create namespace shop", Params::None).await.unwrap();
	db.command_as_root(
		r#"
		create table shop.products {
			id: int4,
			name: utf8,
			price: float8,
			quantity: int4,
			discount_percent: float8,
			tax_rate: float8
		}
		"#,
		Params::None,
	)
	.await
	.unwrap();

	db.command_as_root(
		r#"
		from [
			{ id: 1, name: "Widget", price: 29.99, quantity: 5, discount_percent: 10.0, tax_rate: 8.5 },
			{ id: 2, name: "Gadget", price: 49.99, quantity: 3, discount_percent: 15.0, tax_rate: 8.5 },
			{ id: 3, name: "Tool", price: 99.99, quantity: 2, discount_percent: 20.0, tax_rate: 8.5 },
			{ id: 4, name: "Device", price: 149.99, quantity: 1, discount_percent: 5.0, tax_rate: 8.5 }
		]
		insert shop.products
		"#,
		Params::None,
	)
	.await
	.unwrap();

	// Example 4: Arithmetic on table columns
	info!("\nExample 4: Calculate total price (price * quantity)");
	log_query(
		r#"from shop.products
map { name, price, quantity, price * quantity as total }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop.products
			map { name, price, quantity, price * quantity as total }
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 5: Comptokenize calculations with discounts
	info!("\nExample 5: Calculate discount amount and final price");
	log_query(
		r#"from shop.products
map {
  name,
  price,
  discount_percent,
  price * (discount_percent / 100) as discount_amount,
  price - (price * discount_percent / 100) as discounted_price
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop.products
			map {
				name,
				price,
				discount_percent,
				price * (discount_percent / 100) as discount_amount,
				price - (price * discount_percent / 100) as discounted_price
			}
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 6: Tax calculations
	info!("\nExample 6: Calculate price with tax");
	log_query(
		r#"from shop.products
map {
  name,
  price,
  tax_rate,
  price * (tax_rate / 100) as tax_amount,
  price + (price * tax_rate / 100) as price_with_tax
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop.products
			map {
				name,
				price,
				tax_rate,
				price * (tax_rate / 100) as tax_amount,
				price + (price * tax_rate / 100) as price_with_tax
			}
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 7: Arithmetic in filter conditions
	info!("\nExample 7: Filter using arithmetic expression");
	log_query(
		r#"from shop.products
filter price * quantity > 100"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop.products
			filter price * quantity > 100
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 8: Comptokenize nested calculations
	info!("\nExample 8: Complete order calculation");
	log_query(
		r#"from shop.products
map {
  name,
  quantity,
  price * quantity as subtotal,
  (price * quantity) * (discount_percent / 100) as discount,
  (price * quantity) - ((price * quantity) * (discount_percent / 100)) as after_discount,
  ((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (tax_rate / 100) as tax,
  ((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (1 + tax_rate / 100) as final_total
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop.products
			map {
				name,
				quantity,
				price * quantity as subtotal,
				(price * quantity) * (discount_percent / 100) as discount,
				(price * quantity) - ((price * quantity) * (discount_percent / 100)) as after_discount,
				((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (tax_rate / 100) as tax,
				((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (1 + tax_rate / 100) as final_total
			}
			"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		info!("{}", frame);
	}
}
