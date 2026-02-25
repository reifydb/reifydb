// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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

fn main() {
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();

	// Example 1: Basic arithmetic operations
	info!("Example 1: Basic arithmetic operations");
	log_query(
		r#"map {
  addition: 10 + 5,
  subtraction: 10 - 5,
  multiplication: 10 * 5,
  division: 10 / 5,
  modulo: 10 % 3
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				addition: 10 + 5,
				subtraction: 10 - 5,
				multiplication: 10 * 5,
				division: 10 / 5,
				modulo: 10 % 3
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 2: Operator precedence
	info!("\nExample 2: Operator precedence (multiplication before addition)");
	log_query(
		r#"map {
  without_parens: 2 + 3 * 4,
  with_parens: (2 + 3) * 4
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				without_parens: 2 + 3 * 4,
				with_parens: (2 + 3) * 4
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 3: Arithmetic with floating point
	info!("\nExample 3: Floating point arithmetic");
	log_query(
		r#"map {
  pi_times_two: 3.14 * 2.0,
  decimal_division: 10.5 / 2.5,
  decimal_addition: 1.1 + 2.2
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			map {
				pi_times_two: 3.14 * 2.0,
				decimal_division: 10.5 / 2.5,
				decimal_addition: 1.1 + 2.2
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Set up sample data for more comptokenize examples
	db.admin_as_root("create namespace shop", Params::None).unwrap();
	db.admin_as_root(
		r#"
		create table shop::products {
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
	.unwrap();

	db.command_as_root(
		r#"
		INSERT shop::products [
			{ id: 1, name: "Widget", price: 29.99, quantity: 5, discount_percent: 10.0, tax_rate: 8.5 },
			{ id: 2, name: "Gadget", price: 49.99, quantity: 3, discount_percent: 15.0, tax_rate: 8.5 },
			{ id: 3, name: "Tool", price: 99.99, quantity: 2, discount_percent: 20.0, tax_rate: 8.5 },
			{ id: 4, name: "Device", price: 149.99, quantity: 1, discount_percent: 5.0, tax_rate: 8.5 }
		]
		"#,
		Params::None,
	)
	.unwrap();

	// Example 4: Arithmetic on table columns
	info!("\nExample 4: Calculate total price (price * quantity)");
	log_query(
		r#"from shop::products
map { name, price, quantity, total: price * quantity }"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop::products
			map { name, price, quantity, total: price * quantity }
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 5: Comptokenize calculations with discounts
	info!("\nExample 5: Calculate discount amount and final price");
	log_query(
		r#"from shop::products
map {
  name,
  price,
  discount_percent,
  discount_amount: price * (discount_percent / 100),
  discounted_price: price - (price * discount_percent / 100)
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop::products
			map {
				name,
				price,
				discount_percent,
				discount_amount: price * (discount_percent / 100),
				discounted_price: price - (price * discount_percent / 100)
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 6: Tax calculations
	info!("\nExample 6: Calculate price with tax");
	log_query(
		r#"from shop::products
map {
  name,
  price,
  tax_rate,
  tax_amount: price * (tax_rate / 100),
  price_with_tax: price + (price * tax_rate / 100)
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop::products
			map {
				name,
				price,
				tax_rate,
				tax_amount: price * (tax_rate / 100),
				price_with_tax: price + (price * tax_rate / 100)
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 7: Arithmetic in filter conditions
	info!("\nExample 7: Filter using arithmetic expression");
	log_query(
		r#"from shop::products
filter price * quantity > 100"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop::products
			filter price * quantity > 100
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}

	// Example 8: Comptokenize nested calculations
	info!("\nExample 8: Complete order calculation");
	log_query(
		r#"from shop::products
map {
  name,
  quantity,
  subtotal: price * quantity,
  discount: (price * quantity) * (discount_percent / 100),
  after_discount: (price * quantity) - ((price * quantity) * (discount_percent / 100)),
  tax: ((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (tax_rate / 100),
  final_total: ((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (1 + tax_rate / 100)
}"#,
	);
	for frame in db
		.query_as_root(
			r#"
			from shop::products
			map {
				name,
				quantity,
				subtotal: price * quantity,
				discount: (price * quantity) * (discount_percent / 100),
				after_discount: (price * quantity) - ((price * quantity) * (discount_percent / 100)),
				tax: ((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (tax_rate / 100),
				final_total: ((price * quantity) - ((price * quantity) * (discount_percent / 100))) * (1 + tax_rate / 100)
			}
			"#,
			Params::None,
		)
		.unwrap()
	{
		info!("{}", frame);
	}
}
