// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{path::PathBuf, str::FromStr, thread::sleep, time::Duration};

use reifydb::{
	Params, Session, WithSubsystem,
	core::interface::logging::LogLevel::Trace,
	embedded,
	sub_logging::{FormatStyle, LoggingBuilder},
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Trace)
}

fn main() {
	let mut db = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_worker(|wp| wp)
		.with_flow(|f| {
			f.operators_dir(
				PathBuf::from_str("/home/ddymke/Workspace/red/testsuite/fixture/target/debug").unwrap(),
			)
		})
		.build()
		.unwrap();

	db.start().unwrap();

	// Create namespace
	println!("Creating namespace...");
	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();

	// Create tables
	println!("Creating tables...");
	db.command_as_root(
		r#"create table test.sales { id: int4, product_id: int4, quantity: int4, region_id: int4 }"#,
		Params::None,
	)
	.unwrap();
	db.command_as_root(r#"create table test.products { id: int4, product_name: utf8 }"#, Params::None).unwrap();
	db.command_as_root(r#"create table test.regions { id: int4, region_name: utf8 }"#, Params::None).unwrap();

	// Insert all data BEFORE creating the view
	println!("Inserting products...");
	db.command_as_root(
		r#"
from [
    {id: 1, product_name: "Laptop"},
    {id: 2, product_name: "Phone"},
    {id: 3, product_name: "Tablet"},
    {id: 4, product_name: "Monitor"},
    {id: 5, product_name: "Keyboard"}
] insert test.products
"#,
		Params::None,
	)
	.unwrap();

	println!("Inserting regions...");
	db.command_as_root(
		r#"
from [
    {id: 1, region_name: "North"},
    {id: 2, region_name: "South"},
    {id: 3, region_name: "East"},
    {id: 4, region_name: "West"},
    {id: 5, region_name: "Central"},
    {id: 6, region_name: "Northeast"}
] insert test.regions
"#,
		Params::None,
	)
	.unwrap();

	println!("Inserting sales (30 rows)...");
	db.command_as_root(
		r#"
from [
    {id: 1, product_id: 1, quantity: 10, region_id: 1},
    {id: 2, product_id: 2, quantity: 25, region_id: 2},
    {id: 3, product_id: 3, quantity: 15, region_id: 3},
    {id: 4, product_id: 4, quantity: 8, region_id: 4},
    {id: 5, product_id: 5, quantity: 50, region_id: 5},
    {id: 6, product_id: 1, quantity: 12, region_id: 6},
    {id: 7, product_id: 2, quantity: 30, region_id: 1},
    {id: 8, product_id: 3, quantity: 20, region_id: 2},
    {id: 9, product_id: 4, quantity: 5, region_id: 3},
    {id: 10, product_id: 5, quantity: 45, region_id: 4},
    {id: 11, product_id: 1, quantity: 18, region_id: 5},
    {id: 12, product_id: 2, quantity: 22, region_id: 6},
    {id: 13, product_id: 3, quantity: 13, region_id: 1},
    {id: 14, product_id: 4, quantity: 7, region_id: 2},
    {id: 15, product_id: 5, quantity: 60, region_id: 3},
    {id: 16, product_id: 1, quantity: 9, region_id: 4},
    {id: 17, product_id: 2, quantity: 35, region_id: 5},
    {id: 18, product_id: 3, quantity: 17, region_id: 6},
    {id: 19, product_id: 4, quantity: 6, region_id: 1},
    {id: 20, product_id: 5, quantity: 55, region_id: 2},
    {id: 21, product_id: 1, quantity: 14, region_id: 3},
    {id: 22, product_id: 2, quantity: 28, region_id: 4},
    {id: 23, product_id: 3, quantity: 19, region_id: 5},
    {id: 24, product_id: 4, quantity: 4, region_id: 6},
    {id: 25, product_id: 5, quantity: 40, region_id: 1},
    {id: 26, product_id: 1, quantity: 11, region_id: 2},
    {id: 27, product_id: 2, quantity: 33, region_id: 3},
    {id: 28, product_id: 3, quantity: 16, region_id: 4},
    {id: 29, product_id: 4, quantity: 9, region_id: 5},
    {id: 30, product_id: 5, quantity: 48, region_id: 6}
] insert test.sales
"#,
		Params::None,
	)
	.unwrap();

	// Now create the view - it should backfill with all 30 sales
	println!("\nCreating deferred view with double LEFT JOINs...");
	db.command_as_root(
		r#"
create deferred view test.sales_report {
    sale_id: int4,
    product: utf8,
    region: utf8,
    quantity: int4
} as {
    from test.sales
    left join { from test.products } products on product_id == products.id
    left join { from test.regions } regions on region_id == regions.id
    map {
        sale_id: id,
        product: product_name,
        region: region_name,
        quantity: quantity
    }
}
"#,
		Params::None,
	)
	.unwrap();

	println!("Created deferred view");

	// Wait for view to process data
	sleep(Duration::from_millis(500));

	// Query the deferred view - should have backfilled with all 30 sales
	println!("\n=== Deferred view query (should show all 30 sales with joined data) ===");
	for frame in db.query_as_root(r#"from test.sales_report sort sale_id asc"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\n=== Expected: sale_id, product, region, quantity for all 30 sales ===");
}
