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
		r#"create table test.transfers { id: int4, from_token_id: int4, to_token_id: int4, amount: int4 }"#,
		Params::None,
	)
	.unwrap();
	db.command_as_root(r#"create table test.tokens { id: int4, symbol: utf8, decimals: int4 }"#, Params::None)
		.unwrap();

	// Insert all data BEFORE creating the view
	println!("Inserting tokens...");
	db.command_as_root(
		r#"
from [
    {id: 1, symbol: "BTC", decimals: 8},
    {id: 2, symbol: "ETH", decimals: 18},
    {id: 3, symbol: "USDC", decimals: 6}
] insert test.tokens
"#,
		Params::None,
	)
	.unwrap();

	println!("Inserting transfers...");
	db.command_as_root(
		r#"
from [
    {id: 1, from_token_id: 1, to_token_id: 2, amount: 1000},
    {id: 2, from_token_id: 2, to_token_id: 3, amount: 500},
    {id: 3, from_token_id: 3, to_token_id: 1, amount: 10000},
    {id: 4, from_token_id: 1, to_token_id: 3, amount: 250},
    {id: 5, from_token_id: 4, to_token_id: 2, amount: 100}
] insert test.transfers
"#,
		Params::None,
	)
	.unwrap();

	// Verify data exists
	println!("\nVerifying tokens:");
	for frame in db.query_as_root(r#"from test.tokens"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\nVerifying transfers:");
	for frame in db.query_as_root(r#"from test.transfers"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Now create the view with double join to same table
	println!("\nCreating deferred view with double LEFT JOIN...");
	db.command_as_root(
		r#"
create deferred view test.transfer_details {
    transfer_id: int4,
    from_symbol: utf8,
    from_decimals: int4,
    to_symbol: utf8,
    to_decimals: int4,
    amount: int4
} as {
    from test.transfers
    left join { from test.tokens } from_token on from_token_id == from_token.id
        map {
        transfer_id: id,
        from_symbol: symbol,
        from_decimals: decimals,
        amount: amount
    }
    left join { from test.tokens } to_token on to_token_id == to_token.id
    map {
        transfer_id,
        from_symbol,
        from_decimals,
        to_symbol: symbol,
        to_decimals: decimals,
        amount
    }
}
"#,
		Params::None,
	)
	.unwrap();

	println!("Created deferred view");

	// Wait for view to process data
	sleep(Duration::from_millis(500));

	// Query the deferred view
	println!("\n=== Deferred view query (should show token info from both joins) ===");
	for frame in db.query_as_root(r#"from test.transfer_details sort transfer_id asc"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\n=== Expected: from_symbol/from_decimals and to_symbol/to_decimals should show token info ===");
	println!("=== Row 5 should have Undefined for from_symbol/from_decimals (no token_id=4) ===");
}
