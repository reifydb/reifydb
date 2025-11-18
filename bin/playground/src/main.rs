// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{path::PathBuf, str::FromStr, thread::sleep, time::Duration};

use reifydb::{
	Params, Session, WithSubsystem,
	core::interface::logging::LogLevel::Info,
	embedded,
	sub_logging::{FormatStyle, LoggingBuilder},
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Info)
}

fn main() {
	// let mut db = embedded::sqlite_optimistic(SqliteConfig::new("/tmp/test/test.db"))
	// let mut db = embedded::sqlite_optimistic(SqliteConfig::in_memory())
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

	// Create namespaces
	println!("Creating namespaces...");
	db.command_as_root(r#"create namespace decoded;"#, Params::None).unwrap();
	db.command_as_root(r#"create namespace solana;"#, Params::None).unwrap();
	db.command_as_root(r#"create namespace jupiter;"#, Params::None).unwrap();

	// Create tables
	println!("Creating tables...");
	db.command_as_root(
		r#"create table decoded.jupiter_swaps { signature: utf8, slot: uint8, timestamp: uint8, input_mint: utf8, input_amount: uint8, output_mint: utf8, output_amount: uint8 }"#,
		Params::None,
	)
	.unwrap();
	db.command_as_root(r#"create table solana.token { id: uint2, mint: utf8, decimals: uint1 }"#, Params::None)
		.unwrap();

	// Insert tokens FIRST (before creating view)
	println!("Inserting tokens...");
	db.command_as_root(
		r#"
from [
  {id: 0, mint: "So11111111111111111111111111111111111111112", decimals: 9},
  {id: 1, mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", decimals: 6},
  {id: 2, mint: "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", decimals: 6}
] insert solana.token
"#,
		Params::None,
	)
	.unwrap();

	// Verify tokens exist
	println!("\nVerifying tokens:");
	for frame in db.query_as_root(r#"from solana.token"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	// Create deferred view with double LEFT JOIN
	println!("\nCreating deferred view with double LEFT JOIN...");
	db.command_as_root(
		r#"
create deferred view jupiter.normalized_swaps {
  signature: utf8,
  slot: uint8,
  timestamp: uint8,
  input_mint: utf8,
  input_amount: uint8,
  input_decimals: uint1,
  output_mint: utf8,
  output_amount: uint8,
  output_decimals: uint1
} as {
  from decoded.jupiter_swaps
  left join { from solana.token } token1 on input_mint == token1.mint
  map {
      signature,
      slot,
      timestamp,
      input_mint,
      input_amount,
      input_decimals: decimals,
      output_mint,
      output_amount
  }
  left join { from solana.token } token2 on output_mint == token2.mint
  map {
      signature,
      slot,
      timestamp,
      input_mint,
      input_amount,
      input_decimals,
      output_mint,
      output_amount,
      output_decimals: decimals
  }
}
"#,
		Params::None,
	)
	.unwrap();

	println!("Created deferred view");

	// Insert swap data (SOL to USDC)
	println!("\nInserting swap data...");
	db.command_as_root(
		r#"
from [
  {
      signature: "5Feqr3pDV3YpBtHQdJAYeNbEoKfCAwXVJe8PRccJZvmJRFmem287anU7VLbkVCvEAuXahMi5kZUz5UuEZm77akSB",
      slot: 377679661,
      timestamp: 1762184460,
      input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
      input_amount: 4010000,
      output_mint: "So11111111111111111111111111111111111111112",
      output_amount: 156000000
  },
  {
      signature: "4GxhoyPfxuhFrAuDLVVGf9zp9PqCX48wTkc6YtFnsna2PLfSEBMSSrUBKYhHFtSNsygp24QKCjwxT1H6k6jZqPBf",
      slot: 377679644,
      timestamp: 1762184453,
      input_mint: "So11111111111111111111111111111111111111112",
      input_amount: 100000000,
      output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
      output_amount: 2540000
  }
] insert decoded.jupiter_swaps
"#,
		Params::None,
	)
	.unwrap();

	// Wait for view to process data
	sleep(Duration::from_millis(500));

	// Test 1: Direct query (should work)
	println!("\n=== Test 1: Direct query (should show decimals correctly) ===");
	for frame in db
		.query_as_root(
			r#"
from decoded.jupiter_swaps
left join { from solana.token } token1 on input_mint == token1.mint
map {
  signature,
  input_mint,
  input_decimals: decimals
}
"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Test 2: Query the deferred view (this is where the bug occurs)
	println!("\n=== Test 2: Deferred view query (decimals show as Undefined - BUG) ===");
	for frame in db.query_as_root(r#"from jupiter.normalized_swaps"#, Params::None).unwrap() {
		println!("{}", frame);
	}

	println!("\n=== Expected: input_decimals and output_decimals should show 6 or 9, not Undefined ===");
}
