// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

use reifydb::{Params, WithSubsystem, embedded};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
	let mut db = embedded::memory().await.unwrap().with_flow(|f| f).build().await.unwrap();

	db.start().await.unwrap();

	println!("=== Testing: Left join with token swap events ===\n");

	// Create namespace
	println!(">>> Creating namespace solana");
	db.command_as_root("create namespace solana;", Params::None).await.unwrap();

	// Create swap events table
	println!(">>> Creating table solana.jupiter_6_swap_events");
	for frame in db
		.command_as_root(
			r#"create table solana.jupiter_6_swap_events{
				pool: utf8,
				input_mint: utf8,
				input_amount: uint16,
				output_mint: utf8,
				output_amount: uint16,
				block_id: uint8,
				timestamp: uint8
			}"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		println!("{}", frame);
	}

	// Create token table
	println!("\n>>> Creating table solana.token");
	for frame in db
		.command_as_root(
			r#"create table solana.token {
				id: uint2,
				mint: utf8,
				decimals: uint1
			}"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		println!("{}", frame);
	}

	// Create deferred view with left join
	println!("\n>>> Creating deferred view solana.swap_view");
	for frame in db
		.command_as_root(
			r#"create deferred view solana.swap_view {
				pool: utf8,
				input_mint: utf8,
				input_amount: uint16,
				output_mint: utf8,
				output_amount: uint16,
				block_id: uint8,
				timestamp: uint8,
				price: uint1
			} as {
				from solana.jupiter_6_swap_events
				left join { from solana.token } as token1 using (input_mint, token1.mint)
				map {
					price: token1_decimals
				}
			}"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		println!("{}", frame);
	}

	// Insert token data first
	println!("\n>>> Inserting token data");
	for frame in db
		.command_as_root(
			r#"from [
				{id: 0, mint: "So11111111111111111111111111111111111111112", decimals: 9},
				{id: 1, mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", decimals: 6}
			] insert solana.token"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		println!("{}", frame);
	}

	// Insert a test swap with matching token
	println!("\n>>> Inserting swap event");
	for frame in db
		.command_as_root(
			r#"from [{
				pool: "test_pool",
				input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
				input_amount: 1000000,
				output_mint: "So11111111111111111111111111111111111111112",
				output_amount: 5000000000,
				block_id: 1,
				timestamp: 1000
			}] insert solana.jupiter_6_swap_events"#,
			Params::None,
		)
		.await
		.unwrap()
	{
		println!("{}", frame);
	}

	println!("\n>>> Waiting for view to update...");
	sleep(Duration::from_millis(500)).await;

	// Query swap view
	println!("\n>>> Querying solana.swap_view");
	println!("    Expected: All fields should have values, price = 6");
	for frame in db.query_as_root("from solana.swap_view", Params::None).await.unwrap() {
		println!("{}", frame);
	}

	// Also query source tables for comparison
	println!("\n>>> Source table: solana.jupiter_6_swap_events");
	for frame in db.query_as_root("from solana.jupiter_6_swap_events", Params::None).await.unwrap() {
		println!("{}", frame);
	}

	println!("\n>>> Source table: solana.token");
	for frame in db.query_as_root("from solana.token", Params::None).await.unwrap() {
		println!("{}", frame);
	}

	println!("\n=== Test complete ===");
}
