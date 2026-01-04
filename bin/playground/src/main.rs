// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::{Identity, Params, WithSubsystem, embedded};

#[tokio::main]
async fn main() {
	let mut db = embedded::memory().await.unwrap().with_flow(|f| f).build().await.unwrap();

	db.start().await.unwrap();

	// Test MAP without input
	println!("=== Testing MAP {{1}} ===");
	let result = db.engine().query_new_as(&Identity::root(), "MAP {1 + 3}", Params::None).await;
	match &result {
		Ok(frames) => {
			println!("Success! Got {} frames", frames.len());
			let frame = frames.first().expect("Expected at least one frame");
			println!("Frame display:\n{}", frame);
		}
		Err(e) => println!("Error: {:?}", e),
	}

	// Test EXTEND without input
	println!("\n=== Testing EXTEND {{2}} ===");
	let result = db.engine().query_new_as(&Identity::root(), "EXTEND {2}", Params::None).await;
	match &result {
		Ok(frames) => {
			println!("Success! Got {} frames", frames.len());
			let frame = frames.first().expect("Expected at least one frame");
			println!("Frame display:\n{}", frame);
		}
		Err(e) => println!("Error: {:?}", e),
	}

	println!("\n=== Test complete ===");
}
