// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::time::Duration;

use reifydb::{FromFrame, Params, WithSubsystem, embedded, r#type::Uuid7};
use tokio::time::sleep;

#[derive(FromFrame)]
struct CreateSubscriptionResult {
	pub subscription_id: Uuid7,
	#[allow(dead_code)]
	pub created: bool,
}

#[tokio::main]
async fn main() {
	let mut db = embedded::memory().with_flow(|f| f).build().unwrap();

	println!("Database built with {} subsystems", db.subsystem_count());

	// Check if FlowSubsystem exists
	if db.subsystem_count() == 0 {
		panic!("No subsystems registered!");
	}

	println!("Starting database...");
	db.start().unwrap();
	println!("Database started successfully!");

	println!("Database is_running: {}", db.is_running());

	println!("\n=== Subscription Demo ===\n");

	// 1. Create namespace
	println!(">>> Creating namespace demo");
	db.command_as_root("create namespace demo;", Params::None).unwrap();

	// 2. Create events table
	println!("\n>>> Creating table demo.events");
	for frame in db
		.command_as_root(
			r#"create table demo.events {
				id: int4,
				message: utf8,
				timestamp: uint8
			}"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	println!("\n>>> Creating subscription");
	println!("    This will stream all changes from demo.events");
	let frames = db
		.command_as_root(
			r#"create subscription {
				id: int4,
				message: utf8,
				timestamp: uint8
			} as {
				from demo.events
			}"#,
			Params::None,
		)
		.unwrap();

	let result = CreateSubscriptionResult::from_frame(&frames[0]).unwrap();
	let subscription_id = result.first().unwrap().subscription_id;

	println!(">>> Subscription ID from CREATE result: {}", subscription_id);

	let _frames = db
		.command_as_root(
			r#"create deferred view demo.test_view {
				id: int4,
				message: utf8,
				timestamp: uint8
			} as {
				from demo.events
			}"#,
			Params::None,
		)
		.unwrap();

	for frame in db
		.command_as_root(
			r#"from [{
				id: 1,
				message: "First event",
				timestamp: 1000
			}, {
				id: 2,
				message: "Second event",
				timestamp: 2000
			}, {
				id: 3,
				message: "Third event",
				timestamp: 3000
			}] insert demo.events"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	// Wait for flows to process events
	println!("\n>>> Waiting for flows to process events...");
	sleep(Duration::from_millis(500)).await;
	println!(">>> Done waiting");

	// 6. Query source table for comparison
	println!("\n>>> Source table demo.events:");
	for frame in db.query_as_root("from demo.events", Params::None).unwrap() {
		println!("{}", frame);
	}

	// 8. Verify the subscription flow was created
	println!("\n>>> Subscription flow in system.flows:");
	let flow_name = format!("subscription_{}", subscription_id);
	for frame in
		db.query_as_root(&format!("from system.flows | filter name == '{}'", flow_name), Params::None).unwrap()
	{
		println!("{}", frame);
	}

	// 7. Query the subscription rows using the generator function
	println!("\\n>>> Querying subscription: {}", subscription_id);

	let query = format!("from inspect_subscription {{ id: '{}' }}", subscription_id);
	for frame in db.query_as_root(&query, Params::None).unwrap() {
		println!("{}", frame);
	}

	for frame in db.query_as_root("from demo.test_view", Params::None).unwrap() {
		println!("{}", frame);
	}
}
