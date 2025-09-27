// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{
	MemoryDatabaseOptimistic, Params, Session, WithSubsystem,
	core::interface::{FlowNodeId, Transaction, logging::LogLevel::Info},
	embedded,
	engine::{StandardCommandTransaction, StandardRowEvaluator},
	log_info,
	sub_flow::{FlowBuilder, Operator, TransformOperator, flow::FlowChange},
	sub_logging::{FormatStyle, LoggingBuilder},
};

pub type DB = MemoryDatabaseOptimistic;

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Info)
}

struct MyOP;

impl<T: Transaction> Operator<T> for MyOP {
	fn id(&self) -> FlowNodeId {
		FlowNodeId(12345)
	}

	fn apply(
		&self,
		_txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> reifydb::Result<FlowChange> {
		println!("INVOKED");
		Ok(FlowChange::internal(FlowNodeId(12345), change.diffs))
	}
}

impl<T: Transaction> TransformOperator<T> for MyOP {}

fn flow_configuration<T: Transaction>(flow: FlowBuilder<T>) -> FlowBuilder<T> {
	flow.register_operator("test".to_string(), |_node, _exprs| Ok(Box::new(MyOP {})))
}

fn main() {
	let mut db: DB = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_flow(flow_configuration)
		.with_worker(|wp| wp)
		.build()
		.unwrap();

	db.start().unwrap();

	// Tests processing of Solana Jupiter swap events and LEFT JOIN with token metadata

	// Create namespace
	log_info!("Creating namespace solana...");
	db.command_as_root(r#"create namespace solana;"#, Params::None).unwrap();

	// Create tables
	log_info!("Creating table solana.jupiter_6_swap_events...");
	db.command_as_root(
		r#"create table solana.jupiter_6_swap_events { pool: utf8, input_mint: utf8, input_amount: uint16, output_mint: utf8, output_amount: uint16 }"#,
		Params::None,
	)
	.unwrap();

	log_info!("Creating table solana.token...");
	db.command_as_root(r#"create table solana.token { id: int8, mint: utf8, decimals: uint1 }"#, Params::None)
		.unwrap();

	// Insert known tokens (SOL and USDC)
	log_info!("Inserting known tokens...");
	db.command_as_root(
		r#"
from [
    {id: 1, mint: "So11111111111111111111111111111111111111112", decimals: 9},
    {id: 2, mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", decimals: 6}
] insert solana.token
	"#,
		Params::None,
	)
	.unwrap();

	// Create LEFT JOIN view for swap events with token decimals
	// Now supporting multiple joins to the same table!
	log_info!("Creating deferred view solana.test_view...");
	db.command_as_root(
		r#"
create deferred view solana.test_view {
    input_mint: utf8,
    input_amount: uint16,
    input_decimals: uint1,
    output_mint: utf8,
    output_amount: uint16,
    output_decimals: uint1
} as {
    from solana.jupiter_6_swap_events
    left join { from solana.token | filter mint == $input_mint } token on input_mint == token.mint with { strategy: lazy_loading }
    map {
        input_mint: input_mint,
        input_amount: input_amount,
        input_decimals: decimals,
        output_mint: output_mint,
        output_amount: output_amount
    }

}
	"#,
		Params::None,
	)
	.unwrap();

	// Verify token data
	log_info!("Verifying token data...");
	let result = db.query_as_root("from solana.token", Params::None).unwrap();
	for frame in result {
		println!("Token data:\n{}", frame);
	}

	// Process block 317897944 - Jupiter swap events
	// This simulates the processing that happens in the spawned thread

	// Insert first batch of swap events (SOL to USDC swaps)
	log_info!("Inserting first batch of swap events...");
	db.command_as_root(
		r#"
from [
    {
        pool: "7XawhbbxtsRcQA8KTkHT9f9nc6d69UwqCDh6U5EEbEmX",
        input_mint: "So11111111111111111111111111111111111111112",
        input_amount: 1000000000,
        output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        output_amount: 25430000
    },
    {
        pool: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
        input_mint: "So11111111111111111111111111111111111111112",
        input_amount: 500000000,
        output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        output_amount: 12715000
    }
] insert solana.jupiter_6_swap_events
	"#,
		Params::None,
	)
	.unwrap();

	// Check raw swap events
	log_info!("Checking raw swap events...");
	let result = db.query_as_root("from solana.jupiter_6_swap_events", Params::None).unwrap();
	for frame in result {
		println!("Raw swap events:\n{}", frame);
	}

	// Let the background task process
	sleep(Duration::from_millis(500));

	// Query LEFT JOIN view - should show SOL swaps with correct decimals (SOL=9, USDC=6)
	log_info!("Querying LEFT JOIN view...");
	let result = db.query_as_root("from solana.test_view", Params::None).unwrap();
	for frame in result {
		println!("View with decimals:\n{}", frame);
	}

	// Insert swap with unknown token (should have UNDEFINED decimals)
	log_info!("Inserting swap with unknown token...");
	db.command_as_root(
		r#"
from [{
    pool: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    input_mint: "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    input_amount: 5000000,
    output_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    output_amount: 1250000
}] insert solana.jupiter_6_swap_events
	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	// Query view - unknown input token should show UNDEFINED for input_decimals
	log_info!("Querying view with unknown token...");
	let result = db.query_as_root("from solana.test_view", Params::None).unwrap();
	for frame in result {
		println!("View with unknown token:\n{}", frame);
	}

	// Add the unknown token to token table
	log_info!("Adding unknown token to token table...");
	db.command_as_root(
		r#"
from [{id: 3, mint: "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", decimals: 5}] insert solana.token
	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	// Now the previously unknown token should have decimals (5 for input, 6 for USDC output)
	log_info!("Querying view after adding token info...");
	let result = db.query_as_root("from solana.test_view", Params::None).unwrap();
	for frame in result {
		println!("View with all token decimals:\n{}", frame);
	}

	// Test direct LEFT JOIN query (not using the view)
	log_info!("Testing direct LEFT JOIN query...");
	let result = db
		.query_as_root(
			r#"
from solana.jupiter_6_swap_events
left join { from solana.token } token on input_mint == token.mint
	"#,
			Params::None,
		)
		.unwrap();
	for frame in result {
		println!("Direct LEFT JOIN:\n{}", frame);
	}

	// Insert more complex swap events (multi-hop swaps)
	log_info!("Inserting complex swap events...");
	db.command_as_root(
		r#"
from [
    {
        pool: "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",
        input_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        input_amount: 10000000,
        output_mint: "So11111111111111111111111111111111111111112",
        output_amount: 393000000
    },
    {
        pool: "EGZ7tiLeH62TPV1gL8WwbXGzEPa9zmcpVnnkPKKnrE2U",
        input_mint: "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
        input_amount: 250000000,
        output_mint: "So11111111111111111111111111111111111111112",
        output_amount: 245000000
    }
] insert solana.jupiter_6_swap_events
	"#,
		Params::None,
	)
	.unwrap();

	sleep(Duration::from_millis(500));

	// Final state - mix of known and unknown tokens with correct decimals
	log_info!("Final view query...");
	let result = db.query_as_root("from solana.test_view", Params::None).unwrap();
	for frame in result {
		println!("Final view state:\n{}", frame);
	}

	log_info!("✅ Test completed successfully!");
	log_info!("Shutting down...");
}
