// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "reifydb-forge", about = "Forge — CI platform powered by ReifyDB")]
pub struct Cli {
	/// Run in runner mode, connecting to the orchestrator at this gRPC URL
	#[arg(long)]
	pub runner: Option<String>,

	/// gRPC server bind address (orchestrator mode)
	#[arg(long, default_value = "0.0.0.0:50051")]
	pub grpc_addr: String,

	/// WebSocket server bind address (orchestrator mode)
	#[arg(long, default_value = "0.0.0.0:8091")]
	pub ws_addr: String,

	/// HTTP static file server bind address (orchestrator mode)
	#[arg(long, default_value = "0.0.0.0:3000")]
	pub http_addr: String,
}
