// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[command(name = "reifydb-uptime", version, about = "Self-hostable uptime monitoring built on ReifyDB")]
pub struct RunArgs {
	#[arg(long, env = "UPTIME_HTTP_BIND", default_value = "0.0.0.0:8080")]
	pub http_bind: String,

	#[arg(long, env = "UPTIME_REIFYDB_HTTP_BIND", default_value = "127.0.0.1:8090")]
	pub reifydb_http_bind: String,

	#[arg(long, env = "UPTIME_REIFYDB_WS_BIND", default_value = "0.0.0.0:8091")]
	pub reifydb_ws_bind: String,

	#[arg(long, env = "UPTIME_DATA_DIR", default_value = "/tmp/uptime")]
	pub data_dir: PathBuf,

	#[arg(long, env = "UPTIME_MAX_CONCURRENT_CHECKS", default_value_t = 64)]
	pub max_concurrent_checks: usize,

	#[arg(long, env = "UPTIME_ALLOW_PRIVATE_TARGETS")]
	pub allow_private_targets: bool,

	#[arg(long)]
	pub memory: bool,
}
