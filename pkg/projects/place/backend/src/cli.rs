// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use clap::Parser;

#[derive(Parser)]
pub struct RunArgs {
	#[arg(long, env = "PLACE_WS_BIND", default_value = "0.0.0.0:8091")]
	pub ws_bind: String,
}
