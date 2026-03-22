// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod cli;
mod orchestrator;
mod runner;
mod shared;

use clap::Parser;
use cli::Cli;

fn main() {
	let cli = Cli::parse();

	if let Some(ref url) = cli.runner {
		runner::start(url);
	} else {
		orchestrator::start(&cli);
	}
}
