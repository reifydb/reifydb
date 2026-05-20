// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

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
