// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

mod cli;
mod orchestrator;
mod runner;
mod shared;

use clap::Parser;
use cli::Cli;
use reifydb::allocator;

allocator::set_global_allocator!();

fn main() {
	allocator::verify();

	let cli = Cli::parse();

	if let Some(ref url) = cli.runner {
		runner::start(url);
	} else {
		orchestrator::start(&cli);
	}
}
