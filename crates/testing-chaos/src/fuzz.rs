// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	panic::{self, AssertUnwindSafe},
};

use rand::{RngExt, SeedableRng, rngs::StdRng};

pub fn split(seed: u64) -> (StdRng, u64) {
	let mut master = StdRng::seed_from_u64(seed);
	let parameters: u64 = master.random();
	let sequence: u64 = master.random();
	(StdRng::seed_from_u64(parameters), sequence)
}

pub fn pick<T: Copy>(rng: &mut StdRng, options: &[T]) -> T {
	options[rng.random_range(0..options.len() as u32) as usize]
}

pub fn run_reported<P: fmt::Debug>(label: &str, sequence_seed: u64, params: &P, run: impl FnOnce()) {
	if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(run)) {
		eprintln!(
			"\nCHAOS FAILURE {label}\n  pin this, not the master seed:\n\n\tdrive(\n\t\t{sequence_seed},\n{},\n\t);\n",
			tab_indent(params)
		);
		panic::resume_unwind(payload);
	}
}

fn tab_indent<P: fmt::Debug>(params: &P) -> String {
	format!("{params:#?}")
		.lines()
		.map(|line| format!("\t\t{}", line.replace("    ", "\t")))
		.collect::<Vec<_>>()
		.join("\n")
}
