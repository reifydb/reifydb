// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone)]
pub struct WasmConfig {
	pub max_memory_pages: u32,

	pub max_stack_size: usize,

	pub max_instructions: u64,

	pub max_call_depth: u32,
}

impl Default for WasmConfig {
	fn default() -> Self {
		Self {
			max_memory_pages: 256,
			max_stack_size: 32768,
			max_instructions: 10_000_000,
			max_call_depth: 256,
		}
	}
}
