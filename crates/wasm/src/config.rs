// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Resource limits and configuration for WASM execution sandboxing.

/// Configuration for WASM execution resource limits.
#[derive(Debug, Clone)]
pub struct WasmConfig {
	/// Maximum number of memory pages (each page is 64KB). Default: 256 (16MB).
	pub max_memory_pages: u32,
	/// Maximum value stack size in entries. Default: 32768.
	pub max_stack_size: usize,
	/// Maximum number of instructions to execute (fuel). Default: 10,000,000.
	pub max_instructions: u64,
	/// Maximum call depth for nested function calls. Default: 1000.
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
