// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Call stack management for function calls.

/// A call frame representing a function invocation.
#[derive(Debug)]
pub struct CallFrame {
	/// Function index being executed.
	pub function_index: u16,

	/// Return address (bytecode position to return to).
	pub return_address: usize,

	/// Base pointer for operand stack (for cleanup on return).
	pub operand_base: usize,

	/// Base pointer for pipeline stack.
	pub pipeline_base: usize,

	/// Scope depth at call time (for cleanup).
	pub scope_depth: usize,
}

impl CallFrame {
	/// Create a new call frame.
	pub fn new(
		function_index: u16,
		return_address: usize,
		operand_base: usize,
		pipeline_base: usize,
		scope_depth: usize,
	) -> Self {
		Self {
			function_index,
			return_address,
			operand_base,
			pipeline_base,
			scope_depth,
		}
	}
}

/// Stack of call frames for function calls.
#[derive(Debug)]
pub struct CallStack {
	frames: Vec<CallFrame>,
	max_depth: usize,
}

impl CallStack {
	/// Create a new call stack with the given maximum depth.
	pub fn new(max_depth: usize) -> Self {
		Self {
			frames: Vec::new(),
			max_depth,
		}
	}

	/// Push a new call frame.
	/// Returns false if the stack would exceed max depth.
	pub fn push(&mut self, frame: CallFrame) -> bool {
		if self.frames.len() >= self.max_depth {
			return false;
		}
		self.frames.push(frame);
		true
	}

	/// Pop a call frame.
	pub fn pop(&mut self) -> Option<CallFrame> {
		self.frames.pop()
	}

	/// Get the current (topmost) call frame.
	pub fn current(&self) -> Option<&CallFrame> {
		self.frames.last()
	}

	/// Get a mutable reference to the current call frame.
	pub fn current_mut(&mut self) -> Option<&mut CallFrame> {
		self.frames.last_mut()
	}

	/// Current depth of the call stack.
	pub fn depth(&self) -> usize {
		self.frames.len()
	}

	/// Check if the call stack is empty.
	pub fn is_empty(&self) -> bool {
		self.frames.is_empty()
	}

	/// Iterate over call frames from bottom to top.
	pub fn iter(&self) -> impl Iterator<Item = &CallFrame> {
		self.frames.iter()
	}
}

impl Default for CallStack {
	fn default() -> Self {
		Self::new(256)
	}
}
