// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! VM execution tracer.

use super::{
	diff::compute_diff,
	entry::{DispatchResultSnapshot, InstructionSnapshot, StateSnapshot, TraceEntry},
	format::format_trace,
};

/// VM execution tracer that records state after each instruction step.
#[derive(Debug)]
pub struct VmTracer {
	/// Recorded trace entries.
	entries: Vec<TraceEntry>,

	/// Step counter.
	step_count: usize,

	/// Previous state snapshot (for computing deltas).
	previous_state: Option<StateSnapshot>,
}

impl VmTracer {
	/// Create a new tracer.
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
			step_count: 0,
			previous_state: None,
		}
	}

	/// Record a trace entry after a step completes.
	pub fn record(
		&mut self,
		ip_before: usize,
		bytecode: Vec<u8>,
		instruction: InstructionSnapshot,
		current_state: StateSnapshot,
		result: DispatchResultSnapshot,
	) {
		let ip_after = current_state.ip;

		// Compute changes from previous state
		let changes = match &self.previous_state {
			Some(prev) => compute_diff(prev, &current_state),
			None => {
				// First step - compute changes from empty state
				let empty = StateSnapshot::empty();
				compute_diff(&empty, &current_state)
			}
		};

		let entry = TraceEntry {
			step: self.step_count,
			ip_before,
			ip_after,
			bytecode,
			instruction,
			changes,
			state: current_state.clone(),
			result,
		};

		self.entries.push(entry);
		self.step_count += 1;
		self.previous_state = Some(current_state);
	}

	/// Get the recorded trace entries.
	pub fn entries(&self) -> &[TraceEntry] {
		&self.entries
	}

	/// Take ownership of the recorded trace entries.
	pub fn take_entries(self) -> Vec<TraceEntry> {
		self.entries
	}

	/// Get the number of recorded steps.
	pub fn step_count(&self) -> usize {
		self.step_count
	}

	/// Format the trace as human-readable text.
	pub fn format(&self) -> String {
		format_trace(&self.entries)
	}

	/// Print the trace to stdout.
	pub fn print(&self) {
		print!("{}", self.format());
	}
}

impl Default for VmTracer {
	fn default() -> Self {
		Self::new()
	}
}
