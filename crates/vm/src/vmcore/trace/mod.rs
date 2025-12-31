// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM execution tracing for debugging.
//!
//! This module provides full instrumentation of VM execution, capturing:
//! - Each instruction executed with raw bytecode and decoded form
//! - Full state snapshots after each step
//! - Delta information showing what changed
//!
//! Enable with the `trace` feature flag.

mod diff;
mod entry;
mod format;
mod snapshot;
mod tracer;

pub use entry::{
	CallFrameSnapshot, ColumnSnapshot, DispatchResultSnapshot, FrameSnapshot, InstructionSnapshot, OperandSnapshot,
	OperatorSnapshot, RecordSnapshot, ScopeSnapshot, StateChange, StateSnapshot, TraceEntry,
};
pub use format::format_trace;
pub use snapshot::{
	pipeline_description, snapshot_call_frame, snapshot_dispatch_result, snapshot_operand, snapshot_operator,
	snapshot_state,
};
pub use tracer::VmTracer;
