// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM core - bytecode interpreter and execution state.

pub mod builtin;
pub mod call_stack;
pub mod interpreter;
pub mod scope;
pub mod script;
pub mod state;

#[cfg(feature = "trace")]
pub mod trace;

pub use builtin::BuiltinRegistry;
pub use call_stack::{CallFrame, CallStack};
pub use interpreter::DispatchResult;
pub use scope::{Scope, ScopeChain};
pub use script::BytecodeScriptCaller;
pub use state::{OperandValue, PipelineHandle, VmConfig, VmContext, VmState};
#[cfg(feature = "trace")]
pub use trace::{TraceEntry, VmTracer};
