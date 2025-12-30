// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! VM core - bytecode interpreter and execution state.

pub mod call_stack;
pub mod interpreter;
pub mod scope;
pub mod state;

pub use call_stack::{CallFrame, CallStack};
pub use interpreter::DispatchResult;
pub use scope::{Scope, ScopeChain};
pub use state::{OperandValue, PipelineHandle, VmConfig, VmContext, VmState};
