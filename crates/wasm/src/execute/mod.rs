// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod exec;
pub mod instruction;
pub mod stack;
pub mod state;

pub use exec::{Exec, HostFunctionRegistry};
pub use instruction::{ExecInstruction, ExecResult, ExecStatus};
pub use stack::{Stack, StackAccess};
pub use state::State;

pub type Result<T> = std::result::Result<T, crate::module::Trap>;
