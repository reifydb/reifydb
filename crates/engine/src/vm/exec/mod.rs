// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Inner execution primitives the VM dispatch loop calls into. Arithmetic, comparison, logic, control flow,
//! masking, broadcasting, looping, and the call-into-routine dispatcher: each is the lowest-level operation a
//! handler in `instruction/` ultimately delegates to. Splitting these out keeps individual instruction handlers
//! short and lets the same primitive be reused across many opcodes.

pub(crate) mod arithmetic;
pub(crate) mod broadcast;
pub(crate) mod call;
pub(crate) mod comparison;
pub(crate) mod control;
pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod logic;
pub(crate) mod loops;
pub(crate) mod mask;
pub(crate) mod query;
pub(crate) mod special;
pub(crate) mod stack;
pub(crate) mod vars;
