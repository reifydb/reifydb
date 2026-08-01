// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The lowest-level operations a handler in `instruction/` delegates to, kept separate so one primitive can be
//! reused across many opcodes rather than reimplemented per handler.

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
