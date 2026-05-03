// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Virtual machine that executes the instruction stream emitted by the planner. The VM owns the per-request
//! lifecycle (admin, command, query, subscription), threads parameters through, walks the plan, dispatches each
//! instruction to its handler, and returns the resulting columns or change deltas to the caller.
//!
//! Instruction handlers live alongside the VM so the dispatch table is the single place that decides what an
//! opcode does. Adding a new instruction means writing a handler and wiring it in - planner output never reaches
//! storage without first being interpreted here.

use reifydb_type::params::Params;

#[derive(Debug)]
pub struct Admin<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Command<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Query<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Subscription<'a> {
	pub rql: &'a str,
	pub params: Params,
}

#[derive(Debug)]
pub struct Test<'a> {
	pub rql: &'a str,
	pub params: Params,
}

pub(crate) mod exec;
pub mod executor;
pub mod instruction;
pub mod services;
pub mod stack;
#[allow(clippy::module_inception)]
pub mod vm;
pub mod volcano;
