// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

mod backfill;
pub mod builder;
pub(crate) mod catalog;
pub(crate) mod convert;
pub(crate) mod coordinator;
mod engine;
pub mod ffi;
pub(crate) mod lag;
#[allow(dead_code)]
mod operator;
pub(crate) mod pool;
pub mod subsystem;
pub(crate) mod tracker;
pub mod transaction;
pub(crate) mod worker;

pub use builder::FlowBuilder;
pub use engine::*;
pub use lag::FlowLags;
pub use operator::{Operator, stateful};
