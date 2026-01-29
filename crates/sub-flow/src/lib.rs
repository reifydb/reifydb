// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod builder;
pub(crate) mod catalog;
pub(crate) mod convert;
pub(crate) mod coordinator;
pub mod engine;
#[cfg(reifydb_target = "native")]
pub mod ffi;
pub(crate) mod instruction;
pub(crate) mod lag;
#[allow(dead_code)]
pub mod operator;
pub(crate) mod pool;
pub(crate) mod state;
pub mod subsystem;
pub(crate) mod tracker;
pub mod transaction;
pub(crate) mod worker;

use engine::*;
use operator::Operator;
