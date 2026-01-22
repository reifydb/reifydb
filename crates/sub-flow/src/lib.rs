// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub(crate) mod actor;
pub mod builder;
pub(crate) mod catalog;
pub(crate) mod convert;
pub(crate) mod coordinator;
pub(crate) mod coordinator_actor;
pub mod engine;
#[cfg(feature = "native")]
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

use engine::*;
use operator::Operator;
