// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod builder;
pub(crate) mod catalog;
pub(crate) mod deferred;
pub mod engine;
#[cfg(reifydb_target = "native")]
pub mod ffi;
#[allow(dead_code)]
pub mod operator;
pub mod subsystem;
pub mod transaction;

pub(crate) use operator::Operator;
pub(crate) mod transactional;
