// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ReifyDB Operator SDK

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod catalog;
pub mod error;
pub mod ffi;
pub mod flow;
pub mod marshal;
pub mod operator;
pub mod state;
pub mod store;
pub mod testing;
