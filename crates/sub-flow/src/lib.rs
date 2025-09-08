// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(warnings))] // FIXME

#[allow(dead_code, unused_variables)]
mod engine;
#[allow(dead_code, unused_variables)]
mod operator;
pub mod subsystem;

pub use engine::*;
pub use reifydb_core::Result;
pub use subsystem::{FlowSubsystem, FlowSubsystemFactory};
