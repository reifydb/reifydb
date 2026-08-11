// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Callbacks every stage is allowed to reach. A stage adds its own table on top; a stage that must not reach
//! state or the dictionary simply has no field for them.

pub mod builder;
pub mod memory;
pub mod rql;
