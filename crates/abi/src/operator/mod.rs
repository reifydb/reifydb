// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

//! Operator-specific types for registration, discovery, and vtables

mod capabilities;
mod column;
mod descriptor;
mod types;
mod vtable;

pub use capabilities::*;
pub use column::*;
pub use descriptor::*;
pub use types::*;
pub use vtable::*;
