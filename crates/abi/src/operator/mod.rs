// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for FFI operators. The host inspects the descriptor's capability flags to know which vtable
//! entries an operator actually supports.

pub mod capabilities;
pub mod column;
pub mod descriptor;
pub mod timer;
pub mod types;
pub mod vtable;
