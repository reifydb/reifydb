// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for FFI transforms. A transform evaluates inside an operator pipeline but is authored as a pure
//! function; the separate ABI keeps it off the operator and procedure surfaces, which carry concerns it does not
//! need.

pub mod descriptor;
pub mod types;
pub mod vtable;
