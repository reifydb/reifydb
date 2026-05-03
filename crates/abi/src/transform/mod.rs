// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI shapes for FFI transforms: descriptor, typed shapes, and vtable. Transforms sit between operators and
//! procedures - they evaluate within an operator pipeline like an operator, but author intent is closer to a
//! pure function. The dedicated ABI exists so transform-shape extensions are not forced through the operator or
//! procedure surface, which carry concerns transforms do not need.

pub mod descriptor;
pub mod types;
pub mod vtable;
