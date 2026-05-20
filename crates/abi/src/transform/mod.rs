// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for FFI transforms: descriptor, typed shapes, and vtable. Transforms sit between operators and
//! procedures - they evaluate within an operator pipeline like an operator, but author intent is closer to a
//! pure function. The dedicated ABI exists so transform-shape extensions are not forced through the operator or
//! procedure surface, which carry concerns transforms do not need.

pub mod descriptor;
pub mod types;
pub mod vtable;
