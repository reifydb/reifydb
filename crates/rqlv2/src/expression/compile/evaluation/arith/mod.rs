// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Arithmetic operations module.
//!
//! This module implements arithmetic operations (Add, Sub, Mul, Div, Rem)
//! following the pattern from the old engine implementation with explicit
//! type matching for all supported numeric type combinations.

pub mod add;
pub mod div;
pub mod mul;
pub mod rem;
pub mod sub;

pub(crate) use add::eval_add;
pub(crate) use div::eval_div;
pub(crate) use mul::eval_mul;
pub(crate) use rem::eval_rem;
pub(crate) use sub::eval_sub;
