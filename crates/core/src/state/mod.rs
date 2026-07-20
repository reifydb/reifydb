// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state: the single byte-bounded cache that owns every operator's resident state
//! ([`cache`]), the budget and lease accounting that bounds it ([`budget`]), and the persisted
//! map representation shared by the window engines ([`map`]). This is not window-specific -
//! ffi, native, distinct and take all route their state through the same cache.

pub mod budget;
pub mod cache;
pub mod map;
pub mod store;
