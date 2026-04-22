// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod array;
pub mod compress;
pub mod compute;
pub mod encoding;
pub mod mask;
pub mod nones;
pub mod selection;
pub mod stats;

pub use compute::{CompareOp, SearchResult, compare, filter, min_max, search_sorted, slice, sum, take};
