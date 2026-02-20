// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::module::Value;

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Table {
    pub elements: Vec<Option<Value>>,
    pub limit: TableLimit,
}

#[derive(Clone, PartialEq)]
pub struct TableLimit {
    pub min: u32,
    pub max: Option<u32>,
}
