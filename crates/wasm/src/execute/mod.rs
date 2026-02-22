// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod exec;
pub mod instruction;
pub mod stack;
pub mod state;

use crate::module::Trap;

pub type Result<T> = std::result::Result<T, Trap>;
